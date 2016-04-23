use zmq;

use std::str;
use std::error::Error;
use std::fmt;
use std::cmp;
use std::time::{Duration, Instant};

use serialize::hex::ToHex;
use rustc::util::sha2::{Sha256, Digest};

use super::{bytes_to_int, ErrorKind, XactError};

struct TimedZMQTransaction {
  ctx: zmq::Context,
  sock: zmq::Socket,
  time_to_die: Instant
}

impl Drop for TimedZMQTransaction {
  fn drop(&mut self) {
    match self.sock.close() {
      Ok(()) => { debug!("socket dropped") },
      Err(e) => panic!(e)
    }

    debug!("dropping context.");
    let mut e = self.ctx.destroy();
    while e == Err(zmq::Error::EINTR) {
      e = self.ctx.destroy();
    }
  }
}

impl TimedZMQTransaction {
  pub fn new(endpoint: &str, timeout: Duration) -> Result<TimedZMQTransaction, zmq::Error> {
    let mut ctx = zmq::Context::new();
    let mut sock = try!(ctx.socket(zmq::DEALER));
    try!(sock.set_linger(0));
    try!(sock.connect(endpoint));

    let now = Instant::now();

    Ok(TimedZMQTransaction {
      ctx: ctx,
      sock: sock,
      time_to_die: now + timeout
    })
  }

  pub fn send_multipart(&mut self, parts: &[&[u8]], timeout: Option<Duration>) -> Result<(), zmq::Error> {
    let poll_result = try!(self.poll(timeout, zmq::POLLOUT));
    if poll_result == 0 {
      warn!("Poll failed in send_multipart().");
      return Err(zmq::Error::EBUSY);
    }

    let num_parts = parts.len();
    for (index, part) in parts.iter().enumerate() {
      let flags = if index < num_parts - 1 { zmq::SNDMORE|zmq::DONTWAIT } else { zmq::DONTWAIT };
      try!(self.sock.send(part, flags));
    }
    Ok(())
  }

  pub fn recv_multipart(&mut self, timeout: Option<Duration>) -> Result<Vec<Vec<u8>>, zmq::Error> {
    let poll_result = try!(self.poll(timeout, zmq::POLLIN));
    if poll_result == 0 {
      warn!("Poll failed in recv_multipart().");
      return Err(zmq::Error::EBUSY);
    }

    let mut parts: Vec<Vec<u8>> = vec![];
    loop {
      let part = try!(self.sock.recv_bytes(zmq::DONTWAIT));
      parts.push(part);

      let more_parts = try!(self.sock.get_rcvmore());
      if !more_parts {
        break;
      }
    }

    Ok(parts)
  }

  pub fn poll(&mut self, timeout: Option<Duration>, events: i16) -> Result<i32, zmq::Error> {
    let timeout_ms = self.get_remaining_ms(timeout);
    debug!("About to poll for {} ms.", timeout_ms);
    zmq::poll(&mut [self.sock.as_poll_item(events)], timeout_ms)
  }

  fn get_remaining_ms(&self, timeout: Option<Duration>) -> i64 {
    let remaining = self.get_remaining_duration(timeout);
    let secs = remaining.as_secs();
    let subsec_nanos = remaining.subsec_nanos();
    let millis = secs * 1000 + (subsec_nanos / 1e6 as u32) as u64;
    millis as i64
  }

  fn get_remaining_duration(&self, timeout: Option<Duration>) -> Duration {
    if self.time_to_die <= Instant::now() {
      Duration::new(0, 0)
    } else {
      let max_remaining = self.time_to_die.duration_since(Instant::now());
      match timeout {
        Some(duration) => cmp::min(duration, max_remaining),
        None => max_remaining
      }
    }
  }
}

pub fn send_binary_blob<F>(endpoint: &str, blob_id: &str, data: &[u8], timeout: Duration, consistent: bool,
                   on_progress: F) -> Result<Vec<u8>, XactError> where F: Fn(&str) -> () {

  let mut transactor = try!(TimedZMQTransaction::new(&endpoint, timeout));

  debug!("Sending PING...");
  let ping_timeout = Some(Duration::from_millis(500));
  try!(transactor.send_multipart(&[b"PING"], ping_timeout));
  debug!("\tSent PING.");

  debug!("Waiting for PONG...");
  let ping_response_parts = try!(transactor.recv_multipart(ping_timeout));

  assert!(ping_response_parts.len() == 2, "PING response had wrong number of parts: {}", ping_response_parts.len());
  assert!(ping_response_parts[1] == b"PONG", "Invalid PING response");
  debug!("\tReceived PONG.");

  let data_length = data.len();
  let data_size_str: String = format!("{}", data_length);
  let data_size_msg = data_size_str.as_bytes();

  debug!("Sending START...");
  try!(transactor.send_multipart(&[b"START", blob_id.as_bytes(), data_size_msg], None));
  debug!("\tSent START.");

  debug!("Waiting for GOGO ...");
  let start_response_parts = try!(transactor.recv_multipart(None));
  assert!(start_response_parts.len() == 3, "{}", start_response_parts.len());

  let chunk_size_msg = try!(match (start_response_parts[1].as_slice(), start_response_parts[2].as_slice()) {
    (b"NOGO", _) => Err(XactError::new(ErrorKind::NOGO, "Endpoint was not ready.")),
    (b"GOGO", chunk_size_bytes) => Ok(chunk_size_bytes),
    (_, _) => Err(XactError::new(ErrorKind::INVALID_RESPONSE, "Invalid chunk size"))
  });
  debug!("\tReceived GOGO.");

  let chunk_size = try!(bytes_to_int(chunk_size_msg));
  debug!("Chunk size: {}", chunk_size);

  on_progress("Progress: 0%");

  let mut hash = Sha256::new();

  for (chunk_index, chunk) in data.chunks(chunk_size).enumerate() {
    debug!("Waiting for TOKEN...");
    let chunk_request_parts = try!(transactor.recv_multipart(None));
    assert!(chunk_request_parts.len() >= 2, "{}", chunk_request_parts.len());

    match chunk_request_parts[1].as_slice() {
      b"TOKEN" => {
        debug!("\tReceived TOKEN.");
      },
      _ => { return Err(XactError::new(ErrorKind::INVALID_RESPONSE, "Invalid chunk request")); }
    };

    debug!("Sending chunk...");
    try!(transactor.send_multipart(&[b"CHUNK", chunk], None));
    debug!("\tSent chunk.");

    hash.input(chunk);

    let progress_percent_repr: String = format!("Progress: {}%", 100 * (chunk_index * chunk_size + chunk.len()) / data_length);
    on_progress(&progress_percent_repr);
  }

  let hash_hex: String = hash.result_bytes().to_hex();
  debug!("Sending hash: {:?} ...", hash_hex);
  try!(transactor.send_multipart(&[b"END", hash_hex.as_bytes()], None));
  debug!("\tSent hash.");

  loop {
    debug!("Waiting for OK...");
    let result_parts = try!(transactor.recv_multipart(None));
    assert!(result_parts.len() >= 2, "{}", result_parts.len());

    match result_parts[1].as_slice() {
      b"TOKEN" => {
        debug!("Ignoring extra chunk request.");
        continue;
      },
      b"OK" => {
        debug!("\tReceived OK.");
        break;
      },
      _ => { return Err(XactError::new(ErrorKind::INVALID_RESPONSE, "Invalid end response")); }
    }
  }

  if consistent {
    let result_parts = try!(transactor.recv_multipart(None));
    assert!(result_parts.len() == 3, "{}", result_parts.len());
    assert!(result_parts[1] == b"CONS", "Invalid consistency response");

    let res = result_parts[2].clone();
    Ok(res)
  } else {
    debug!("Exiting send_binary_blob().");
    Ok(vec![])
  }
}
