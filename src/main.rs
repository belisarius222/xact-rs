#![feature(rustc_private)]

extern crate zmq;

use std::str;
use std::error::Error;
use std::fmt;
use std::cmp;
use std::time::{Duration, Instant};

extern crate rustc;
use rustc::util::sha2::{Sha256, Digest};

#[allow(non_camel_case_types)]
#[derive(Clone, Debug)]
enum ErrorKind {
  ZMQ_ERROR(zmq::Error),
  TIMEOUT,
  INVALID_RESPONSE,
  NOGO,
}

impl fmt::Display for ErrorKind {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    let desc = match self.clone() {
      ErrorKind::ZMQ_ERROR(e) => e.description().to_owned(),
      ErrorKind::TIMEOUT => "TIMEOUT".to_string(),
      ErrorKind::INVALID_RESPONSE => "INVALID_RESPONSE".to_string(),
      ErrorKind::NOGO => "NOGO".to_string()
    };
    write!(f, "{}", desc)
  }
}

#[derive(Clone, Debug)]
struct XactError {
  kind: ErrorKind,
  msg: String,
  full_desc: String,
}

impl fmt::Display for XactError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", self.full_desc)
  }
}

impl std::error::Error for XactError {
  fn description(&self) -> &str {
    self.full_desc.as_ref()
  }
}

impl XactError {
  fn new(kind: ErrorKind, msg: &str) -> Self {
    let full_desc = format!("Error of type: {}, msg: '{}'", kind, msg);
    XactError {
      kind: kind,
      msg: String::from(msg),
      full_desc: full_desc
    }
  }

  fn from_zmq(e: zmq::Error, msg: &str) -> Self {
    XactError::new(ErrorKind::ZMQ_ERROR(e), msg)
  }
}

impl From<zmq::Error> for XactError {
  fn from(e: zmq::Error) -> Self {
    XactError::new(ErrorKind::ZMQ_ERROR(e), e.description())
  }
}

struct TimedZMQTransaction {
  _ctx: zmq::Context,
  sock: zmq::Socket,
  time_to_die: Instant
}

impl TimedZMQTransaction {
  pub fn new(endpoint: &str, timeout: Duration) -> Result<TimedZMQTransaction, zmq::Error> {
    let mut ctx = zmq::Context::new();
    let mut sock = try!(ctx.socket(zmq::DEALER));
    try!(sock.set_linger(0));
    try!(sock.connect(endpoint));

    let now = Instant::now();

    Ok(TimedZMQTransaction {
      _ctx: ctx,
      sock: sock,
      time_to_die: now + timeout
    })
  }

  pub fn send_multipart(&mut self, parts: &[&[u8]], timeout: Option<Duration>) -> Result<(), zmq::Error> {
    println!("send_multipart()");
    let poll_result = try!(self.poll(timeout, zmq::POLLOUT));
    if poll_result == 0 {
      return Err(zmq::Error::EBUSY);
    }

    let num_parts = parts.len();
    for (index, part) in parts.iter().enumerate() {
      let flags = if index < num_parts - 1 { zmq::SNDMORE|zmq::DONTWAIT } else { zmq::DONTWAIT };
      try!(self.sock.send(part, flags));
      println!("Sent part.");
    }
    Ok(())
  }

  pub fn recv_multipart(&mut self, timeout: Option<Duration>) -> Result<Vec<Vec<u8>>, zmq::Error> {
    println!("recv_multipart()");
    let poll_result = try!(self.poll(timeout, zmq::POLLIN));
    if poll_result == 0 {
      return Err(zmq::Error::EBUSY);
    }

    let mut parts: Vec<Vec<u8>> = vec![];
    loop {
      println!("Waiting to receive a part.");
      let part = try!(self.sock.recv_bytes(zmq::DONTWAIT));
      parts.push(part);
      println!("Received a part.");

      let more_parts = try!(self.sock.get_rcvmore());
      if !more_parts {
        println!("No more parts.");
        break;
      }
    }

    Ok(parts)
  }

  pub fn poll(&mut self, timeout: Option<Duration>, events: i16) -> Result<i32, zmq::Error> {
    let timeout_ms = self.get_remaining_ms(timeout);
    zmq::poll(&mut [self.sock.as_poll_item(events)], timeout_ms)
  }

  fn get_remaining_ms(&self, timeout: Option<Duration>) -> i64 {
    (self.get_remaining_duration(timeout).as_secs() * 1000) as i64
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

fn send_binary_blob<F>(endpoint: &str, blob_id: &str, data: &[u8], timeout: Duration, consistent: bool,
                   on_progress: F) -> Result<Vec<u8>, XactError> where F: Fn(&str) -> () {

  let mut transactor = try!(TimedZMQTransaction::new(&endpoint, timeout));

  print!("Sending PING...");
  let ping_timeout = Some(Duration::from_millis(500));
  try!(transactor.send_multipart(&[b"PING"], ping_timeout));
  println!("Sent.");

  print!("Receiving PING response...");
  let ping_response_parts = try!(transactor.recv_multipart(ping_timeout));
  println!("Received.");

  assert!(ping_response_parts.len() == 2, "PING response had wrong number of parts: {}", ping_response_parts.len());
  assert!(ping_response_parts[1] == b"PONG", "Invalid PING response");

  let data_length = data.len() as u64;
  let data_size_str: String = format!("{}", data_length);
  let data_size_msg = data_size_str.as_bytes();

  print!("Sending start message...");
  try!(transactor.send_multipart(&[b"START", blob_id.as_bytes(), data_size_msg], None));
  println!("Sent.");

  print!("Waiting for start response...");
  let start_response_parts = try!(transactor.recv_multipart(None));
  assert!(start_response_parts.len() == 3, "{}", start_response_parts.len());
  println!("Received.");

  let chunk_size_msg = try!(match (start_response_parts[1].as_slice(), start_response_parts[2].as_slice()) {
    (b"NOGO", _) => Err(XactError::new(ErrorKind::NOGO, "Endpoint was not ready.")),
    (b"GOGO", chunk_size_bytes) => Ok(chunk_size_bytes),
    (_, _) => Err(XactError::new(ErrorKind::INVALID_RESPONSE, "Invalid chunk size"))
  });

  let chunk_size_str = try!(str::from_utf8(chunk_size_msg).map_err(|_| {
    XactError::new(ErrorKind::INVALID_RESPONSE, "Unable to parse chunk size as utf8")
  }));
  let chunk_size = try!(chunk_size_str.parse::<u64>().map_err(|_| {
    XactError::new(ErrorKind::INVALID_RESPONSE, "Unable to parse chunk size as integer")
  }));
  println!("Chunk size: {}", chunk_size);

  on_progress("0");

  let mut data_cursor = 0;
  let mut chunks_requested = 0;
  let mut hash = Sha256::new();

  while data_cursor < data_length {
    let poll_timeout = Some(Duration::from_millis(500));

    while try!(transactor.poll(poll_timeout, zmq::POLLIN)) != 0 {
      let chunk_request_parts = try!(transactor.recv_multipart(None));
      assert!(chunk_request_parts.len() >= 2, "{}", chunk_request_parts.len());

      match chunk_request_parts[1].as_slice() {
        b"TOKEN" => { chunks_requested += 1 },
        _ => { return Err(XactError::new(ErrorKind::INVALID_RESPONSE, "Invalid chunk request")); }
      };
    }

    while (chunks_requested > 0) && (data_cursor < data_length) {
      let data_start = data_cursor as usize;
      let data_end = cmp::min(data_start + chunk_size as usize, data_length as usize);
      let chunk = &data[data_start..data_end];

      try!(transactor.send_multipart(&[b"CHUNK", chunk], None));

      hash.input(chunk);
      data_cursor = data_end as u64;
      chunks_requested -= 1;

      let progress_percent_repr: String = format!("{}", 100 * data_end as u64 / data_length);
      on_progress(&progress_percent_repr);
    }
  }

  try!(transactor.send_multipart(&[b"END", hash.result_bytes().as_slice()], None));

  loop {
    let result_parts = try!(transactor.recv_multipart(None));
    assert!(result_parts.len() >= 2, "{}", result_parts.len());

    match result_parts[1].as_slice() {
      b"TOKEN" => { continue; },  // Ignore extra chunk requests.
      b"OK" => { break; },
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
    Ok(vec![])
  }
}

fn main() {
  match send_binary_blob("tcp://127.0.0.1:1234", "message_id", "ermahgerd".as_bytes(), Duration::from_millis(500), false, |s| { println!("{}", s) }) {
    Ok(result_bytes) => { println!("{}", result_bytes.len()); },
    Err(e) => panic!(e)
  };
}
