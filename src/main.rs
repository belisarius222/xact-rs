extern crate zmq;

use std::cmp;
use std::time::{Duration, Instant};

struct TimedZMQTransaction {
  ctx: zmq::Context,
  sock: zmq::Socket,
  time_to_die: Instant
}

impl TimedZMQTransaction {
  pub fn new(endpoint: &str, timeout: Duration) -> Result<TimedZMQTransaction, zmq::Error> {
    let mut ctx: zmq::Context = zmq::Context::new();
    let mut sock: zmq::Socket = try!(ctx.socket(zmq::DEALER));
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
    try!(self.poll(timeout, zmq::POLLOUT));

    let num_parts = parts.len();
    for (index, part) in parts.iter().enumerate() {
      let flags = if index < num_parts - 1 { zmq::SNDMORE|zmq::DONTWAIT } else { zmq::DONTWAIT };
      try!(self.sock.send(part, flags));
    }
    Ok(())
  }

  pub fn recv_multipart(&mut self, timeout: Option<Duration>) -> Result<Vec<Vec<u8>>, zmq::Error> {
    try!(self.poll(timeout, zmq::POLLIN));

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

fn send_message<F>(endpoint: &str, message_id: &str, data: &[u8], timeout: Duration, consistent: bool, on_progress: F) -> Result<(), zmq::Error>
  where F: Fn(&str) -> () {

  let mut transactor = try!(TimedZMQTransaction::new(&endpoint, timeout));
  try!(transactor.send_multipart(&[data], Some(timeout)));

  Ok(())
}

fn main() {
  match send_message("tcp://127.0.0.1:1234", "message_id", "ermahgerd".as_bytes(), Duration::from_millis(500), false, |s| { println!("{}", s) }) {
    Ok(_) => (),
    Err(e) => panic!(e)
  };
}
