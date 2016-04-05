extern crate zmq;

use std::cmp;
use std::io;
use std::time { Duration, Instant };

struct TimedZMQTransaction<'a> {
  ctx: &'a mut zmq::Context,
  sock: &'a mut zmq::Socket,
  timeout: Duration,
  time_to_die: Instant
}

impl TimedZMQTransaction {
  pub fn new(endpoint: &str, timeout: Duration) -> Result<TimedZMQTransaction, zmq::Error> {
    let mut ctx = zmq::Context::new();
    let mut sock = ctx.socket(zmq::DEALER);
    try!(sock.set_linger(0));
    try!(sock.connect(endpoint));

    let now = Instant::now();

    Ok(TimedZMQTransaction {
      ctx: &zmq::Context,
      sock: &sock,
      timeout: timeout,
      time_to_die: now + timeout
    })
  }

  pub fn get_remaining_time(&self, timeout: Option<Duration>) -> Duration {
    if self.time_to_die <= Instant::now() {
      return Duration::new(0, 0);
    }

    let max_remaining = self.time_to_die.duration_from_earlier(Instant::now());
    match timeout {
      Some(duration) => cmp::min(duration, max_remaining),
      None => max_remaining
    }
  }

  pub fn send_multipart(&mut self, parts: &[&[u8]], description: &str, timeout: Option<Duration>) -> io::Result<()> {
    let use_desc = |e| io::Error::new(&description);

    let num_parts = parts.len();
    for (index, part) in parts.enumerate() {
      let send_timeout = self.get_remaining_time(timeout).as_secs() * 1000;
      let send_timeout_ms = (send_timeout.as_secs() * 1000) as i64;
      try!(zmq::poll([self.sock.as_poll_item(zmq::POLLOUT)], send_timeout_ms).map_err(use_desc));

      let  flags = if index < num_parts - 1 { zmq::SNDMORE|zmq::DONTWAIT } else { zmq::DONTWAIT };
      try!(self.sock.send(part, flags).map_err(use_desc));
    }
    Ok(())
  }

  pub fn recv_multipart(&mut self, description: &str, timeout: Option<Duration>) -> io::Result<&[&[u8]]> {
    let use_desc = |e| io::Error::new(&description);

  }
}

fn send_message(endpoint: &str, message_id: &str, data: [u8], timeout: &Duration, consistent: bool, on_progress: F) -> io::Result<()>
  where F: Fn(&str) -> () {

  let mut transactor = try!(TimedZMQTransaction::new(&endpoint, timeout));



  Ok(())
}

fn main() {
  send_message("tcp://127.0.0.1:1234", "message_id", "ermahgerd".as_bytes(), Duration::from_millis(500), false, |s| -> { println!(s) })
}
