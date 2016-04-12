use zmq;

use std::str;
use std::error::Error;
use std::fmt;
use std::cmp;
use std::time::{Duration, Instant};

use serialize::hex::ToHex;
use rustc::util::sha2::{Sha256, Digest};

use super::{ErrorKind, XactError};

use std::thread;
use std::sync::mpsc::{channel, SendError};
use std::sync::mpsc::Receiver as ChannelReceiver;
use std::sync::mpsc::Sender as ChannelSender;
use std::collections::HashMap;

const BLOB_TTL_SECONDS: u32 = 10;
const DEFAULT_CHUNK_SIZE = 1e7 as usize;
const MAX_SIMUL_CHUNKS: u32 = 10;
const MSG_PADDING: usize = 100;
const STOP: u32 = 1;

pub struct Blob {
  pub id: String,
  pub array: Vec<u8>,
  pub hash: Sha256,
  time_to_die: Instant
}

impl Blob {
  pub fn new(id: &str, array_size: usize) -> Blob {
    Blob {
      id: id.to_owned(),
      array: Vec::with_capacity(array_size),
      hash: Sha256::new(),
      time_to_die: Blob::get_next_ttl()
    }
  }

  pub fn is_alive(&self) -> bool {
    Instant::now() < self.time_to_die
  }

  pub fn update_ttl(&mut self) {
    self.time_to_die = Blob::get_next_ttl();
  }

  fn get_next_ttl() {
    Instant::now() + Duration::from_secs(BLOB_TTL_SECONDS)
  }

  pub fn consume(&mut self, bytes: &mut [u8]) {
    self.array.append(bytes);
    self.hash.input(bytes);
  }
}

pub struct BlobReceiver<I, C, R> where
                        I: FnMut(&str),
                        C: FnMut(&str, &[u8]),
                        R: FnMut(&Self) -> bool {
  main_thread: Option<thread::Thread>,
  bind_address: String,
  chunk_size: usize,
  widToBlob: HashMap<String, Blob>,
  ctx: zmq::Context,
  sock: zmq::Socket,
  tx: ChannelSender,
  rx: ChannelReceiver,
  pub on_info: I,
  pub on_complete: C,
  pub on_ready: R,
}

impl BlobReceiver {
  pub fn new(bind_address: &str, chunk_size: usize) -> Result<BlobReceiver, XactError> {
    let mut ctx = zmq::Context::new();  // TODO set threads to 2
    let mut sock = try!(ctx.socket(zmq::ROUTER));
    try!(sock.set_linger(0));
    try!(sock.set_maxmsgsize(chunk_size as i64 + MSG_PADDING));
    try!(sock.set_rcvhwm(MAX_SIMUL_CHUNKS));
    try!(sock.bind(bind_address));
    debug!("Bound interface: {}", bind_address);

    let (tx, rx) = channel();

    Ok(BlobReceiver {
      main_thread: None,
      bind_address: bind_address.to_owned(),
      chunk_size: chunk_size,
      widToBlob: HashMap::new(),
      ctx: ctx,
      sock: sock,
      tx: tx,
      rx: rx,
      on_info: |s| { debug!("{}", s) },
      on_complete: |s, v| { debug!("Blob id: {} complete. Size: {} bytes.", s, v.len()); },
      on_ready: |_| { true }
    })
  }

  pub fn start(&mut self) {
    self.main_thread = Some(thread::spawn(move || {
      self.run();
    }));
  }

  pub fn stop(&mut self) {
    if self.rx.send(STOP).is_err() {
      debug!("BlobReceiver already dead.");
    }
  }

  fn run(&mut self) {
    let mut running = true;
    while running {
      self.prune_dead_blobs();
      self.send_cons_msgs();

      if try!(self.sock.poll(50, zmq::POLLIN)) == 0 {
        continue;
      }

      let [wid, msg_type, args] = try!(self.sock.recv_multipart());
    }
  }
}
