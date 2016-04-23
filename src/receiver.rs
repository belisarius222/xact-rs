use zmq;

use std::str;
use std::error::Error;
use std::fmt;
use std::cmp;
use std::time::{Duration, Instant};

use serialize::hex::ToHex;
use rustc::util::sha2::{Sha256, Digest};

use super::{bytes_to_int, ErrorKind, int_to_bytes, XactError};

use std::thread;
use std::sync::mpsc::{channel, SendError};
use std::sync::mpsc::Receiver as ChannelReceiver;
use std::sync::mpsc::Sender as ChannelSender;
use std::collections::HashMap;
use std::marker::{Send, Sized};

const BLOB_TTL_SECONDS: u64 = 10;
pub const DEFAULT_CHUNK_SIZE: usize = 1e7 as usize;
const MAX_SIMUL_CHUNKS: usize = 10;
const MSG_PADDING: usize = 100;
pub const STOP: bool = true;

pub struct Blob {
  pub id: Vec<u8>,
  pub array: Vec<u8>,
  pub hash: Sha256,
  time_to_die: Instant
}

impl Blob {
  pub fn new(id: &[u8], array_size: usize) -> Blob {
    let mut hash = Sha256::new();
    Blob {
      id: id.to_vec(),
      array: Vec::with_capacity(array_size),
      hash: hash,
      time_to_die: Blob::get_next_ttl()
    }
  }

  pub fn is_alive(&self) -> bool {
    Instant::now() < self.time_to_die
  }

  pub fn update_ttl(&mut self) {
    self.time_to_die = Blob::get_next_ttl();
  }

  fn get_next_ttl() -> Instant {
    Instant::now() + Duration::from_secs(BLOB_TTL_SECONDS)
  }

  pub fn consume(&mut self, bytes: &[u8]) {
    self.array.extend_from_slice(&bytes);
    self.hash.input(&bytes);
    self.update_ttl();
  }
}

pub trait BlobReceiverBehavior {
  fn on_ready(&mut self, data_size: usize) -> bool;
  fn on_info(&mut self, msg: &str);
  fn on_complete(&mut self, id: &[u8], array: &[u8]);
}

pub struct BasicBlobReceiverBehavior;

impl BlobReceiverBehavior for BasicBlobReceiverBehavior {
  fn on_ready(&mut self, data_size: usize) -> bool {
    true
  }

  fn on_info(&mut self, msg: &str) {
    debug!("{}", msg);
  }

  fn on_complete(&mut self, id: &[u8], array: &[u8]) {
    debug!("Blob id: {:?} complete. Size: {} bytes.", id, array.len());
  }
}

pub struct BlobReceiver<'a> {
  pub bind_address: String,
  pub chunk_size: usize,
  blobs: HashMap<Vec<u8>, Blob>,  // sender_id to blob
  ctx: zmq::Context,
  sock: zmq::Socket,
  pub behavior: Box<BlobReceiverBehavior + 'a>
}

// TODO: Merge this with the Drop impl for TimedZMQTransaction.
impl<'a> Drop for BlobReceiver<'a> {
  fn drop(&mut self) {
    match self.sock.close() {
      Ok(()) => { debug!("Socket dropped") },
      Err(e) => panic!(e)
    }

    debug!("dropping context.");
    let mut e = self.ctx.destroy();
    while e == Err(zmq::Error::EINTR) {
      e = self.ctx.destroy();
    }
  }
}

impl<'a> BlobReceiver<'a> {
  pub fn new<B: BlobReceiverBehavior + 'a>(bind_address: &str, chunk_size: usize, b: B) -> Result<BlobReceiver<'a>, XactError> {
    let mut ctx = zmq::Context::new();  // TODO set threads to 2
    let mut sock = try!(ctx.socket(zmq::ROUTER));
    try!(sock.set_linger(0));
    try!(sock.set_maxmsgsize((chunk_size + MSG_PADDING) as i64));
    try!(sock.set_rcvhwm(MAX_SIMUL_CHUNKS as i32));
    try!(sock.bind(bind_address));
    debug!("Bound interface: {}", bind_address);

    Ok(BlobReceiver {
      bind_address: bind_address.to_owned(),
      chunk_size: chunk_size,
      blobs: HashMap::new(),
      ctx: ctx,
      sock: sock,
      behavior: Box::new(b)
    })
  }

  pub fn run(&mut self, stop_rx: ChannelReceiver<bool>) {
    loop {
      self.prune_dead_blobs();
      self.send_cons_msgs();

      let poll_result = self.sock.poll(zmq::POLLIN, 50);
      if poll_result.is_err() && poll_result.unwrap() == 0 {
        continue;
      }

      // NOTE: This will panic if the socket receives an error.
      let responses = self.sock.recv_multipart(0).unwrap();

      let responses_map: Vec<&[u8]> = responses.iter().map(|p| p.as_slice()).collect();

      match responses_map.as_slice() {
        [ref sender_id, b"PING"] => {
          debug!("RECV PING");
          self.do_ping(&sender_id);
        },
        [ref sender_id, b"START", ref blob_id, ref data_size] => {
          debug!("RECV START");
          self.do_start(&sender_id, &blob_id, &data_size);
        },
        [ref sender_id, b"CHUNK", ref bytes] => {
          debug!("RECV CHUNK");
          self.do_chunk(&sender_id, &bytes);
        },
        [ref sender_id, b"END", ref hash_bytes] => {
          debug!("RECV END");
          self.do_end(&sender_id, &hash_bytes);
        },
        parts => {
          debug!("Wrong number of responses: {}", parts.len());
        }
      };

      if stop_rx.try_recv().is_ok() {
        self.behavior.on_info("Received shutdown signal. Exiting.");
        break;
      }
    }
  }

  fn prune_dead_blobs(&mut self) {
    let mut blobs = &mut self.blobs;

    let keys_to_remove = blobs.keys()
                              .map(|k| k.to_owned())
                              .filter(|sender_id| {
      let blob = blobs.get(sender_id).unwrap();
      !blob.is_alive()
    }).collect::<Vec<Vec<u8>>>();

    for key in keys_to_remove {
      debug!("Removing dead blob: {:?}", key);
      blobs.remove(&key);
    }
  }

  fn send_cons_msgs(&mut self) {

  }

  fn do_ping(&mut self, sender_id: &[u8]) {
    self.sock.send_multipart(&[sender_id, b"", b"PONG"], 0).unwrap_or_else(|e| {
      debug!("Error responding to PING: {:?}", e);
    });
  }

  fn do_start(&mut self, sender_id: &[u8], blob_id: &[u8], data_size: &[u8]) {
    let parse_result = bytes_to_int(data_size);
    if parse_result.is_err() {
      debug!("Invalid START request. Aborting transaction.");
      self.abort_transaction(&sender_id);
      return;
    }
    let data_size = parse_result.unwrap();

    if !self.behavior.on_ready(data_size) {
      self.sock.send_multipart(&[sender_id, b"", b"NOGO", b"0"], 0).unwrap_or_else(|_| {
        debug!("Error sending NOGO message. Ignoring.");
      });
      self.behavior.on_info("Not ready. NOGO sent.");
      return;
    }

    let blob = Blob::new(&blob_id, data_size);
    // Do this in a new scope to allow more mutable borrows of self later.
    {
      let mut blobs = &mut self.blobs;
      blobs.insert(sender_id.to_vec(), blob);
    }
    self.behavior.on_info("Created new blob.");

    let chunk_size_vec = int_to_bytes(self.chunk_size);
    let chunk_size_bytes = chunk_size_vec.as_slice();

    let send_result = self.sock.send_multipart(&[sender_id, b"", b"GOGO", chunk_size_bytes], 0);
    send_result.unwrap_or_else(|e| {
      let err_msg = format!("Error sending GOGO message: {:?}. Aborting transaction.", e);
      self.behavior.on_info(&err_msg);
      self.abort_transaction(&sender_id);
      return;
    });

    self.request_chunks(&sender_id, MAX_SIMUL_CHUNKS);
  }

  fn do_chunk(&mut self, sender_id: &[u8], bytes: &[u8]) {
    if !self.blobs.contains_key(&sender_id.to_vec()) {
      debug!("Chunk with invalid sender_id: {:?}", &sender_id);
      return;
    }

    // Do this in a new scope to allow more mutable borrows of self later.
    {
      let mut blob = self.blobs.get_mut(&sender_id.to_vec()).unwrap();
      blob.consume(&bytes);
    }
    self.behavior.on_info("Appended chunk to blob.");

    self.request_chunks(&sender_id, 1);
  }

  fn do_end(&mut self, sender_id: &[u8], hash_bytes: &[u8]) {
    let blob_or_none = self.blobs.remove(&sender_id.to_vec());
    if blob_or_none.is_none() {
      let msg = format!("END with invalid sender_id: {:?}. Ignoring.", &sender_id);
      self.behavior.on_info(&msg);
      return;
    }
    let mut blob = blob_or_none.unwrap();

    self.behavior.on_info("Checking hash.");
    let blob_hash_str = blob.hash.result_bytes().to_hex();
    let blob_hash = blob_hash_str.as_bytes();
    if hash_bytes != blob_hash {
      self.behavior.on_info("Checksum wrong. Sending FAIL.");
      self.sock.send_multipart(&[sender_id, b"", b"FAIL", b"Hash mismatch"], 0).unwrap_or_else(|_| ());
      self.abort_transaction(&sender_id);
      return;
    }

    self.sock.send_multipart(&[sender_id, b"", b"OK", b"Great success"], 0).unwrap_or_else(|e| {
      debug!("OK message failed to send. Error: {:?}", e);
    });
    self.behavior.on_info("Sent OK.");

    self.behavior.on_info("Queueing completion action.");
    self.behavior.on_complete(&sender_id, &blob.array);
  }

  fn request_chunks(&mut self, sender_id: &[u8], num_chunks: usize) {
    for i in 0..num_chunks {
      if let Err(e) = self.sock.send_multipart(&[sender_id, b"", b"TOKEN"], 0) {
        debug!("Chunk {} failed to send. Error: {:?}", i, e);
        return;
      }
      self.behavior.on_info("Requested chunk.");
    }
  }

  fn abort_transaction(&mut self, sender_id: &[u8]) {
    debug!("Aborting transaction, sender_id: {:?}", sender_id);
    let mut blobs = &mut self.blobs;
    blobs.remove(&sender_id.to_vec());
  }
}
