#![feature(rustc_private)]

extern crate xact;
use xact::receiver::{BlobReceiver, BasicBlobReceiverBehavior, DEFAULT_CHUNK_SIZE, STOP};

#[macro_use]
extern crate log;

use std::sync::mpsc::channel;

fn main() {
  let behavior = BasicBlobReceiverBehavior {};
  let mut receiver = BlobReceiver::new("ipc:///tmp/testing.ipc", DEFAULT_CHUNK_SIZE, behavior).unwrap();
  let (tx, rx) = channel();
  receiver.run(rx);
}
