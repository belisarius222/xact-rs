#![feature(rustc_private)]

extern crate xact;

use xact::sender::{send_binary_blob};
use xact::receiver::{BlobReceiver, BasicBlobReceiverBehavior, DEFAULT_CHUNK_SIZE, STOP};

#[macro_use]
extern crate log;

use std::error::Error;  // So we can use e.description()
use std::thread;
use std::time::Duration;
use std::sync::mpsc::channel;

#[test]
#[ignore]
fn send_small_string() {
  match send_binary_blob("tcp://127.0.0.1:1234", "msg-0", "ermahgerd".as_bytes(), Duration::from_millis(2000), false, |s| { info!("{}", s) }) {
    Ok(result_bytes) => { info!("Result: {:?}", result_bytes); },
    Err(e) => {
      error!("Error: {}", xact::XactError::description(&e));
      panic!(e)
    }
  };
}

#[test]
#[ignore]
fn send_big_vec() {
  match send_binary_blob("tcp://127.0.0.1:1234", "msg-1", vec![0x2a as u8; 1e8 as usize].as_slice(), Duration::from_millis(20000), false, |s| { info!("{}", s) }) {
    Ok(result_bytes) => { info!("Result: {:?}", result_bytes); },
    Err(e) => {
      error!("Error: {}", xact::XactError::description(&e));
      panic!(e)
    }
  };
}

#[test]
fn recv_big_vec() {
  let (tx, rx) = channel();

  let recv_handle = thread::spawn(move || {
    let behavior = BasicBlobReceiverBehavior {};
    let mut receiver = BlobReceiver::new("tcp://*:1234", DEFAULT_CHUNK_SIZE, behavior).unwrap();
    receiver.run(rx);
  });

  match send_binary_blob("tcp://127.0.0.1:1234", "msg-1", vec![0x2a as u8; 1e8 as usize].as_slice(), Duration::from_millis(20000), false, |s| { info!("{}", s) }) {
    Ok(result_bytes) => { info!("Result: {:?}", result_bytes); },
    Err(e) => {
      error!("Error: {}", xact::XactError::description(&e));
      panic!(e)
    }
  };

  tx.send(STOP);
  recv_handle.join().unwrap();
}
