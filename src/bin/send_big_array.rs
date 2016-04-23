#![feature(rustc_private)]

extern crate xact;
use xact::sender::{send_binary_blob};

#[macro_use]
extern crate log;

use std::error::Error;  // So we can use e.description()
use std::time::Duration;

fn main() {
  match send_binary_blob("ipc:///tmp/testing.ipc", "msg-1", vec![0x2a as u8; 1e8 as usize].as_slice(), Duration::from_millis(20000), false, |s| { info!("{}", s) }) {
    Ok(result_bytes) => { info!("Result: {:?}", result_bytes); },
    Err(e) => {
      error!("Error: {}", xact::XactError::description(&e));
      panic!(e)
    }
  };
}
