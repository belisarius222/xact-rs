extern crate xact;

#[macro_use]
extern crate log;

use std::error::Error;  // So we can use e.description()
use std::time::Duration;

#[test]
fn send_small_string() {
  match xact::send_binary_blob("tcp://127.0.0.1:1234", "msg-0", "ermahgerd".as_bytes(), Duration::from_millis(2000), false, |s| { info!("{}", s) }) {
    Ok(result_bytes) => { info!("Result: {:?}", result_bytes); },
    Err(e) => {
      error!("Error: {}", e.description());
      panic!(e)
    }
  };
}

#[test]
fn send_big_vec() {
  match xact::send_binary_blob("tcp://127.0.0.1:1234", "msg-1", vec![0x2a as u8; 1e8 as usize].as_slice(), Duration::from_millis(20000), false, |s| { info!("{}", s) }) {
    Ok(result_bytes) => { info!("Result: {:?}", result_bytes); },
    Err(e) => {
      error!("Error: {}", e.description());
      panic!(e)
    }
  };
}
