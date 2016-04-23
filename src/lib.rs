#![feature(rustc_private, slice_patterns)]

extern crate zmq;

#[macro_use]
extern crate log;

use std::str;
use std::error::Error;
use std::fmt;
use std::cmp;
use std::time::{Duration, Instant};

extern crate serialize;
use serialize::hex::ToHex;

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
pub struct XactError {
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
  fn new(kind: ErrorKind, msg: &str) -> XactError {
    let full_desc = format!("Error of type: {}, msg: '{}'", kind, msg);
    XactError {
      kind: kind,
      msg: String::from(msg),
      full_desc: full_desc
    }
  }

  fn from_zmq(e: zmq::Error, msg: &str) -> XactError {
    XactError::new(ErrorKind::ZMQ_ERROR(e), msg)
  }
}

impl From<zmq::Error> for XactError {
  fn from(e: zmq::Error) -> Self {
    XactError::new(ErrorKind::ZMQ_ERROR(e), e.description())
  }
}

pub fn bytes_to_int(bytes: &[u8]) -> Result<usize, XactError> {
  let int_str = try!(str::from_utf8(bytes).map_err(|_| {
    XactError::new(ErrorKind::INVALID_RESPONSE, "Unable to parse bytes as utf-8")
  }));

  let res = try!(int_str.parse::<usize>().map_err(|_| {
    XactError::new(ErrorKind::INVALID_RESPONSE, "Unable to parse string as integer")
  }));

  Ok(res)
}

pub fn int_to_bytes(num: usize) -> Vec<u8> {
  format!("{}", num).as_bytes().to_vec()
}

pub mod sender;
pub mod receiver;
