use zmq;

use std::str;
use std::error::Error;
use std::fmt;
use std::cmp;
use std::time::{Duration, Instant};

use serialize::hex::ToHex;
use rustc::util::sha2::{Sha256, Digest};

use super::{ErrorKind, XactError};

pub struct Blob {
  id: String,
  array: Vec<u8>,
  index: usize,
  hash: Sha256,
  time_to_die: Instant
}


pub struct BlobReceiver {

}
