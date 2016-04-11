# xact-rs
Rust implementation of the XACT protocol.

Note: currently requires the nightly build of the rustc compiler so we can use its SHA256 algorithm.
We should probably switch over to the [octavo](https://github.com/libOctavo/octavo) crypto library instead.

Also, the linker dies on OS X when attempting to link with the rust-zmq library. Works fine on linux.
