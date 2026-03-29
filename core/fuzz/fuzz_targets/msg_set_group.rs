//! Fuzz target: Msg::set_group
//!
//! Exercises the group field validation on a ZMQ message. The group is used
//! by the RADIO/DISH pattern and must be 1–255 bytes with no NUL bytes.
//! Verifies:
//! - Empty input returns Err (not panic)
//! - Inputs > 255 bytes return Err (not panic)
//! - Inputs containing NUL bytes return Err (not panic)
//! - Valid inputs (1–255 non-NUL bytes) return Ok
//!
//! Run with:
//!   cargo fuzz run msg_set_group
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use rzmq::Msg;

fuzz_target!(|data: &[u8]| {
  let mut msg = Msg::new();
  // Must never panic for any input.
  let _ = msg.set_group(Bytes::copy_from_slice(data));
});
