//! Fuzz target: ZmtpGreeting::decode
//!
//! Feeds arbitrary bytes into the ZMTP greeting parser. The greeting is a
//! fixed 64-byte handshake frame with strict format requirements. Verifies:
//! - No panics for any input length or byte pattern
//! - Invalid greetings always produce clean Err results, never panics
//! - All validation paths (bad signature, wrong version, bad as_server byte,
//!   non-zero padding) are exercised
//!
//! Run with:
//!   cargo fuzz run zmtp_greeting
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rzmq::protocol::zmtp::ZmtpGreeting;

fuzz_target!(|data: &[u8]| {
  let mut buf = BytesMut::from(data);
  // Must never panic. Returns Ok(None) if < 64 bytes, Ok(Some) or Err otherwise.
  let _ = ZmtpGreeting::decode(&mut buf);
});
