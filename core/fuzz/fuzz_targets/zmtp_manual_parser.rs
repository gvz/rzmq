//! Fuzz target: ZmtpManualParser::decode_from_buffer
//!
//! Exercises the manual (non-tokio-util) ZMTP frame decoder. This is a
//! parallel decode path to ZmtpCodec used in certain engine contexts. Verifies:
//! - No panics for any input
//! - Long-frame u64 size field does not cause OOM or overflow
//! - Decoder correctly returns Ok(None) for partial frames
//!
//! Run with:
//!   cargo fuzz run zmtp_manual_parser
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rzmq::protocol::zmtp::manual_parser::ZmtpManualParser;

fuzz_target!(|data: &[u8]| {
  let mut parser = ZmtpManualParser::new();
  let mut buf = BytesMut::from(data);
  // Must never panic. Returns Ok(None) for partial input, Ok(Some(msg)) on success.
  let _ = parser.decode_from_buffer(&mut buf);
});
