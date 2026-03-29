//! Fuzz target: ZmtpCodec::decode
//!
//! Feeds arbitrary bytes into the ZMTP frame decoder. Verifies that:
//! - No panics occur for any input
//! - OOM is not triggered by a crafted long-frame size field (u64::MAX)
//! - The decoder always returns Ok(Some), Ok(None), or Err — never panics
//!
//! Run with:
//!   cargo fuzz run zmtp_codec
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use rzmq::protocol::zmtp::ZmtpCodec;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
  let mut codec = ZmtpCodec::new();
  let mut buf = BytesMut::from(data);
  // Must never panic. May return Ok(None) (need more data), Ok(Some(msg)), or Err.
  let _ = codec.decode(&mut buf);
});
