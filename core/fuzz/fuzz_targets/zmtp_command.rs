//! Fuzz target: ZmtpCommand::parse and ZmtpReady::parse_properties
//!
//! Constructs a COMMAND-flagged Msg from arbitrary bytes and parses it. Covers:
//! - PING: previously only guarded body.len() >= 5 before slicing body[7..]
//!   (fixed — guard is now >= 7); this target acts as a regression guard
//! - PONG, READY, ERROR, JOIN, LEAVE: all dispatch paths
//! - ZmtpReady::parse_properties: u32 value_len up to ~4 GB per property;
//!   verifies no OOM allocation occurs for crafted inputs
//! - Unknown commands: catch-all branch
//!
//! Run with:
//!   cargo fuzz run zmtp_command
#![no_main]

use libfuzzer_sys::fuzz_target;
use rzmq::{Msg, MsgFlags, ZmtpCommand};

fuzz_target!(|data: &[u8]| {
  // Build a COMMAND-flagged message from the raw fuzz input.
  let mut msg = Msg::from_vec(data.to_vec());
  msg.set_flags(MsgFlags::COMMAND);
  // Must never panic regardless of content.
  let _ = ZmtpCommand::parse(&msg);
});
