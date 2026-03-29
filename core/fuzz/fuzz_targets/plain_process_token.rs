//! Fuzz target: PlainMechanism::process_token
//!
//! Feeds arbitrary bytes as a PLAIN security token into both the server-side
//! and client-side state machines. Tests the full token parsing path:
//! - command_name_len = token[0], slice token[1..1+command_name_len]
//!   (bounds-checked; this target acts as a regression guard)
//! - HELLO body: username/password length-prefixed fields
//! - WELCOME / ERROR handling on the client side
//! - All malformed inputs must return Err, never panic
//!
//! Run with:
//!   cargo fuzz run plain_process_token
#![no_main]

use libfuzzer_sys::fuzz_target;
use rzmq::{Mechanism, PlainMechanism};

fuzz_target!(|data: &[u8]| {
  // Test server side: expects a HELLO token.
  let mut server = PlainMechanism::new(true);
  server.set_server_expected_credentials(Some(b"user".to_vec()), Some(b"pass".to_vec()));
  let _ = server.process_token(data);

  // Test client side (in ClientExpectWelcome state): expects WELCOME or ERROR.
  // Advance client to the waiting state first by producing the HELLO token.
  let mut client = PlainMechanism::new(false);
  client.set_client_credentials(Some(b"user".to_vec()), Some(b"pass".to_vec()));
  // produce_token moves state to ClientExpectWelcome
  let _ = client.produce_token();
  let _ = client.process_token(data);
});
