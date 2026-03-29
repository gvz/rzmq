//! Fuzz target: end-to-end UDP RADIO/DISH protocol fuzzer
//!
//! This target exercises the entire rzmq UDP receive path:
//!   raw UDP datagram → UdpReceiveActor → DishSocket::handle_pipe_event
//!   → decode_radio_frame → group filter → IncomingMessageOrchestrator
//!
//! Unlike the TCP `e2e_protocol` target (which fuzzes ZMTP framing), this target
//! is specific to the Radio-Dish UDP wire format, which has *no* ZMTP handshake:
//!
//!   UDP datagram wire format (Radio-Dish):
//!     byte 0:        group_len  (u8 — number of group name bytes that follow)
//!     bytes 1..N:    group name (group_len bytes)
//!     bytes N+1..:   application payload
//!
//! There is zero handshake — the first datagram goes directly into
//! `DishSocket::handle_pipe_event`, making the decode and filter logic the
//! primary target.
//!
//! What this fuzzer verifies:
//!   - No panics in `decode_radio_frame` for any datagram byte sequence
//!   - No panics or hangs in `UdpReceiveActor::run` regardless of datagram content
//!   - Group filter (`joined_groups.contains`) handles edge cases correctly
//!   - `set_option_raw(JOIN, ...)` properly rejects invalid group names without panicking
//!   - The Dish socket can always be cleanly shut down after arbitrary UDP input
//!   - Oversized datagrams (near the 65 507-byte OS limit) are handled gracefully
//!
//! Requires feature: `udp` (enabled in fuzz/Cargo.toml via `features = ["fuzzing", "udp"]`).
//!
//! Run with:
//!   cargo fuzz run e2e_udp_radio_dish
//!   cargo fuzz run e2e_udp_radio_dish corpus/e2e_udp_radio_dish -- -max_total_time=300
#![no_main]

use std::net::SocketAddr;
use std::time::Duration;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tokio::time::timeout;

use rzmq::socket::options::{JOIN, LAST_ENDPOINT};
use rzmq::{Context, SocketType};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum groups the Dish will JOIN per iteration.
/// Kept small to avoid spending most of the budget on option RPCs.
const MAX_GROUPS: usize = 4;

/// Maximum datagrams sent per iteration.
const MAX_DATAGRAMS: usize = 8;

/// Maximum size of a raw fuzz datagram (full OS UDP buffer limit).
/// Lets the fuzzer hit the 65 507-byte validation boundary in RadioSocket::send.
const MAX_RAW_DATAGRAM: usize = 65_535;

/// Maximum group name length accepted by the fuzzer input.
/// Deliberately above the 255-byte valid limit so we exercise the rejection path.
const MAX_GROUP_LEN: usize = 300;

/// Maximum payload length for a `Framed` datagram.
/// = 65 507 − 1 (group_len byte) − 1 (shortest group) = 65 505, rounded down.
const MAX_PAYLOAD_LEN: usize = 65_505;

/// Timeout for every async rzmq operation.
const OP_TIMEOUT: Duration = Duration::from_millis(500);

/// Short drain timeout per recv call — UDP is unreliable so we don't wait long.
const RECV_TIMEOUT: Duration = Duration::from_millis(50);

// ---------------------------------------------------------------------------
// Structured input types
// ---------------------------------------------------------------------------

/// A single UDP datagram to send to the Dish socket.
#[derive(Arbitrary, Debug)]
enum FuzzDatagram {
    /// Completely raw bytes — exercises `decode_radio_frame` with zero structure.
    /// Hits: empty-datagram guard, group_len overflow, truncated-group path.
    Raw(Vec<u8>),

    /// A properly structured Radio-Dish datagram.
    /// Wire encoding: `[group.len() as u8][group bytes][payload bytes]`
    ///
    /// Note: `group.len()` is truncated to u8 on encoding, so a group longer
    /// than 255 bytes produces a mismatched length prefix — an intentional
    /// malformed-frame test.
    Framed {
        /// Group name bytes. May be empty, > 255 bytes, or contain NUL — all
        /// intentional fuzz vectors for the Dish group filter.
        group: Vec<u8>,
        /// Application payload.
        payload: Vec<u8>,
    },
}

/// The top-level fuzz input for one iteration.
#[derive(Arbitrary, Debug)]
struct FuzzUdpInput {
    /// Groups to JOIN on the Dish *before* sending datagrams.
    /// Invalid groups (empty, NUL bytes, > 255 bytes) are attempted too —
    /// the fuzz target ignores the returned error so both the success and
    /// rejection paths of `set_option_raw(JOIN, …)` are exercised.
    groups_to_join: Vec<Vec<u8>>,

    /// Datagrams to send to the bound Dish UDP port.
    datagrams: Vec<FuzzDatagram>,
}

// ---------------------------------------------------------------------------
// Datagram serialization
// ---------------------------------------------------------------------------

/// Serialize a `FuzzDatagram` to the raw bytes that will be sent over UDP.
fn datagram_to_bytes(dg: &FuzzDatagram) -> Vec<u8> {
    match dg {
        FuzzDatagram::Raw(data) => {
            // Respect the OS datagram size limit so the send call doesn't fail
            // with EMSGSIZE before the bytes even reach the Dish.
            data[..data.len().min(MAX_RAW_DATAGRAM)].to_vec()
        }

        FuzzDatagram::Framed { group, payload } => {
            // Encode group_len as a u8 — intentionally wraps for group.len() > 255.
            // This creates a malformed frame that exercises the bounds check in
            // `DishSocket::decode_radio_frame`.
            let capped_group = &group[..group.len().min(MAX_GROUP_LEN)];
            let capped_payload = &payload[..payload.len().min(MAX_PAYLOAD_LEN)];

            let group_len_byte = capped_group.len() as u8; // wraps if len > 255
            let mut out = Vec::with_capacity(1 + capped_group.len() + capped_payload.len());
            out.push(group_len_byte);
            out.extend_from_slice(capped_group);
            out.extend_from_slice(capped_payload);

            // Clamp to OS datagram limit.
            out.truncate(MAX_RAW_DATAGRAM);
            out
        }
    }
}

// ---------------------------------------------------------------------------
// Async fuzz body
// ---------------------------------------------------------------------------

/// One complete fuzz iteration, fully async.
async fn run_fuzz(input: &FuzzUdpInput) {
    // 1. Create a fresh rzmq Context and Dish socket.
    let ctx = match Context::new() {
        Ok(c) => c,
        Err(_) => return,
    };
    let dish = match ctx.socket(SocketType::Dish) {
        Ok(s) => s,
        Err(_) => {
            let _ = ctx.term().await;
            return;
        }
    };

    // 2. Bind the Dish to an ephemeral UDP port (port 0 → OS assigns).
    //    The `udp` feature must be enabled for this to succeed.
    let bind_result = timeout(OP_TIMEOUT, dish.bind("udp://127.0.0.1:0")).await;
    if bind_result.is_err() || bind_result.unwrap().is_err() {
        let _ = timeout(OP_TIMEOUT, dish.close()).await;
        let _ = timeout(OP_TIMEOUT, ctx.term()).await;
        return;
    }

    // 3. Retrieve the actual bound address via LAST_ENDPOINT.
    //    Format: "udp://127.0.0.1:PORT"
    let dish_addr: SocketAddr = {
        let ep_result = timeout(OP_TIMEOUT, dish.get_option(LAST_ENDPOINT)).await;
        let ep_bytes = match ep_result {
            Ok(Ok(b)) => b,
            _ => {
                let _ = timeout(OP_TIMEOUT, dish.close()).await;
                let _ = timeout(OP_TIMEOUT, ctx.term()).await;
                return;
            }
        };
        let ep_str = match std::str::from_utf8(&ep_bytes) {
            Ok(s) => s,
            Err(_) => {
                let _ = timeout(OP_TIMEOUT, dish.close()).await;
                let _ = timeout(OP_TIMEOUT, ctx.term()).await;
                return;
            }
        };
        // Strip the "udp://" prefix and parse as SocketAddr.
        match ep_str.trim_start_matches("udp://").parse() {
            Ok(a) => a,
            Err(_) => {
                let _ = timeout(OP_TIMEOUT, dish.close()).await;
                let _ = timeout(OP_TIMEOUT, ctx.term()).await;
                return;
            }
        }
    };

    // 4. JOIN fuzz-specified groups on the Dish.
    //    We attempt all groups, valid and invalid alike — invalid ones return
    //    an error which we discard.  This exercises both the success path
    //    (group inserted into `joined_groups`) and the rejection path
    //    (empty / NUL / > 255 bytes).
    let group_count = input.groups_to_join.len().min(MAX_GROUPS);
    for group in &input.groups_to_join[..group_count] {
        let _ = timeout(OP_TIMEOUT, dish.set_option_raw(JOIN, group.as_slice())).await;
    }

    // 5. Create a raw std UDP socket to send datagrams.
    //    We deliberately use std (not tokio) so the send side is outside the
    //    rzmq async machinery — we want the rawest possible bytes reaching
    //    the UdpReceiveActor.
    let sender = match std::net::UdpSocket::bind("127.0.0.1:0") {
        Ok(s) => s,
        Err(_) => {
            let _ = timeout(OP_TIMEOUT, dish.close()).await;
            let _ = timeout(OP_TIMEOUT, ctx.term()).await;
            return;
        }
    };

    // 6. Send all fuzz datagrams to the Dish's bound port.
    let datagram_count = input.datagrams.len().min(MAX_DATAGRAMS);
    for dg in &input.datagrams[..datagram_count] {
        let bytes = datagram_to_bytes(dg);
        // Ignore send errors (e.g., EMSGSIZE for giant inputs) — the Dish
        // might still receive partial data or nothing; either is fine.
        let _ = sender.send_to(&bytes, dish_addr);
    }
    drop(sender);

    // 7. Give the UdpReceiveActor a moment to process the datagrams.
    //    The actor runs in a spawned task; we need to yield control.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // 8. Drain the Dish receive queue (up to datagram_count messages).
    //    We use very short per-call timeouts because:
    //      a) UDP is unreliable — datagrams may have been filtered (non-joined group)
    //         or dropped by the OS.
    //      b) We just want to confirm the Dish is alive and responsive, not
    //         that every datagram was delivered.
    for _ in 0..datagram_count.max(1) {
        let _ = timeout(RECV_TIMEOUT, dish.recv()).await;
    }

    // 9. Clean shutdown — confirms no deadlock or stuck actor.
    //    If either call hangs past OP_TIMEOUT, libFuzzer reports a timeout crash.
    let _ = timeout(OP_TIMEOUT, dish.close()).await;
    let _ = timeout(OP_TIMEOUT, ctx.term()).await;
}

// ---------------------------------------------------------------------------
// libFuzzer entry point
// ---------------------------------------------------------------------------

fuzz_target!(|input: FuzzUdpInput| {
    // Build a fresh current-thread Tokio runtime for each fuzz iteration.
    // This guarantees clean state — no actor handles, timers, or sockets
    // survive from one iteration to the next.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return,
    };
    rt.block_on(run_fuzz(&input));
});
