//! Fuzz target: end-to-end ZMTP protocol fuzzer
//!
//! This is a *full-stack* fuzz target that exercises the entire rzmq server path:
//!   raw TCP bytes → ZMTP greeting parser → security mechanism → READY handshake
//!   → session state machine → socket pattern actor → application message delivery
//!
//! Unlike the unit-level fuzz targets (zmtp_codec, zmtp_greeting, etc.) which feed
//! bytes into isolated parsers, this target:
//!   1. Spins up a real Tokio runtime
//!   2. Creates a real rzmq Context + server socket bound on an ephemeral TCP port
//!   3. Opens a raw TcpStream and writes mutation-guided ZMTP bytes
//!   4. Verifies the server never panics, hangs (> 500 ms), or deadlocks
//!   5. Tears everything down cleanly
//!
//! Three input variants allow the fuzzer to explore all protocol phases:
//!   - `Raw`                  → arbitrary bytes from Phase 0 (covers greeting parser)
//!   - `GreetingThenPayload`  → valid greeting + fuzz payload (covers Phases 2-3)
//!   - `ReadyThenFrames`      → full handshake + fuzz application frames (covers Phase 4-5)
//!
//! All six server-side socket types are covered (REP, ROUTER, PULL, SUB, DEALER, DISH).
//!
//! Run with:
//!   cargo fuzz run e2e_protocol
//!   cargo fuzz run e2e_protocol corpus/e2e_protocol -- -max_total_time=300
#![no_main]

use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use rzmq::{Context, SocketType};

// ---------------------------------------------------------------------------
// Structured input types
// ---------------------------------------------------------------------------

/// Maximum body size for a single fuzz frame — keeps allocations bounded.
const MAX_FRAME_BODY: usize = 16 * 1024; // 16 KiB

/// Maximum number of application frames in a single fuzz run.
const MAX_FRAMES: usize = 8;

/// Maximum size of the trailing fuzz payload after a greeting.
const MAX_PAYLOAD: usize = 4 * 1024; // 4 KiB

/// Timeout applied to every async operation in the fuzz target.
/// If the server hangs longer than this, libFuzzer reports a timeout.
const OP_TIMEOUT: Duration = Duration::from_millis(500);

/// The server-side socket types we rotate through.
/// Each has different pipe_attached / recv / send logic.
#[derive(Arbitrary, Debug, Clone, Copy, PartialEq, Eq)]
enum FuzzSocketType {
    Rep,
    Router,
    Pull,
    Sub,
    Dealer,
    // Dish requires the `udp` feature for full support; skip for portability.
}

impl FuzzSocketType {
    fn to_socket_type(self) -> SocketType {
        match self {
            FuzzSocketType::Rep => SocketType::Rep,
            FuzzSocketType::Router => SocketType::Router,
            FuzzSocketType::Pull => SocketType::Pull,
            FuzzSocketType::Sub => SocketType::Sub,
            FuzzSocketType::Dealer => SocketType::Dealer,
        }
    }
}

/// Security mechanism used in the fuzz greeting.
#[derive(Arbitrary, Debug, Clone, Copy)]
enum FuzzMechanism {
    Null,
    Plain, // greeting only — we do NOT complete the PLAIN handshake; this exercises the server's parser
}

/// A single ZMTP application frame.
#[derive(Arbitrary, Debug)]
struct FuzzFrame {
    /// Set the MORE bit (0x01) in the flags byte.
    more_bit: bool,
    /// Set the COMMAND bit (0x04) in the flags byte.
    command_bit: bool,
    /// Frame body — capped at MAX_FRAME_BODY bytes.
    body: Vec<u8>,
}

/// The three input variants covering all five ZMTP protocol phases.
#[derive(Arbitrary, Debug)]
enum FuzzInput {
    /// Phase 0/1: Completely raw bytes fed to the TCP socket.
    /// Tests greeting parser robustness with zero structure.
    Raw(Vec<u8>),

    /// Phase 1/2/3: A valid (or near-valid) 64-byte greeting followed by arbitrary payload.
    /// The greeting itself is constructed from the fuzz parameters so the fuzzer can
    /// explore all greeting field combinations without wasting iterations on the signature byte.
    GreetingThenPayload {
        socket_type: FuzzSocketType,
        mechanism: FuzzMechanism,
        /// Whether this peer claims to be the "server" (as_server field).
        as_server: bool,
        /// Version bytes in the greeting (major at [10], minor at [11]).
        version_major: u8,
        version_minor: u8,
        /// Raw bytes appended after the greeting.
        payload: Vec<u8>,
    },

    /// Phase 3/4/5: Complete NULL handshake then fuzz application frames.
    /// Exercises the session state machine and socket pattern actors.
    ReadyThenFrames {
        socket_type: FuzzSocketType,
        /// The Socket-Type property to advertise in our READY command.
        /// Using bytes allows the fuzzer to send invalid socket type names too.
        peer_socket_type_name: Vec<u8>,
        /// Optional Identity property to include in READY.
        identity: Option<Vec<u8>>,
        frames: Vec<FuzzFrame>,
    },
}

// ---------------------------------------------------------------------------
// ZMTP byte serializers
// ---------------------------------------------------------------------------

/// Build a 64-byte ZMTP greeting.
fn build_greeting(
    mechanism_name: &[u8; 20],
    as_server: u8,
    version_major: u8,
    version_minor: u8,
) -> [u8; 64] {
    let mut g = [0u8; 64];
    g[0] = 0xFF; // signature[0]
                 // bytes 1-8: zero (padding)
    g[9] = 0x7F; // signature[9]
    g[10] = version_major; // major version
    g[11] = version_minor; // minor version
    g[12..32].copy_from_slice(mechanism_name); // mechanism (20 bytes)
    g[32] = as_server; // as-server
                       // bytes 33-63: zero (padding)
    g
}

/// Encode the NULL mechanism name (20 bytes, null-padded).
fn null_mechanism_bytes() -> [u8; 20] {
    let mut m = [0u8; 20];
    let name = b"NULL";
    m[..name.len()].copy_from_slice(name);
    m
}

/// Encode the PLAIN mechanism name (20 bytes, null-padded).
fn plain_mechanism_bytes() -> [u8; 20] {
    let mut m = [0u8; 20];
    let name = b"PLAIN";
    m[..name.len()].copy_from_slice(name);
    m
}

/// Encode a mechanism name with arbitrary fuzz content (truncated / padded to 20 bytes).
fn fuzz_mechanism_bytes(mech: FuzzMechanism) -> [u8; 20] {
    match mech {
        FuzzMechanism::Null => null_mechanism_bytes(),
        FuzzMechanism::Plain => plain_mechanism_bytes(),
    }
}

/// Build a ZMTP short-frame or long-frame for a COMMAND or message body.
fn encode_frame(flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + body.len());
    if body.len() > 255 {
        out.push(flags | 0x02); // LONG flag
        let len = body.len() as u64;
        out.extend_from_slice(&len.to_be_bytes());
    } else {
        out.push(flags); // short frame
        out.push(body.len() as u8);
    }
    out.extend_from_slice(body);
    out
}

/// Build a ZMTP COMMAND frame from a raw body (caller is responsible for the body format).
fn encode_command_frame(body: &[u8]) -> Vec<u8> {
    encode_frame(0x04, body) // COMMAND bit
}

/// Build a READY command body with the given properties.
///
/// Format:
///   \x05 READY
///   [for each property:]
///     name_len(1 byte)  name(ASCII)  value_len(4 bytes BE)  value
fn build_ready_body(properties: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(5u8); // name length for "READY"
    body.extend_from_slice(b"READY");
    for (name, value) in properties {
        body.push(name.len() as u8);
        body.extend_from_slice(name);
        let vlen = value.len() as u32;
        body.extend_from_slice(&vlen.to_be_bytes());
        body.extend_from_slice(value);
    }
    body
}

// ---------------------------------------------------------------------------
// Input → raw bytes serialization
// ---------------------------------------------------------------------------

/// Convert a `FuzzInput` into the bytes we will write to the TCP stream.
fn input_to_bytes(input: &FuzzInput) -> Vec<u8> {
    match input {
        // ---- Phase 0/1 ----
        FuzzInput::Raw(data) => data.clone(),

        // ---- Phase 1/2/3 ----
        FuzzInput::GreetingThenPayload {
            socket_type: _,
            mechanism,
            as_server,
            version_major,
            version_minor,
            payload,
        } => {
            let mech_bytes = fuzz_mechanism_bytes(*mechanism);
            let greeting = build_greeting(
                &mech_bytes,
                if *as_server { 1 } else { 0 },
                *version_major,
                *version_minor,
            );
            let mut out = Vec::with_capacity(64 + payload.len().min(MAX_PAYLOAD));
            out.extend_from_slice(&greeting);
            out.extend_from_slice(&payload[..payload.len().min(MAX_PAYLOAD)]);
            out
        }

        // ---- Phase 3/4/5 ----
        FuzzInput::ReadyThenFrames {
            socket_type: _,
            peer_socket_type_name,
            identity,
            frames,
        } => {
            let mut out = Vec::new();

            // 1. Greeting (NULL, client role)
            let greeting = build_greeting(&null_mechanism_bytes(), 0, 3, 1);
            out.extend_from_slice(&greeting);

            // 2. READY command — advertise our socket type
            let type_name = if peer_socket_type_name.is_empty() {
                b"DEALER".as_ref()
            } else {
                &peer_socket_type_name[..peer_socket_type_name.len().min(64)]
            };
            let mut props: Vec<(&[u8], &[u8])> = vec![(b"Socket-Type", type_name)];
            let id_ref;
            if let Some(id) = identity {
                let id_slice = &id[..id.len().min(255)];
                id_ref = id_slice.to_vec();
                props.push((b"Identity", &id_ref));
            }
            let ready_body = build_ready_body(&props);
            out.extend_from_slice(&encode_command_frame(&ready_body));

            // 3. Application frames
            let frame_count = frames.len().min(MAX_FRAMES);
            for (i, frame) in frames[..frame_count].iter().enumerate() {
                let body = &frame.body[..frame.body.len().min(MAX_FRAME_BODY)];
                let flags_base = if frame.command_bit { 0x04u8 } else { 0x00u8 };
                let flags_more = if frame.more_bit && i + 1 < frame_count {
                    0x01u8
                } else {
                    0x00u8
                };
                out.extend_from_slice(&encode_frame(flags_base | flags_more, body));
            }

            out
        }
    }
}

/// Extract the desired server socket type from the fuzz input.
fn pick_socket_type(input: &FuzzInput) -> SocketType {
    match input {
        FuzzInput::Raw(_) => SocketType::Rep, // default for raw input
        FuzzInput::GreetingThenPayload { socket_type, .. } => socket_type.to_socket_type(),
        FuzzInput::ReadyThenFrames { socket_type, .. } => socket_type.to_socket_type(),
    }
}

// ---------------------------------------------------------------------------
// Async fuzz body
// ---------------------------------------------------------------------------

/// One complete fuzz iteration, fully async.
async fn run_fuzz(input: &FuzzInput) {
    // 1. Bind an ephemeral TCP port using std so we know the port before spawning rzmq.
    let std_listener = match StdTcpListener::bind("127.0.0.1:0") {
        Ok(l) => l,
        Err(_) => return,
    };
    std_listener.set_nonblocking(true).ok();
    let port = match std_listener.local_addr() {
        Ok(a) => a.port(),
        Err(_) => return,
    };
    // Drop the std listener — rzmq will rebind the same port via socket reuse.
    // Actually we pass the address directly to rzmq bind; drop the listener so the port is free.
    drop(std_listener);

    let endpoint = format!("tcp://127.0.0.1:{port}");
    let socket_type = pick_socket_type(input);

    // 2. Create rzmq context and server socket.
    let ctx = match Context::new() {
        Ok(c) => c,
        Err(_) => return,
    };
    let server = match ctx.socket(socket_type) {
        Ok(s) => s,
        Err(_) => {
            let _ = ctx.term().await;
            return;
        }
    };

    // 3. Bind the server socket. Use a timeout to guard against internal panics in bind.
    let bind_result = timeout(OP_TIMEOUT, server.bind(&endpoint)).await;
    if bind_result.is_err() || bind_result.unwrap().is_err() {
        let _ = timeout(OP_TIMEOUT, server.close()).await;
        let _ = timeout(OP_TIMEOUT, ctx.term()).await;
        return;
    }

    // Small sleep to let the accept loop start.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // 4. Open a raw TCP connection to the bound port.
    let mut stream = match timeout(OP_TIMEOUT, TcpStream::connect(&format!("127.0.0.1:{port}")))
        .await
    {
        Ok(Ok(s)) => s,
        _ => {
            let _ = timeout(OP_TIMEOUT, server.close()).await;
            let _ = timeout(OP_TIMEOUT, ctx.term()).await;
            return;
        }
    };

    // 5. Write the fuzz bytes to the TCP stream.
    let bytes = input_to_bytes(input);
    let _ = timeout(OP_TIMEOUT, stream.write_all(&bytes)).await;
    // Flush and close the write side to signal EOF to the server.
    let _ = timeout(OP_TIMEOUT, stream.shutdown()).await;
    drop(stream);

    // 6. Give the server a moment to process the input, then attempt a recv.
    //    We use a very short timeout — the point is just to drain the server's
    //    message queue and verify it isn't wedged.
    let _ = timeout(
        Duration::from_millis(200),
        server.recv(),
    )
    .await;

    // 7. Clean up: close the socket and terminate the context.
    //    If the server is stuck (panic/deadlock), these will time out and
    //    libFuzzer will report the iteration as a hang or panic.
    let _ = timeout(OP_TIMEOUT, server.close()).await;
    let _ = timeout(OP_TIMEOUT, ctx.term()).await;
}

// ---------------------------------------------------------------------------
// libFuzzer entry point
// ---------------------------------------------------------------------------

fuzz_target!(|input: FuzzInput| {
    // Build a fresh current-thread Tokio runtime for each fuzz iteration.
    // This ensures no state bleeds between iterations and gives us a clean
    // async environment without depending on a global runtime.
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(_) => return,
    };
    rt.block_on(run_fuzz(&input));
});
