# UDP io_uring Backend — Overview & Design Decisions

## Goal

Add an optional io_uring-accelerated path for the existing UDP transport
(Radio/Dish sockets). When both the `udp` and `io-uring` features are enabled,
and the user opts in via socket options, UDP datagrams are sent and received
through `io_uring` kernel calls (`IORING_OP_SENDMSG` / `IORING_OP_RECVMSG`)
instead of Tokio's epoll-based `UdpSocket`. The existing Tokio-based UDP path
remains the default and is untouched.

## Architecture Decision: Standalone `UdpUringActor`

**Chosen approach:** A dedicated `UdpUringActor` that owns its own `IoUring`
instance, running on a dedicated OS thread — structurally similar to the
existing `UringWorker` but purpose-built for UDP datagrams.

**Why not extend the existing `UringWorker`?**

| Concern | Reason |
|---|---|
| Stream vs datagram semantics | The worker's main loop (Phase 3 "Ensure Reads") uses `opcode::Read` with `BUFFER_SELECT` for stream sockets. UDP requires `opcode::RecvMsg` with `msghdr` structures, which is fundamentally different. |
| Handler trait mismatch | `UringConnectionHandler` is designed for stateful, connection-oriented protocols (greeting → security handshake → data phase). UDP has no connection lifecycle. |
| Multishot recvmsg vs multishot read | Multishot `RECV_MULTI` (for TCP) and multishot `RecvMsg` (for UDP) have different CQE layouts. UDP multishot recvmsg returns source address metadata per datagram. |
| Isolation | A standalone actor avoids polluting the TCP worker with UDP-specific code paths and simplifies reasoning about each. |

**What the standalone actor provides:**

- Its own `IoUring` instance (small, e.g., 64 entries)
- Its own `BufferRingManager` for provided-buffer receives
- A simple main loop: gather sends → submit SQEs → process CQEs
- Wakeup via `eventfd` (same pattern as `UringWorker`)
- Integration with Radio/Dish sockets through channel-based IPC

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Actor model | Standalone OS thread with own `IoUring` | Isolation from TCP worker; simpler UDP-specific loop |
| Receive strategy | Multishot `IORING_OP_RECVMSG` | Single SQE receives many datagrams; highest throughput |
| Send strategy | `IORING_OP_SENDMSG` + optional `IORING_OP_SENDMSG_ZC` | Standard sendmsg by default; zero-copy opt-in for high-throughput |
| Buffer management | Provided buffer ring (`io_uring_buf_ring`) for recv; optional registered buffers for ZC send | Same pattern as TCP backend |
| Feature gating | Requires both `udp` AND `io-uring` features | `#[cfg(all(feature = "udp", feature = "io-uring"))]` |
| Socket option | `IO_URING_UDP_ENABLED` (option ID 1176) | Explicit opt-in per socket, default `false` |
| ZMTP handshake | **No** | Raw UDP datagrams only (same as Tokio path) |
| Actor lifetime | One actor per bound UDP socket | Each `bind()` or `connect()` spawns its own actor |
| Multicast/Broadcast | Socket created with `socket2` before handoff | Same socket setup as Tokio path, but FD is passed to io_uring actor |

## Scope

### In Scope
- `UdpUringActor` struct and main loop on dedicated OS thread
- Multishot `recvmsg` with provided buffer rings
- `sendmsg` and `sendmsg_zc` (zero-copy) send paths
- `UdpUringSendConnection` implementing `ISocketConnection`
- Command processor wiring for `Endpoint::Udp` when io_uring is enabled
- Socket option `IO_URING_UDP_ENABLED` to opt in
- Integration tests comparing Tokio and io_uring UDP paths
- Benchmarks

### Out of Scope
- Modifying the existing `UringWorker` or `UringConnectionHandler` trait
- UDP for socket types other than Radio/Dish
- ZMTP over UDP
- `recvmmsg`/`sendmmsg` batching (future optimization)

## Feature Flag Interaction

```
udp only              → Tokio-based UDP (existing)
io-uring only         → io_uring TCP only (existing)
udp + io-uring        → Both UDP paths available; socket option selects
full-linux            → Includes udp + io-uring
```

When both features are active:
- Default behavior: Tokio UDP path (backward compatible)
- If `IO_URING_UDP_ENABLED = true` set on socket before bind/connect: io_uring path

## Wire Format

Unchanged from the existing UDP implementation:

```
[ group_len: u8 | group: [u8; group_len] | payload: [u8] ]
```

The io_uring backend sends and receives identical datagrams. No protocol
changes are needed. `RadioSocket::encode_radio_frame()` and
`DishSocket::decode_radio_frame()` are reused unchanged.

## Relationship to Existing Code

```
                    ┌─────────────────┐
                    │   RadioSocket   │ encode_radio_frame()
                    │   DishSocket    │ decode_radio_frame()
                    └───────┬─────────┘
                            │ ISocketConnection / handle_pipe_event
                ┌───────────┴───────────┐
                │                       │
    ┌───────────▼──────────┐  ┌─────────▼───────────┐
    │  Tokio UDP Path      │  │  io_uring UDP Path  │
    │  (UdpSendConnection) │  │ (UdpUringSendConn)  │
    │  (UdpReceiveActor)   │  │ (UdpUringActor)     │
    │  [existing, default] │  │ [new, opt-in]       │
    └──────────────────────┘  └─────────────────────┘
```

## Files Overview

### New Files (5)
```
core/src/io_uring_backend/udp_uring_actor.rs     — Main actor + loop (~600 lines)
core/src/io_uring_backend/udp_uring_ops.rs       — SQE/CQE types for UDP (~150 lines)
core/src/io_uring_backend/udp_send_connection.rs — ISocketConnection impl (~120 lines)
core/tests/udp_iouring_radio_dish.rs             — Integration tests (~400 lines)
core/benches/udp_throughput.rs                   — Benchmarks (~200 lines)
```

### Modified Files (5)
```
core/src/io_uring_backend/mod.rs     — pub mod declarations
core/src/socket/options.rs           — IO_URING_UDP_ENABLED option
core/src/socket/connection_iface.rs  — UdpUringSendConnection (or re-export)
core/src/socket/core/command_processor.rs — io_uring UDP path in bind/connect
core/Cargo.toml                      — test/bench entries
```
