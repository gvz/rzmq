# UDP io_uring Backend — Implementation Order

## Prerequisites

Before starting, ensure:

1. **Step 8 of the UDP plan is complete**: The command processor must have
   the Tokio UDP path wired (`plans/udp/05_command_processor.md`). The
   io_uring path branches off from the same `Endpoint::Udp` match arms.

2. **Kernel 6.0+**: Development and testing requires Linux 6.0+ for
   multishot recvmsg support.

3. **io_uring feature compiles**: `cargo check --features "udp,io-uring"` passes.

## Phase 1: Foundation (Actor + Types)

**Goal:** Get the `UdpUringActor` compiling with minimal functionality.

### Step 1.1: Socket Options
**File:** `core/src/socket/options.rs`
**Changes:**
- Add `IO_URING_UDP_ENABLED` (1176) and `IO_URING_UDP_SNDZEROCOPY` (1177) constants
- Add `UdpUringOptions` struct
- Add field to `SocketOptions`
- Wire `apply_core_option_value()` and `retrieve_core_option_value()`

**Validation:**
```bash
cargo check --features "udp,io-uring"
```

### Step 1.2: Actor Types and Spawn
**File:** `core/src/io_uring_backend/udp_uring_actor.rs`
**Changes:**
- Define all types: `UdpUringActor`, `UdpUringActorConfig`, `UdpUringActorHandle`,
  `UdpUringCommand`, `UdpSendRequest`, `UdpUringActorState`, `UdpUringOpType`
- Define `RecvMsgContext` and `SendMsgContext`
- Implement `UdpUringActor::spawn()` — creates IoUring, eventfd, channels, OS thread
- Implement `initialize()` — stub (returns Ok)
- Implement `run_loop()` — minimal loop that checks for Stop command and exits

**Validation:**
```bash
cargo check --features "udp,io-uring"
```

### Step 1.3: Module Declarations
**Files:** `core/src/io_uring_backend/mod.rs`, `core/src/transport/mod.rs`
**Changes:**
- Add `pub(crate) mod udp_uring_actor;` (gated)
- Add `pub(crate) mod udp_send_connection;` (gated)
- Add `pub(crate) mod udp_recv_delivery;` (gated)
- Add `pub(crate) mod udp_uring;` in transport (gated)

**Validation:**
```bash
cargo check --features "udp,io-uring"
```

## Phase 2: Receive Path

**Goal:** Dish can receive datagrams via io_uring multishot recvmsg.

### Step 2.1: Buffer Initialization
**File:** `core/src/io_uring_backend/udp_uring_actor.rs`
**Changes:**
- Implement `initialize_recv_buffers()` — creates `BufferRingManager`
- Implement `submit_eventfd_poll()` — POLL_ADD on eventfd
- Wire into `initialize()`

**Validation:** Unit test that spawns actor, verifies it starts and stops.

### Step 2.2: Multishot recvmsg Submission
**File:** `core/src/io_uring_backend/udp_uring_actor.rs`
**Changes:**
- Implement `submit_multishot_recvmsg()` — builds and submits the SQE
- Add to `gather_work()` — ensures multishot is always active

**Validation:** Actor starts, submits recvmsg SQE (visible in trace logs).

### Step 2.3: CQE Processing for recvmsg
**File:** `core/src/io_uring_backend/udp_uring_actor.rs`
**Changes:**
- Implement `process_cqes()` — full CQE dispatch logic
- Implement `handle_recv_cqe()` — extracts datagram from buffer, creates Msg
- Implement `handle_eventfd_cqe()` — drains eventfd, re-arms poll
- Handle multishot termination and re-submission

**Validation:** Log output shows datagrams being parsed from CQEs.

### Step 2.4: Recv Delivery Task
**File:** `core/src/io_uring_backend/udp_recv_delivery.rs`
**Changes:**
- Implement `spawn_recv_delivery_task()` — Tokio task bridging to ISocket
- Wire channel between actor's `deliver_to_socket()` and this task

**Validation:** End-to-end test: raw UDP socket sends datagram → actor receives → delivery task → DishSocket.

## Phase 3: Send Path

**Goal:** Radio can send datagrams via io_uring sendmsg.

### Step 3.1: UdpUringSendConnection
**File:** `core/src/io_uring_backend/udp_send_connection.rs`
**Changes:**
- Implement `UdpUringSendConnection` with `ISocketConnection` trait
- Size validation (65507 byte limit)
- Channel send + eventfd signal

**Validation:**
```bash
cargo check --features "udp,io-uring"
```

### Step 3.2: Standard sendmsg
**File:** `core/src/io_uring_backend/udp_uring_actor.rs`
**Changes:**
- Implement `SendMsgContext::new()` and `msghdr_ptr()`
- Implement `queue_sendmsg()` — builds SQE from `UdpSendRequest`
- Add send drain to `gather_work()`
- Handle SendMsg CQE in `process_cqes()`

**Validation:** End-to-end: RadioSocket → UdpUringSendConnection → actor → sendmsg → network.

### Step 3.3: Zero-Copy sendmsg
**File:** `core/src/io_uring_backend/udp_uring_actor.rs`
**Changes:**
- Implement `queue_sendmsg_zc()` — uses `opcode::SendMsgZc`
- Handle two-CQE flow (submission ack + CQE_F_NOTIF)
- Select standard vs ZC based on `send_zerocopy_enabled` config

**Validation:** Test with `IO_URING_UDP_SNDZEROCOPY = true`, verify messages arrive.

## Phase 4: Socket Creation Helpers

**Goal:** Raw FD creation for io_uring without Tokio conversion.

### Step 4.1: Transport Helpers
**File:** `core/src/transport/udp_uring.rs`
**Changes:**
- Implement `create_bound_raw_udp_socket()` — Radio bind
- Implement `create_connected_raw_udp_socket()` — Radio connect
- Implement `create_recv_raw_udp_socket()` — Dish bind/connect
- Reuse socket2 setup from existing `udp.rs` functions

**Validation:**
```bash
cargo check --features "udp,io-uring"
```

## Phase 5: Command Processor Integration

**Goal:** `bind("udp://...")` and `connect("udp://...")` use io_uring when opted in.

### Step 5.1: State Storage
**File:** `core/src/socket/core/state.rs` (or `mod.rs`)
**Changes:**
- Add `udp_uring_actors: HashMap<String, UdpUringActorHandle>` to `CoreState`

### Step 5.2: Bind Handlers
**File:** `core/src/socket/core/command_processor.rs`
**Changes:**
- Implement `handle_radio_bind_uring()`
- Implement `handle_dish_bind_uring()`
- Add io_uring branch in `Endpoint::Udp` arm of `handle_user_bind()`

**Validation:** Test: set option → bind → send/recv works.

### Step 5.3: Connect Handlers
**File:** `core/src/socket/core/command_processor.rs`
**Changes:**
- Implement `handle_radio_connect_uring()`
- Implement `handle_dish_connect_uring()`
- Add io_uring branch in `Endpoint::Udp` arm of `handle_user_connect()`

**Validation:** Test: set option → connect → send/recv works.

### Step 5.4: Shutdown Cleanup
**File:** `core/src/socket/core/shutdown.rs`
**Changes:**
- Add actor cleanup in shutdown sequence

**Validation:** Test: create → use → shutdown → no thread leaks.

## Phase 6: Tests and Benchmarks

### Step 6.1: Integration Tests
**File:** `core/tests/udp_iouring_radio_dish.rs`
**Changes:** Implement all 15 test cases from `09_tests.md`

**Validation:**
```bash
cargo test --features "udp,io-uring" --test udp_iouring_radio_dish
```

### Step 6.2: Benchmarks
**File:** `core/benches/udp_throughput.rs`
**Changes:** Implement throughput and latency benchmarks

**Validation:**
```bash
cargo bench --features "udp,io-uring" --bench udp_throughput
```

### Step 6.3: Cargo.toml Entries
**File:** `core/Cargo.toml`
**Changes:** Add `[[test]]` and `[[bench]]` entries

## Phase 7: Final Validation

### Step 7.1: Full Feature Regression
```bash
# Compile all features
cargo check --features "full-linux"

# Run all tests
cargo test --features "full-linux"

# Clippy
cargo clippy --features "full-linux"

# Ensure non-uring path still works
cargo test --features "udp" --test udp_radio_dish
```

### Step 7.2: Code Review Checklist

- [ ] All new files have `#![cfg(all(feature = "udp", feature = "io-uring"))]`
- [ ] No changes to existing Tokio UDP path behavior
- [ ] `unsafe` blocks are minimal and documented
- [ ] All `SendMsgContext` lifetimes are correct (no use-after-free)
- [ ] Buffer ring buffers are always returned (no leaks)
- [ ] eventfd signaling works in both directions
- [ ] Shutdown is clean: no hanging threads, no leaked FDs
- [ ] Multishot recvmsg re-submits after termination
- [ ] Zero-copy sends wait for CQE_F_NOTIF before freeing buffers
- [ ] 2-space indentation per `rustfmt.toml`

## Summary: Implementation Timeline

| Phase | Steps | Est. Effort | Dependencies |
|---|---|---|---|
| 1. Foundation | 1.1-1.3 | ~2 hours | UDP Step 8 complete |
| 2. Receive Path | 2.1-2.4 | ~4 hours | Phase 1 |
| 3. Send Path | 3.1-3.3 | ~3 hours | Phase 1 |
| 4. Socket Helpers | 4.1 | ~1 hour | None |
| 5. Integration | 5.1-5.4 | ~3 hours | Phases 2, 3, 4 |
| 6. Tests | 6.1-6.3 | ~3 hours | Phase 5 |
| 7. Validation | 7.1-7.2 | ~1 hour | Phase 6 |
| **Total** | | **~17 hours** | |

Phases 2 and 3 can be developed in parallel since they're independent paths
(receive vs send). Phase 4 can also be developed in parallel with Phases 2-3.
