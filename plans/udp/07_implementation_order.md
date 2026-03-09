# UDP Transport — Implementation Order

Each step is self-contained and compilable (with the feature gate) before
moving to the next. Steps 1–4 add no runtime behaviour; steps 5–8 add the
actual transport; step 9 wires everything together; step 10 validates.

---

## Step 1 — Error variant
**File:** `core/src/error.rs`

Add `MessageTooLarge(usize, usize)` to `ZmqError`.

Compilable immediately. No other files change. Run existing tests to confirm
nothing broke.

```
cargo test --all-features
```

---

## Step 2 — Cargo feature + test entry
**File:** `core/Cargo.toml`

```toml
[features]
udp        = []
full       = ["default", "ipc", "inproc", "plain", "noise_xx", "curve", "udp"]
full-linux = ["full", "io-uring"]

[[test]]
name = "udp_radio_dish"
required-features = ["udp"]
```

Confirm `cargo check --features udp` passes (no source yet, but feature
compiles cleanly against the empty module list).

---

## Step 3 — `udp_endpoint.rs` (parser only)
**File:** `core/src/transport/udp_endpoint.rs` *(new)*

Implement `UdpMode`, `UdpEndpoint`, and `parse_udp_endpoint`.
No I/O, no socket2 usage here — pure parsing logic.

Add to `core/src/transport/mod.rs`:
```rust
#[cfg(feature = "udp")]
pub mod udp_endpoint;
```

Write unit tests inline (`#[cfg(test)]` block inside `udp_endpoint.rs`):
- Parse unicast IPv4
- Parse unicast IPv6
- Parse broadcast (255.255.255.255)
- Parse IPv4 multicast with interface
- Parse IPv6 multicast with interface
- Reject empty address
- Reject missing port
- Reject zero port

```
cargo test --features udp
```

---

## Step 4 — `Endpoint::Udp` variant
**File:** `core/src/transport/endpoint.rs`

Add `#[cfg(feature = "udp")] Udp(...)` variant and the `"udp"` match arm in
`parse_endpoint`. Add unit tests:
- `parse_endpoint("udp://127.0.0.1:5900")` returns `Endpoint::Udp`
- `parse_endpoint("udp://")` returns `Err(InvalidEndpoint)`

```
cargo test --features udp
```

---

## Step 5 — Socket options
**File:** `core/src/socket/options.rs`

Add `UDP_MULTICAST_LOOP`, `UDP_MULTICAST_HOPS` constants, `UdpSocketOptions`
struct, field on `SocketOptions`, and the `apply`/`retrieve` arms.

```
cargo test --features udp --test socket_options  # existing tests still pass
```

---

## Step 6 — `UdpSendConnection`
**File:** `core/src/socket/connection_iface.rs`

Add the `UdpSendConnection` struct and its `ISocketConnection` impl.
No async runtime needed for unit tests here; verify it compiles correctly.

```
cargo check --features udp
```

---

## Step 7 — `udp.rs` (transport actors)
**File:** `core/src/transport/udp.rs` *(new)*

Implement in this order:
1. `resolve_iface_to_ipv4` helper
2. `resolve_iface_to_index` helper
3. `create_connected_send_socket` (Radio connect path)
4. `create_bound_send_socket` (Radio bind path)
5. `UdpReceiveActor` struct + `create_and_spawn`
6. `UdpReceiveActor::run` (the async receive loop)

Add to `core/src/transport/mod.rs`:
```rust
#[cfg(feature = "udp")]
pub mod udp;
```

Smoke-test: write a small inline `#[cfg(test)]` that creates a send socket and
a receive actor on 127.0.0.1:59090, sends one raw datagram, and receives it.
This validates the socket setup without the full rzmq stack.

```
cargo test --features udp
```

---

## Step 8 — Command processor wiring
**File:** `core/src/socket/core/command_processor.rs`

Add the four match arms described in `05_command_processor.md`:
- `handle_user_bind` / `Endpoint::Udp` / Radio path
- `handle_user_bind` / `Endpoint::Udp` / Dish path
- `handle_user_connect` / `Endpoint::Udp` / Radio path
- `handle_user_connect` / `Endpoint::Udp` / Dish path

Also add the `udp_socket_type_check` helper.

Import any new types at the top of the file under `#[cfg(feature = "udp")]`.

```
cargo check --features udp
cargo test --features udp   # existing tests still pass
```

---

## Step 9 — Integration: first smoke test
**File:** `core/tests/udp_radio_dish.rs` *(new, initially sparse)*

Write just tests A1 and A2 (basic unicast both topologies). Run them:

```
cargo test --features udp --test udp_radio_dish
```

Fix any wiring issues discovered here before expanding the test suite.

---

## Step 10 — Full test suite
**File:** `core/tests/udp_radio_dish.rs`

Expand the test file with all tests from `06_tests.md` in category order:
B (group filtering) → C (multi-peer) → D (edge cases) → E (SO_REUSEPORT) →
F (IPv6) → G (broadcast) → H (IPv4 multicast) → I (IPv6 multicast) →
J (socket options) → K (invalid endpoints) → L (concurrent) → M (lifecycle).

Add CI-safe skip guards for tests requiring:
- IPv6 (`std::net::UdpSocket::bind("[::1]:0")` probe)
- Multicast on loopback (OS-dependent)
- Broadcast to 255.255.255.255 (may be blocked in containers)

```
cargo test --features udp --test udp_radio_dish -- --nocapture
```

---

## Step 11 — Final regression check

```bash
# All features, all tests
cargo test --all-features

# Default features (udp excluded) — nothing should change
cargo test

# Clippy
cargo clippy --features udp -- -D warnings

# Doc check
cargo doc --features udp --no-deps
```

---

## Dependency Notes

- `socket2` (already in `Cargo.toml` at `0.6` with `features = ["all"]`) —
  provides `set_reuse_address`, `set_reuse_port`, `set_broadcast`,
  `join_multicast_v4`, `join_multicast_v6`, `set_multicast_loop_v4`, etc.
- `libc` (already in `Cargo.toml` at `0.2`) — provides `if_nametoindex` for
  interface index resolution.
- `tokio` `net` feature (already enabled) — provides `UdpSocket`.
- No new dependencies are needed.

---

## Risk Areas

| Risk | Mitigation |
|---|---|
| `SO_REUSEPORT` not available on all platforms | Wrap in `#[cfg(unix)]`; on Windows omit it |
| Multicast join on loopback fails in containers | Runtime probe + `#[ignore]` on failure |
| Broadcast blocked by firewall/container | Runtime probe + `#[ignore]` on failure |
| Dual-stack IPv6 disabled in some kernels | Runtime probe + `#[ignore]` on failure |
| `if_nametoindex` returns 0 for unknown iface | Return `ZmqError::InvalidEndpoint` |
| Radio bind: Distributor sends to UdpSendConnection | send_addr not populated for bind mode; Radio bind only makes sense as a server sending datagrams TO connected Dish peers via their Dish-connect addresses. If Radio binds but no Dish connects, sends are silently dropped (no peers in Distributor). This is correct libzmq behaviour. |
