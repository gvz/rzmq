# UDP Transport — Comprehensive Test Plan

## File: `core/tests/udp_radio_dish.rs`

Feature gate: `#[cfg(feature = "udp")]` on the entire file.
Test registration in `core/Cargo.toml`:
```toml
[[test]]
name = "udp_radio_dish"
required-features = ["udp"]
```

## Port Allocation

All UDP tests use ports in the range **5900–5990** (not used by any existing
test file). Each test uses a unique port to allow parallel execution where the
`#[serial]` attribute is not present.

```
5900  — basic unicast (dish bind / radio connect)
5901  — basic unicast (radio bind / dish connect)
5902  — group filtering — joined group delivered
5903  — group filtering — non-joined group dropped
5904  — multiple groups on one dish
5905  — join then leave
5906  — multiple dishes, independent groups
5907  — multiple dishes, same group (fan-out)
5908  — multiple radios, one dish (fan-in)
5909  — empty payload (group only)
5910  — max group name (255 bytes)
5911  — message too large (> 65507 bytes)
5912  — SO_REUSEPORT: two dishes same port
5913  — IPv6 unicast
5920  — IPv4 broadcast
5930  — IPv4 multicast (loopback)
5940  — IPv6 multicast (loopback)
5950  — socket options: UDP_MULTICAST_HOPS read/write
5951  — socket options: UDP_MULTICAST_LOOP read/write
5960  — invalid endpoint: missing port
5961  — invalid endpoint: wrong socket type (Push)
5962  — invalid endpoint: group too large at send (>255)
5970  — concurrent senders, one dish
5980  — radio bind + dish connect (reversed topology)
5981  — dish bind + radio connect (standard topology)
5990  — shutdown: context term stops receive actor
```

---

## Test Helpers (in `udp_radio_dish.rs`)

```rust
mod common;

use rzmq::socket::options::{JOIN, LEAVE, UDP_MULTICAST_HOPS, UDP_MULTICAST_LOOP};
use rzmq::{Msg, SocketType, ZmqError};
use serial_test::serial;
use std::time::Duration;

const SHORT: Duration = Duration::from_millis(200);
const LONG:  Duration = Duration::from_secs(3);
const SETTLE: Duration = Duration::from_millis(100);

/// Build a Msg with a group and payload, matching the RADIO-DISH API.
fn make_msg(group: &str, payload: &[u8]) -> Msg {
    let mut m = Msg::from_vec(payload.to_vec());
    m.set_group(group).expect("valid group");
    m
}

/// Receive with timeout; returns Err(ZmqError::Timeout) on expiry.
/// Re-uses common::recv_timeout.
```

---

## Test Catalogue

### Category A — Basic Unicast

#### A1. `test_udp_unicast_dish_bind_radio_connect`
```
Port: 5900
Setup:
  dish.bind("udp://127.0.0.1:5900")
  dish.set_option_raw(JOIN, b"news")
  radio.connect("udp://127.0.0.1:5900")
  sleep(SETTLE)
Send:
  radio.send(make_msg("news", b"hello udp"))
Assert:
  recv = dish.recv()  [LONG timeout]
  recv.group() == Some(b"news")
  recv.data()  == Some(b"hello udp")
```

#### A2. `test_udp_unicast_radio_bind_dish_connect`
```
Port: 5901
Setup:
  radio.bind("udp://0.0.0.0:5901")
  dish.connect("udp://127.0.0.1:5901")
  dish.set_option_raw(JOIN, b"sensor")
  sleep(SETTLE)
Send:
  radio.send(make_msg("sensor", b"42.5"))
Assert:
  recv = dish.recv()  [LONG timeout]
  recv.group() == Some(b"sensor")
  recv.data()  == Some(b"42.5")
```

Rationale: validates the reverse topology (Radio as server/binder).

---

### Category B — Group Filtering

#### B1. `test_udp_joined_group_delivered`
```
Port: 5902
dish.set_option_raw(JOIN, b"alpha")
radio sends make_msg("alpha", b"data")
Assert: dish receives it
```

#### B2. `test_udp_non_joined_group_dropped`
```
Port: 5903
dish does NOT join "beta"
radio sends make_msg("beta", b"data")
Assert: dish.recv() returns Err(ZmqError::Timeout) within SHORT
```

#### B3. `test_udp_multiple_groups_on_one_dish`
```
Port: 5904
dish joins "alpha" and "beta", does NOT join "gamma"
radio sends: make_msg("alpha", b"A"), make_msg("beta", b"B"), make_msg("gamma", b"C")
sleep(SETTLE) after each send
Assert:
  dish receives two messages (either order)
  groups received == {"alpha", "beta"}
  "gamma" not received (third recv times out)
```

#### B4. `test_udp_join_then_leave`
```
Port: 5905
dish joins "updates"
radio sends make_msg("updates", b"msg1") → dish receives it
dish.set_option_raw(LEAVE, b"updates")
sleep(SETTLE)
radio sends make_msg("updates", b"msg2")
Assert: dish.recv() returns Err(Timeout) — message dropped after leave
```

Rationale: verifies LEAVE takes effect immediately in the local filter set.

---

### Category C — Multi-Peer Topologies

#### C1. `test_udp_one_radio_multiple_dishes`
```
Port: 5906
radio.bind("udp://0.0.0.0:5906")
dish_a.connect + JOIN "alpha"
dish_b.connect + JOIN "beta"
dish_ab.connect + JOIN "alpha" + JOIN "beta"
sleep(SETTLE)

radio sends make_msg("alpha", b"A")
radio sends make_msg("beta",  b"B")

Assert:
  dish_a  receives exactly 1 msg, group "alpha"
  dish_b  receives exactly 1 msg, group "beta"
  dish_ab receives 2 msgs, groups {"alpha","beta"}
  (all with SHORT timeout for no-more-message check)
```

#### C2. `test_udp_same_group_multiple_dishes`
```
Port: 5907
radio.bind("udp://0.0.0.0:5907")
dish_1.connect + JOIN "news"
dish_2.connect + JOIN "news"
sleep(SETTLE)
radio sends make_msg("news", b"broadcast")
Assert: BOTH dish_1 and dish_2 receive the message
```

Rationale: UDP broadcast/unicast semantics — each connected Dish independently
receives the datagram sent to their socket.

#### C3. `test_udp_multiple_radios_one_dish`
```
Port: 5908
dish.bind("udp://0.0.0.0:5908") + JOIN "telemetry"
radio_1.connect("udp://127.0.0.1:5908")
radio_2.connect("udp://127.0.0.1:5908")
sleep(SETTLE)

radio_1 sends make_msg("telemetry", b"from-r1")
radio_2 sends make_msg("telemetry", b"from-r2")

Assert:
  dish receives 2 messages
  payloads contain both b"from-r1" and b"from-r2" (order not guaranteed)
```

Rationale: validates fan-in — multiple Radio sockets sending to one Dish port.

---

### Category D — Edge Cases & Correctness

#### D1. `test_udp_empty_payload`
```
Port: 5909
dish.bind + JOIN "ping"
radio.connect
radio.send(make_msg("ping", b""))   // empty payload
Assert: dish receives msg, group="ping", data=Some(&[])
```

#### D2. `test_udp_max_group_name`
```
Port: 5910
group = "a" repeated 255 times
dish.bind + JOIN group
radio.connect
radio.send(make_msg(&group, b"data"))
Assert: dish receives msg with 255-byte group
```

#### D3. `test_udp_message_too_large`
```
Port: 5911
dish.bind + JOIN "big"
radio.connect
Build a Msg where group="big" and payload = vec![0u8; 65507]
(encoded = 1 + 3 + 65507 = 65511 > 65507)
result = radio.send(oversized_msg)
Assert: result == Err(ZmqError::MessageTooLarge(65511, 65507))

Boundary case: payload = vec![0u8; 65503]
(encoded = 1 + 3 + 65503 = 65507 — exactly at limit)
result = radio.send(exactly_at_limit_msg)
Assert: result == Ok(())
dish receives message (verify datagram went through)
```

#### D4. `test_udp_no_group_on_send_errors`
```
radio.connect("udp://127.0.0.1:5900")
msg = Msg::from_static(b"no group")
result = radio.send(msg)   // no set_group() called
Assert: result == Err(ZmqError::InvalidState(_))
```

Rationale: the group check happens in RadioSocket::send before any UDP code.

#### D5. `test_udp_radio_cannot_recv`
```
radio = ctx.socket(SocketType::Radio)
Assert: radio.recv().await == Err(ZmqError::InvalidState(_))
```

#### D6. `test_udp_dish_cannot_send`
```
dish = ctx.socket(SocketType::Dish)
msg = Msg::from_static(b"x")
Assert: dish.send(msg).await == Err(ZmqError::InvalidState(_))
```

#### D7. `test_udp_payload_integrity`
```
Port: 5913 (also used for IPv6 unicast — adjust if needed; use 5912-B)
dish.bind + JOIN "integrity"
radio.connect
Send 50 messages with different payloads:
  for i in 0..50:
    radio.send(make_msg("integrity", format!("msg-{}", i).as_bytes()))
    sleep(1ms)
Assert: dish receives all 50 in order, payloads match
```

Note: UDP datagrams to loopback are extremely reliable and arrive in order.
This test validates that the encode/decode round-trip preserves content exactly.

---

### Category E — SO_REUSEPORT

#### E1. `test_udp_reuseport_two_dishes_same_port`
```
Port: 5912
dish_1.bind("udp://0.0.0.0:5912") + JOIN "shared"
dish_2.bind("udp://0.0.0.0:5912") + JOIN "shared"   // must not fail (SO_REUSEPORT)
sleep(SETTLE)

radio.connect("udp://127.0.0.1:5912")
radio.send(make_msg("shared", b"reuseport-test"))

// At least one dish must receive the message.
// OS load-balances datagrams across SO_REUSEPORT sockets.
let r1 = recv_timeout(&dish_1, LONG).await;
let r2 = recv_timeout(&dish_2, LONG).await;
Assert: r1.is_ok() || r2.is_ok()
// exactly one dish receives each datagram (OS distributes)
```

Rationale: core requirement — `SO_REUSEPORT` allows multiple bind on same port.

---

### Category F — IPv6

#### F1. `test_udp_ipv6_unicast`
```
Port: 5913
dish.bind("udp://[::1]:5913") + JOIN "ipv6"
radio.connect("udp://[::1]:5913")
sleep(SETTLE)
radio.send(make_msg("ipv6", b"hello v6"))
Assert: dish receives, group="ipv6", data=b"hello v6"
```

Note: Skip with `#[ignore]` if `::1` is not available (CI environment check).
Detect at test start: `std::net::TcpListener::bind("[::1]:0")` — if it fails, skip.

---

### Category G — Broadcast

#### G1. `test_udp_broadcast`
```
Port: 5920
dish.bind("udp://0.0.0.0:5920") + JOIN "bcast"
sleep(SETTLE)
radio.connect("udp://255.255.255.255:5920")
sleep(SETTLE)
radio.send(make_msg("bcast", b"broadcast data"))
Assert: dish receives, group="bcast", data=b"broadcast data"

Note: Some CI environments block 255.255.255.255 at the OS level.
Wrap with: if cfg!(target_os = "linux") { ... } or use #[ignore] fallback.
```

---

### Category H — IPv4 Multicast

#### H1. `test_udp_ipv4_multicast_loopback`
```
Port: 5930
Group: 239.255.0.1 (site-local multicast, loopback safe)
Iface: "lo" (or "127.0.0.1" as iface address)

dish.bind("udp://lo;239.255.0.1:5930") + JOIN "mcast4"
sleep(SETTLE)
radio.connect("udp://lo;239.255.0.1:5930")
sleep(SETTLE)
radio.send(make_msg("mcast4", b"multicast v4"))
Assert: dish receives, group="mcast4", data=b"multicast v4"
```

Note: `UDP_MULTICAST_LOOP` must be true (default) for loopback multicast test.
Skip on systems where `lo` does not support multicast.

#### H2. `test_udp_ipv4_multicast_multiple_receivers`
```
Port: 5931
dish_1.bind("udp://lo;239.255.0.2:5931") + JOIN "multi"
dish_2.bind("udp://lo;239.255.0.2:5931") + JOIN "multi"
sleep(SETTLE)
radio.connect("udp://lo;239.255.0.2:5931")
sleep(SETTLE)
radio.send(make_msg("multi", b"to all"))
Assert: BOTH dish_1 and dish_2 receive the message
```

Rationale: core multicast property — one sender, multiple receivers on the
same multicast group.

---

### Category I — IPv6 Multicast

#### I1. `test_udp_ipv6_multicast_loopback`
```
Port: 5940
Group: ff02::1 (all-nodes link-local multicast)
Iface: "lo"

dish.bind("udp://lo;[ff02::1]:5940") + JOIN "mcast6"
sleep(SETTLE)
radio.connect("udp://lo;[ff02::1]:5940")
sleep(SETTLE)
radio.send(make_msg("mcast6", b"multicast v6"))
Assert: dish receives, group="mcast6", data=b"multicast v6"
```

Note: Skip if IPv6 is not available on the test host.

---

### Category J — Socket Options

#### J1. `test_udp_option_multicast_hops_set_get`
```
dish = ctx.socket(SocketType::Dish)
dish.set_option_raw(UDP_MULTICAST_HOPS, &5i32.to_ne_bytes()).await?
result = dish.get_option(UDP_MULTICAST_HOPS).await?
Assert: i32::from_ne_bytes(result.try_into().unwrap()) == 5

// Out of range
err = dish.set_option_raw(UDP_MULTICAST_HOPS, &(-1i32).to_ne_bytes()).await
Assert: err == Err(ZmqError::InvalidOptionValue(_))

err = dish.set_option_raw(UDP_MULTICAST_HOPS, &256i32.to_ne_bytes()).await
Assert: err == Err(ZmqError::InvalidOptionValue(_))
```

#### J2. `test_udp_option_multicast_loop_set_get`
```
radio = ctx.socket(SocketType::Radio)
// Default should be 1 (enabled)
result = radio.get_option(UDP_MULTICAST_LOOP).await?
Assert: i32::from_ne_bytes(...) == 1

radio.set_option_raw(UDP_MULTICAST_LOOP, &0i32.to_ne_bytes()).await?
result = radio.get_option(UDP_MULTICAST_LOOP).await?
Assert: i32::from_ne_bytes(...) == 0
```

---

### Category K — Invalid Endpoint Parsing

#### K1. `test_udp_invalid_endpoint_missing_port`
```
result = dish.bind("udp://127.0.0.1").await   // no port
Assert: result == Err(ZmqError::InvalidEndpoint(_))
```

#### K2. `test_udp_invalid_endpoint_bad_scheme`
```
result = dish.bind("udp://").await   // empty address
Assert: result == Err(ZmqError::InvalidEndpoint(_))
```

#### K3. `test_udp_wrong_socket_type_push`
```
push = ctx.socket(SocketType::Push)
result = push.bind("udp://127.0.0.1:5961").await
Assert: result == Err(ZmqError::UnsupportedTransport(_))

result = push.connect("udp://127.0.0.1:5961").await
Assert: result == Err(ZmqError::UnsupportedTransport(_))
```

#### K4. `test_udp_wrong_socket_type_pub`
```
pub_sock = ctx.socket(SocketType::Pub)
result = pub_sock.bind("udp://127.0.0.1:5962").await
Assert: result == Err(ZmqError::UnsupportedTransport(_))
```

#### K5. `test_udp_invalid_group_at_send`
```
// Group string longer than 255 bytes must be caught by RadioSocket::send
radio.connect("udp://127.0.0.1:5900")
let long_group = "a".repeat(256);
let mut msg = Msg::from_static(b"data");
msg.set_group(&long_group)  // should fail at set_group
Assert: returns Err(_)
```

---

### Category L — Concurrent / Throughput

#### L1. `test_udp_concurrent_senders`
```
Port: 5970
dish.bind("udp://0.0.0.0:5970") + JOIN "concurrent"
sleep(SETTLE)

Spawn 4 tasks, each with their own radio:
  Task N:
    radio_n.connect("udp://127.0.0.1:5970")
    sleep(SETTLE)
    for i in 0..25:
      radio_n.send(make_msg("concurrent", format!("task{}-msg{}", N, i).as_bytes())).await
      sleep(1ms)

Join all 4 tasks.

Collect 100 messages from dish (4 tasks × 25 msgs):
  let mut received = Vec::new();
  for _ in 0..100 {
    received.push(recv_timeout(&dish, LONG).await?)
  }
Assert:
  received.len() == 100
  All groups == "concurrent"
  Payloads contain all 100 distinct messages (use HashSet)
```

Note: UDP loopback datagrams very rarely drop under localhost load. This test
may occasionally miss messages under extreme CI load — mark as `#[ignore]` if
flaky, or reduce to 2 tasks × 10 messages.

---

### Category M — Lifecycle / Shutdown

#### M1. `test_udp_context_term_stops_receive_actor`
```
Port: 5990
ctx = test_context()
{
  dish = ctx.socket(SocketType::Dish)
  dish.bind("udp://0.0.0.0:5990") + JOIN "shutdown"
  radio = ctx.socket(SocketType::Radio)
  radio.connect("udp://127.0.0.1:5990")
  sleep(SETTLE)

  radio.send(make_msg("shutdown", b"before term")).await?
  msg = common::recv_timeout(&dish, LONG).await?
  Assert: msg.data() == Some(b"before term")
}
ctx.term().await?
// If we get here without hanging, the UdpReceiveActor shut down cleanly.
Assert: term() returns Ok(())
```

#### M2. `test_udp_socket_close_stops_receive_actor`
```
Port: 5991
dish.bind("udp://0.0.0.0:5991") + JOIN "close"
radio.connect("udp://127.0.0.1:5991")
sleep(SETTLE)

radio.send(make_msg("close", b"pre-close")).await?
dish.recv() // consume it

dish.close().await?
// UdpReceiveActor should stop; no hang
ctx.term().await?
Assert: no panic or hang
```

#### M3. `test_udp_unbind_then_rebind`
```
Port: 5992 / 5993
dish.bind("udp://0.0.0.0:5992") + JOIN "rebind"
radio.connect("udp://127.0.0.1:5992")
sleep(SETTLE)
radio.send(make_msg("rebind", b"first")) → dish receives

dish.unbind("udp://0.0.0.0:5992").await?
sleep(SETTLE)

dish.bind("udp://0.0.0.0:5993") + JOIN "rebind"
radio_2.connect("udp://127.0.0.1:5993")
sleep(SETTLE)
radio_2.send(make_msg("rebind", b"second")) → dish receives

Assert: both messages received correctly
```

---

## Test Structure / Attributes

```
tests using loopback TCP-like ports (serial risk):
  → use #[serial] to avoid port conflicts with other test files

tests for broadcast/multicast:
  → may need #[serial] + OS-capability skip

tests for IPv6:
  → detect at runtime, skip gracefully if no IPv6

tests for pure state validation (no network):
  → no #[serial], can run in parallel
```

## Port Reservation Summary

```
5900–5919  Unicast tests (A, B, C, D, E categories)
5920–5929  Broadcast tests (G)
5930–5939  IPv4 multicast tests (H)
5940–5949  IPv6 multicast tests (I)
5950–5959  Socket option tests (J) — no network needed, no port allocation
5960–5969  Invalid endpoint / type tests (K)
5970–5979  Concurrent tests (L)
5980–5999  Lifecycle / shutdown tests (M)
```
