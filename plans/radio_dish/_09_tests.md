# 09 — Integration Tests

## File

`core/tests/radio_dish.rs` (new file)

## Imports and Constants

```rust
use rzmq::socket::options::{JOIN, LEAVE};
use rzmq::socket::SocketEvent;
use rzmq::{Context, Msg, SocketType, ZmqError};
use serial_test::serial;
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(3);
const SETTLE: Duration = Duration::from_millis(100); // connection/subscription settle time
```

TCP ports used: 5800–5815 (verify no conflict with other test files).

---

## Test 1 — Basic send/receive over TCP

```rust
#[tokio::test]
#[serial]
async fn test_radio_dish_basic_tcp() -> Result<(), ZmqError> {
    // Setup: RADIO binds, DISH connects and joins group "news"
    // Action: RADIO sends a message with group "news"
    // Assert: DISH receives the message; msg.data() matches; msg.group() == Some(b"news")
    ctx.term().await?;
    Ok(())
}
```

Use port 5800. Sleep `SETTLE` after connect + JOIN before sending.

---

## Test 2 — DISH receives nothing without JOIN

```rust
#[tokio::test]
#[serial]
async fn test_radio_dish_no_join_drops_messages() -> Result<(), ZmqError> {
    // Setup: RADIO binds, DISH connects but does NOT join any group
    // Action: RADIO sends message with group "alerts"
    // Assert: DISH recv times out (ZmqError::Timeout after SHORT_TIMEOUT)
    Ok(())
}
```

Use port 5801.

---

## Test 3 — Multiple groups

```rust
#[tokio::test]
#[serial]
async fn test_radio_dish_multiple_groups() -> Result<(), ZmqError> {
    // Setup: DISH joins "sports" and "news"
    // Action: RADIO sends to "sports", "news", "weather"
    // Assert: DISH receives "sports" and "news" messages; "weather" is dropped
    //         (recv for "weather" times out)
    Ok(())
}
```

Use port 5802.

---

## Test 4 — Leave stops delivery

```rust
#[tokio::test]
#[serial]
async fn test_radio_dish_leave_group() -> Result<(), ZmqError> {
    // Setup: DISH joins "updates"
    // Step 1: RADIO sends "updates" message — DISH receives it
    // Step 2: DISH leaves "updates" (set_option(LEAVE, b"updates"))
    // Sleep SETTLE to allow LEAVE to propagate
    // Step 3: RADIO sends another "updates" message — DISH does NOT receive it
    // Assert: second recv times out
    Ok(())
}
```

Use port 5803.

---

## Test 5 — RADIO send without group returns error

```rust
#[tokio::test]
async fn test_radio_send_without_group_errors() -> Result<(), ZmqError> {
    // No bind/connect needed
    let radio = ctx.socket(SocketType::Radio)?;
    let msg = Msg::from_static(b"hello"); // no group set
    let result = radio.send(msg).await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
    ctx.term().await?;
    Ok(())
}
```

---

## Test 6 — RADIO cannot receive

```rust
#[tokio::test]
async fn test_radio_cannot_recv() -> Result<(), ZmqError> {
    let radio = ctx.socket(SocketType::Radio)?;
    let result = radio.recv().await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
    ctx.term().await?;
    Ok(())
}
```

---

## Test 7 — DISH cannot send

```rust
#[tokio::test]
async fn test_dish_cannot_send() -> Result<(), ZmqError> {
    let dish = ctx.socket(SocketType::Dish)?;
    let msg = Msg::from_static(b"hello");
    let result = dish.send(msg).await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
    ctx.term().await?;
    Ok(())
}
```

---

## Test 8 — RADIO send_multipart returns error

```rust
#[tokio::test]
async fn test_radio_no_multipart() -> Result<(), ZmqError> {
    let radio = ctx.socket(SocketType::Radio)?;
    let result = radio.send_multipart(vec![Msg::from_static(b"part1")]).await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
    ctx.term().await?;
    Ok(())
}
```

---

## Test 9 — Received message has group set

```rust
#[tokio::test]
#[serial]
async fn test_dish_received_msg_has_correct_group() -> Result<(), ZmqError> {
    // Setup: DISH joins "sensor-data"
    // Action: RADIO sends msg with group "sensor-data" and payload b"42.5"
    // Assert:
    //   received.group() == Some(b"sensor-data")
    //   received.data()  == Some(b"42.5")
    Ok(())
}
```

Use port 5804.

---

## Test 10 — Multiple DISH sockets, independent groups

```rust
#[tokio::test]
#[serial]
async fn test_radio_multiple_dishes_independent_groups() -> Result<(), ZmqError> {
    // Setup:
    //   dish_a joins "alpha"
    //   dish_b joins "beta"
    //   dish_ab joins both "alpha" and "beta"
    // Action: RADIO sends to "alpha", then "beta"
    // Assert:
    //   dish_a  receives "alpha", times out on "beta"
    //   dish_b  receives "beta",  times out on "alpha"
    //   dish_ab receives both "alpha" and "beta"
    Ok(())
}
```

Use port 5805.

---

## Test 11 — IPC transport (feature-gated)

```rust
#[cfg(feature = "ipc")]
#[tokio::test]
async fn test_radio_dish_ipc() -> Result<(), ZmqError> {
    // Mirror test_radio_dish_basic_tcp using common::unique_ipc_endpoint()
    Ok(())
}
```

---

## Test 12 — Inproc transport (feature-gated)

```rust
#[cfg(feature = "inproc")]
#[tokio::test]
async fn test_radio_dish_inproc() -> Result<(), ZmqError> {
    // Mirror test_radio_dish_basic_tcp using common::unique_inproc_endpoint()
    Ok(())
}
```

---

## Test 13 — Invalid group name rejected

```rust
#[tokio::test]
async fn test_invalid_group_rejected_by_join() -> Result<(), ZmqError> {
    let dish = ctx.socket(SocketType::Dish)?;
    // Empty group
    let result = dish.set_option_raw(JOIN, b"").await;
    assert!(result.is_err());
    // Group with NUL byte
    let result2 = dish.set_option_raw(JOIN, b"bad\x00group").await;
    assert!(result2.is_err());
    // Group > 255 bytes
    let long_group = vec![b'a'; 256];
    let result3 = dish.set_option_raw(JOIN, &long_group).await;
    assert!(result3.is_err());
    ctx.term().await?;
    Ok(())
}
```

---

## Test 14 — JOIN replayed to late-connecting RADIO

```rust
#[tokio::test]
#[serial]
async fn test_dish_join_replayed_on_connect() -> Result<(), ZmqError> {
    // Setup: DISH joins "live" BEFORE connecting to RADIO
    // Then RADIO binds, DISH connects
    // Sleep SETTLE to allow JOIN replay
    // Action: RADIO sends to "live"
    // Assert: DISH receives the message
    // (proves pipe_attached replays existing joins to the new connection)
    Ok(())
}
```

Use port 5806.

---

## Test Helpers Needed

No new helpers needed — all tests use the existing `common::recv_timeout`,
`common::test_context`, and `common::unique_ipc_endpoint` /
`common::unique_inproc_endpoint`.

The `Msg::set_group` method (from `02_msg_changes.md`) is the primary new API
used in tests.
