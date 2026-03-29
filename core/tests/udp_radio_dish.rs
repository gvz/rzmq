// UDP Radio-Dish Transport Tests
// NOTE: These tests will work once Step 8 (command_processor.rs wiring) is completed

#![cfg(feature = "udp")]

mod common;

use common::{recv_timeout, test_context};
use rzmq::socket::options::{JOIN, LEAVE};
use rzmq::{Context, SocketType, ZmqError};
use std::time::Duration;

const SHORT: Duration = Duration::from_millis(200);
const LONG: Duration = Duration::from_secs(3);
const SETTLE: Duration = Duration::from_millis(100);

/// Helper to create a Radio-Dish message with group
fn make_msg(group: &str, payload: &[u8]) -> rzmq::Msg {
  let mut m = rzmq::Msg::from_vec(payload.to_vec());
  m.set_group(group).expect("valid group");
  m
}

// ============================================================================
// Category A: Basic Unicast Tests
// ============================================================================

#[tokio::test]
#[serial_test::serial]
async fn test_udp_unicast_dish_bind_radio_connect() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://127.0.0.1:5900").await?;
  dish.set_option_raw(JOIN, b"news").await?;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5900").await?;

  tokio::time::sleep(SETTLE).await;

  radio.send(make_msg("news", b"hello udp")).await?;

  let recv = recv_timeout(&dish, LONG).await?;
  assert_eq!(recv.group(), Some(b"news".as_ref()));
  assert_eq!(recv.data(), Some(b"hello udp".as_ref()));

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_unicast_radio_bind_dish_connect() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.bind("udp://0.0.0.0:5901").await?;

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.connect("udp://127.0.0.1:5901").await?;
  dish.set_option_raw(JOIN, b"sensor").await?;

  tokio::time::sleep(SETTLE).await;

  radio.send(make_msg("sensor", b"42.5")).await?;

  let recv = recv_timeout(&dish, LONG).await?;
  assert_eq!(recv.group(), Some(b"sensor".as_ref()));
  assert_eq!(recv.data(), Some(b"42.5".as_ref()));

  ctx.term().await?;
  Ok(())
}

// ============================================================================
// Category B: Group Filtering Tests
// ============================================================================

#[tokio::test]
#[serial_test::serial]
async fn test_udp_joined_group_delivered() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://127.0.0.1:5902").await?;
  dish.set_option_raw(JOIN, b"alpha").await?;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5902").await?;

  tokio::time::sleep(SETTLE).await;

  radio.send(make_msg("alpha", b"data")).await?;

  let recv = recv_timeout(&dish, LONG).await?;
  assert_eq!(recv.group(), Some(b"alpha".as_ref()));

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_non_joined_group_dropped() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://127.0.0.1:5903").await?;
  // Note: NOT joining "beta"

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5903").await?;

  tokio::time::sleep(SETTLE).await;

  radio.send(make_msg("beta", b"data")).await?;

  // Should timeout - message filtered out
  let result = recv_timeout(&dish, SHORT).await;
  assert!(result.is_err());

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_join_then_leave() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://127.0.0.1:5905").await?;
  dish.set_option_raw(JOIN, b"updates").await?;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5905").await?;

  tokio::time::sleep(SETTLE).await;

  // First message should be received
  radio.send(make_msg("updates", b"msg1")).await?;
  let recv = recv_timeout(&dish, LONG).await?;
  assert_eq!(recv.data(), Some(b"msg1".as_ref()));

  // Leave the group
  dish.set_option_raw(LEAVE, b"updates").await?;
  tokio::time::sleep(SETTLE).await;

  // Second message should be dropped
  radio.send(make_msg("updates", b"msg2")).await?;
  let result = recv_timeout(&dish, SHORT).await;
  assert!(result.is_err());

  ctx.term().await?;
  Ok(())
}

// ============================================================================
// Category D: Edge Cases
// ============================================================================

#[tokio::test]
#[serial_test::serial]
async fn test_udp_empty_payload() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://127.0.0.1:5909").await?;
  dish.set_option_raw(JOIN, b"ping").await?;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5909").await?;

  tokio::time::sleep(SETTLE).await;

  radio.send(make_msg("ping", b"")).await?;

  let recv = recv_timeout(&dish, LONG).await?;
  assert_eq!(recv.group(), Some(b"ping".as_ref()));
  assert_eq!(recv.data(), Some(b"".as_ref()));

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_message_too_large() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://127.0.0.1:5911").await?;
  dish.set_option_raw(JOIN, b"big").await?;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5911").await?;

  tokio::time::sleep(SETTLE).await;

  // Create a message that's too large (65507 is the limit)
  // group="big" (3 bytes) + len byte (1) = 4 bytes overhead
  // So payload must be <= 65503 to fit
  let oversized = vec![0u8; 65507]; // This will exceed after adding group
  let msg = make_msg("big", &oversized);

  let result = radio.send(msg).await;
  assert!(matches!(result, Err(ZmqError::MessageTooLarge(_, _))));

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_message_at_limit() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://127.0.0.1:5912").await?;
  dish.set_option_raw(JOIN, b"big").await?;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5912").await?;

  tokio::time::sleep(SETTLE).await;

  // Exactly at limit: 1 (len) + 3 (group) + 65503 (payload) = 65507
  let at_limit = vec![0u8; 65503];
  let msg = make_msg("big", &at_limit);

  // Should succeed
  radio.send(msg).await?;

  let recv = recv_timeout(&dish, LONG).await?;
  assert_eq!(recv.data().unwrap().len(), 65503);

  ctx.term().await?;
  Ok(())
}

// ============================================================================
// Category E: SO_REUSEPORT Test
// ============================================================================

#[tokio::test]
#[serial_test::serial]
#[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
async fn test_udp_reuseport_two_dishes_same_port() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();

  let dish_1 = ctx.socket(SocketType::Dish).await?;
  dish_1.bind("udp://0.0.0.0:5920").await?;
  dish_1.set_option_raw(JOIN, b"shared").await?;

  let dish_2 = ctx.socket(SocketType::Dish).await?;
  // Should not fail due to SO_REUSEPORT
  dish_2.bind("udp://0.0.0.0:5920").await?;
  dish_2.set_option_raw(JOIN, b"shared").await?;

  tokio::time::sleep(SETTLE).await;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://127.0.0.1:5920").await?;

  tokio::time::sleep(SETTLE).await;

  radio.send(make_msg("shared", b"reuseport-test")).await?;

  // At least one dish should receive the message
  // (OS load-balances across SO_REUSEPORT sockets)
  let r1 = recv_timeout(&dish_1, LONG).await;
  let r2 = recv_timeout(&dish_2, LONG).await;

  assert!(
    r1.is_ok() || r2.is_ok(),
    "At least one dish should receive the message"
  );

  ctx.term().await?;
  Ok(())
}

// ============================================================================
// Category F: IPv6 Test
// ============================================================================

#[tokio::test]
#[serial_test::serial]
async fn test_udp_ipv6_unicast() -> Result<(), Box<dyn std::error::Error>> {
  // Skip if IPv6 localhost not available
  if std::net::TcpListener::bind("[::1]:0").is_err() {
    println!("IPv6 not available, skipping test");
    return Ok(());
  }

  let ctx = test_context();

  let dish = ctx.socket(SocketType::Dish).await?;
  dish.bind("udp://[::1]:5913").await?;
  dish.set_option_raw(JOIN, b"ipv6").await?;

  let radio = ctx.socket(SocketType::Radio).await?;
  radio.connect("udp://[::1]:5913").await?;

  tokio::time::sleep(SETTLE).await;

  radio.send(make_msg("ipv6", b"hello v6")).await?;

  let recv = recv_timeout(&dish, LONG).await?;
  assert_eq!(recv.group(), Some(b"ipv6".as_ref()));
  assert_eq!(recv.data(), Some(b"hello v6".as_ref()));

  ctx.term().await?;
  Ok(())
}

// ============================================================================
// Category K: Invalid Endpoint Tests
// ============================================================================

#[tokio::test]
async fn test_udp_invalid_endpoint_missing_port() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();
  let dish = ctx.socket(SocketType::Dish).await?;

  let result = dish.bind("udp://127.0.0.1").await;
  assert!(result.is_err());

  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_udp_wrong_socket_type_push() -> Result<(), Box<dyn std::error::Error>> {
  let ctx = test_context();
  let push = ctx.socket(SocketType::Push).await?;

  let result = push.bind("udp://127.0.0.1:5961").await;
  assert!(matches!(result, Err(ZmqError::UnsupportedTransport(_))));

  ctx.term().await?;
  Ok(())
}
