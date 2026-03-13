// UDP io_uring Radio-Dish Transport Tests
// Tests the io_uring-based UDP transport implementation

#![cfg(all(feature = "udp", feature = "io-uring"))]

mod common;

use common::{recv_timeout, test_context};
use rzmq::socket::options::{JOIN, LEAVE};
use rzmq::{Context, SocketType, ZmqError};
use std::time::Duration;

const SHORT: Duration = Duration::from_millis(200);
const LONG: Duration = Duration::from_secs(3);
const SETTLE: Duration = Duration::from_millis(100);

fn make_msg(group: &str, payload: &[u8]) -> rzmq::Msg {
    let mut m = rzmq::Msg::from_vec(payload.to_vec());
    m.set_group(group).expect("valid group");
    m
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_unicast_dish_bind_radio_connect() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6900").await?;
    dish.set_option_raw(JOIN, b"news").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6900").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("news", b"hello udp iouring")).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.group(), Some(b"news".as_ref()));
    assert_eq!(recv.data(), Some(b"hello udp iouring".as_ref()));

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_unicast_radio_bind_dish_connect() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.bind("udp://0.0.0.0:6901").await?;

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.connect("udp://127.0.0.1:6901").await?;
    dish.set_option_raw(JOIN, b"sensor").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("sensor", b"42.5")).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.group(), Some(b"sensor".as_ref()));
    assert_eq!(recv.data(), Some(b"42.5".as_ref()));

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_joined_group_delivered() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6902").await?;
    dish.set_option_raw(JOIN, b"alpha").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6902").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("alpha", b"data")).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.group(), Some(b"alpha".as_ref()));

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_non_joined_group_dropped() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6903").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6903").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("beta", b"data")).await?;

    let result = recv_timeout(&dish, SHORT).await;
    assert!(result.is_err());

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_join_then_leave() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6905").await?;
    dish.set_option_raw(JOIN, b"updates").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6905").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("updates", b"msg1")).await?;
    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.data(), Some(b"msg1".as_ref()));

    dish.set_option_raw(LEAVE, b"updates").await?;
    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("updates", b"msg2")).await?;
    let result = recv_timeout(&dish, SHORT).await;
    assert!(result.is_err());

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_empty_payload() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6909").await?;
    dish.set_option_raw(JOIN, b"ping").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6909").await?;

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
async fn test_udp_iouring_message_too_large() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6911").await?;
    dish.set_option_raw(JOIN, b"big").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6911").await?;

    tokio::time::sleep(SETTLE).await;

    let oversized = vec![0u8; 65507];
    let msg = make_msg("big", &oversized);

    let result = radio.send(msg).await;
    assert!(matches!(result, Err(ZmqError::MessageTooLarge(_, _))));

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_message_at_limit() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6912").await?;
    dish.set_option_raw(JOIN, b"big").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6912").await?;

    tokio::time::sleep(SETTLE).await;

    let at_limit = vec![0u8; 65503];
    let msg = make_msg("big", &at_limit);

    radio.send(msg).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.data().unwrap().len(), 65503);

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
#[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
async fn test_udp_iouring_reuseport_two_dishes_same_port() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish_1 = ctx.socket(SocketType::Dict).await?;
    dish_1.bind("udp://0.0.0.0:6920").await?;
    dish_1.set_option_raw(JOIN, b"shared").await?;

    let dish_2 = ctx.socket(SocketType::Dict).await?;
    dish_2.bind("udp://0.0.0.0:6920").await?;
    dish_2.set_option_raw(JOIN, b"shared").await?;

    tokio::time::sleep(SETTLE).await;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6920").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("shared", b"reuseport-test")).await?;

    let r1 = recv_timeout(&dish_1, LONG).await;
    let r2 = recv_timeout(&dish_2, LONG).await;

    assert!(
        r1.is_ok() || r2.is_ok(),
        "At least one dish should receive the message"
    );

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_ipv6_unicast() -> Result<(), Box<dyn std::error::Error>> {
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        println!("IPv6 not available, skipping test");
        return Ok(());
    }

    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://[::1]:6913").await?;
    dish.set_option_raw(JOIN, b"ipv6").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://[::1]:6913").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("ipv6", b"hello v6")).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.group(), Some(b"ipv6".as_ref()));
    assert_eq!(recv.data(), Some(b"hello v6".as_ref()));

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
async fn test_udp_iouring_invalid_endpoint_missing_port() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();
    let dish = ctx.socket(SocketType::Dict).await?;

    let result = dish.bind("udp://127.0.0.1").await;
    assert!(result.is_err());

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_multiple_datagrams() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6925").await?;
    dish.set_option_raw(JOIN, b"stream").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6925").await?;

    tokio::time::sleep(SETTLE).await;

    for i in 0..100 {
        radio.send(make_msg("stream", format!("msg-{}", i).as_bytes())).await?;
    }

    let mut received = Vec::new();
    for _ in 0..100 {
        match recv_timeout(&dish, LONG).await {
            Ok(msg) => {
                received.push(msg.data().unwrap().to_vec());
            }
            Err(e) => {
                panic!("Unexpected timeout: {:?}", e);
            }
        }
    }

    assert_eq!(received.len(), 100, "Should receive all 100 messages");

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_graceful_shutdown() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.bind("udp://0.0.0.0:6926").await?;

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.connect("udp://127.0.0.1:6926").await?;
    dish.set_option_raw(JOIN, b"shutdown").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("shutdown", b"before-drop")).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.data(), Some(b"before-drop".as_ref()));

    drop(radio);

    tokio::time::sleep(SETTLE).await;

    drop(dish);

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_context_shutdown() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.bind("udp://0.0.0.0:6927").await?;

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.connect("udp://127.0.0.1:6927").await?;
    dish.set_option_raw(JOIN, b"ctx").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("ctx", b"test")).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.data(), Some(b"test".as_ref()));

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_option_defaults() -> Result<(), Box<dyn std::error::Error>> {
    use rzmq::socket::options::{IO_URING_UDP_ENABLED, IO_URING_UDP_SNDZEROCOPY};
    
    let ctx = test_context();

    let radio = ctx.socket(SocketType::Radio).await?;
    
    let enabled_default: i32 = radio.get_option(IO_URING_UDP_ENABLED).await?;
    assert_eq!(enabled_default, 0, "IO_URING_UDP_ENABLED should default to 0");
    
    let zerocopy_default: i32 = radio.get_option(IO_URING_UDP_SNDZEROCOPY).await?;
    assert_eq!(zerocopy_default, 0, "IO_URING_UDP_SNDZEROCOPY should default to 0");
    
    radio.set_option(IO_URING_UDP_ENABLED, 1).await?;
    
    let enabled_after: i32 = radio.get_option(IO_URING_UDP_ENABLED).await?;
    assert_eq!(enabled_after, 1, "IO_URING_UDP_ENABLED should be 1 after setting");

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_works_without_uring_option() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6928").await?;
    dish.set_option_raw(JOIN, b"fallback").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6928").await?;

    tokio::time::sleep(SETTLE).await;

    radio.send(make_msg("fallback", b"no-uring-option")).await?;

    let recv = recv_timeout(&dish, LONG).await?;
    assert_eq!(recv.group(), Some(b"fallback".as_ref()));
    assert_eq!(recv.data(), Some(b"no-uring-option".as_ref()));

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_zerocopy_send() -> Result<(), Box<dyn std::error::Error>> {
    use rzmq::socket::options::IO_URING_UDP_SNDZEROCOPY;
    
    let ctx = test_context();

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.set_option(IO_URING_UDP_SNDZEROCOPY, 1).await?;
    radio.bind("udp://0.0.0.0:6930").await?;

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.connect("udp://127.0.0.1:6930").await?;
    dish.set_option_raw(JOIN, b"zc").await?;

    tokio::time::sleep(SETTLE).await;

    for i in 0..10 {
        radio.send(make_msg("zc", format!("zc-{}", i).as_bytes())).await?;
    }

    for _ in 0..10 {
        let recv = recv_timeout(&dish, LONG).await?;
        assert!(recv.data().unwrap().starts_with(b"zc-"));
    }

    ctx.term().await?;
    Ok(())
}

#[tokio::test]
#[serial_test::serial]
async fn test_udp_iouring_burst_throughput() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = test_context();

    let dish = ctx.socket(SocketType::Dict).await?;
    dish.bind("udp://127.0.0.1:6935").await?;
    dish.set_option_raw(JOIN, b"burst").await?;

    let radio = ctx.socket(SocketType::Radio).await?;
    radio.connect("udp://127.0.0.1:6935").await?;

    tokio::time::sleep(SETTLE).await;

    const BURST_COUNT: usize = 10000;
    for i in 0..BURST_COUNT {
        radio.send(make_msg("burst", format!("{}", i).as_bytes())).await?;
    }

    let mut received = 0;
    for _ in 0..BURST_COUNT {
        match recv_timeout(&dish, Duration::from_secs(10)).await {
            Ok(_) => received += 1,
            Err(ZmqError::Timeout) => break,
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    let success_rate = (received as f64 / BURST_COUNT as f64) * 100.0;
    assert!(
        success_rate >= 95.0,
        "Expected >= 95% delivery rate, got {:.1}%",
        success_rate
    );

    ctx.term().await?;
    Ok(())
}
