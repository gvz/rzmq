// tests/radio_dish.rs

use rzmq::socket::options::{JOIN, LEAVE};
use rzmq::{Msg, SocketType, ZmqError};
use serial_test::serial;
use std::time::Duration;
mod common;

const SHORT_TIMEOUT: Duration = Duration::from_millis(200);
const LONG_TIMEOUT: Duration = Duration::from_secs(3);
const SETTLE: Duration = Duration::from_millis(100);

#[tokio::test]
#[serial]
async fn test_radio_dish_basic_tcp() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = "tcp://127.0.0.1:5800";

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(endpoint).await?;
    dish.set_option_raw(JOIN, b"news").await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg = Msg::from_static(b"breaking news content");
    msg.set_group("news")?;
    radio.send(msg).await?;

    let received = common::recv_timeout(&dish, LONG_TIMEOUT).await?;
    assert_eq!(received.data().unwrap(), b"breaking news content" as &[u8]);
    assert_eq!(received.group(), Some(b"news" as &[u8]));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_radio_dish_no_join_drops_messages() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = "tcp://127.0.0.1:5801";

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(endpoint).await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg = Msg::from_static(b"alert message");
    msg.set_group("alerts")?;
    radio.send(msg).await?;

    let result = common::recv_timeout(&dish, SHORT_TIMEOUT).await;
    assert!(matches!(result, Err(ZmqError::Timeout)));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_radio_dish_multiple_groups() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = "tcp://127.0.0.1:5802";

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(endpoint).await?;
    dish.set_option_raw(JOIN, b"sports").await?;
    dish.set_option_raw(JOIN, b"news").await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg_sports = Msg::from_static(b"sports score");
    msg_sports.set_group("sports")?;
    radio.send(msg_sports).await?;

    let mut msg_news = Msg::from_static(b"news update");
    msg_news.set_group("news")?;
    radio.send(msg_news).await?;

    let mut msg_weather = Msg::from_static(b"weather forecast");
    msg_weather.set_group("weather")?;
    radio.send(msg_weather).await?;

    let received1 = common::recv_timeout(&dish, LONG_TIMEOUT).await?;
    let received2 = common::recv_timeout(&dish, LONG_TIMEOUT).await?;

    let groups: Vec<_> = [received1.group(), received2.group()].into_iter().collect();
    assert!(groups.contains(&Some(b"sports" as &[u8])));
    assert!(groups.contains(&Some(b"news" as &[u8])));

    let result = common::recv_timeout(&dish, SHORT_TIMEOUT).await;
    assert!(matches!(result, Err(ZmqError::Timeout)));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_radio_dish_leave_group() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = "tcp://127.0.0.1:5803";

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(endpoint).await?;
    dish.set_option_raw(JOIN, b"updates").await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg1 = Msg::from_static(b"update 1");
    msg1.set_group("updates")?;
    radio.send(msg1).await?;

    let received = common::recv_timeout(&dish, LONG_TIMEOUT).await?;
    assert_eq!(received.group(), Some(b"updates" as &[u8]));

    dish.set_option_raw(LEAVE, b"updates").await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg2 = Msg::from_static(b"update 2");
    msg2.set_group("updates")?;
    radio.send(msg2).await?;

    let result = common::recv_timeout(&dish, SHORT_TIMEOUT).await;
    assert!(matches!(result, Err(ZmqError::Timeout)));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_radio_send_without_group_errors() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let msg = Msg::from_static(b"hello");
    let result = radio.send(msg).await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_radio_cannot_recv() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let result = radio.recv().await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_dish_cannot_send() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let dish = ctx.socket(SocketType::Dish)?;
    let msg = Msg::from_static(b"hello");
    let result = dish.send(msg).await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_radio_no_multipart() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let result = radio.send_multipart(vec![Msg::from_static(b"part1")]).await;
    assert!(matches!(result, Err(ZmqError::InvalidState(_))));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_dish_received_msg_has_correct_group() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = "tcp://127.0.0.1:5804";

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(endpoint).await?;
    dish.set_option_raw(JOIN, b"sensor-data").await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg = Msg::from_static(b"42.5");
    msg.set_group("sensor-data")?;
    radio.send(msg).await?;

    let received = common::recv_timeout(&dish, LONG_TIMEOUT).await?;
    assert_eq!(received.group(), Some(b"sensor-data" as &[u8]));
    assert_eq!(received.data().unwrap(), b"42.5" as &[u8]);
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_radio_multiple_dishes_independent_groups() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish_a = ctx.socket(SocketType::Dish)?;
    let dish_b = ctx.socket(SocketType::Dish)?;
    let dish_ab = ctx.socket(SocketType::Dish)?;
    let endpoint = "tcp://127.0.0.1:5805";

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish_a.connect(endpoint).await?;
    dish_a.set_option_raw(JOIN, b"alpha").await?;

    dish_b.connect(endpoint).await?;
    dish_b.set_option_raw(JOIN, b"beta").await?;

    dish_ab.connect(endpoint).await?;
    dish_ab.set_option_raw(JOIN, b"alpha").await?;
    dish_ab.set_option_raw(JOIN, b"beta").await?;

    tokio::time::sleep(SETTLE).await;

    let mut msg_alpha = Msg::from_static(b"alpha data");
    msg_alpha.set_group("alpha")?;
    radio.send(msg_alpha).await?;

    let mut msg_beta = Msg::from_static(b"beta data");
    msg_beta.set_group("beta")?;
    radio.send(msg_beta).await?;

    let received_a = common::recv_timeout(&dish_a, LONG_TIMEOUT).await?;
    assert_eq!(received_a.group(), Some(b"alpha" as &[u8]));

    let result_beta_a = common::recv_timeout(&dish_a, SHORT_TIMEOUT).await;
    assert!(matches!(result_beta_a, Err(ZmqError::Timeout)));

    let received_b = common::recv_timeout(&dish_b, LONG_TIMEOUT).await?;
    assert_eq!(received_b.group(), Some(b"beta" as &[u8]));

    let result_alpha_b = common::recv_timeout(&dish_b, SHORT_TIMEOUT).await;
    assert!(matches!(result_alpha_b, Err(ZmqError::Timeout)));

    let received_ab_1 = common::recv_timeout(&dish_ab, LONG_TIMEOUT).await?;
    let received_ab_2 = common::recv_timeout(&dish_ab, LONG_TIMEOUT).await?;
    let groups_ab: Vec<_> = [received_ab_1.group(), received_ab_2.group()]
      .into_iter()
      .collect();
    assert!(groups_ab.contains(&Some(b"alpha" as &[u8])));
    assert!(groups_ab.contains(&Some(b"beta" as &[u8])));
  }
  ctx.term().await?;
  Ok(())
}

#[cfg(feature = "ipc")]
#[tokio::test]
async fn test_radio_dish_ipc() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = common::unique_ipc_endpoint();

    radio.bind(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(&endpoint).await?;
    dish.set_option_raw(JOIN, b"ipcgroup").await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg = Msg::from_static(b"ipc message");
    msg.set_group("ipcgroup")?;
    radio.send(msg).await?;

    let received = common::recv_timeout(&dish, LONG_TIMEOUT).await?;
    assert_eq!(received.data().unwrap(), b"ipc message" as &[u8]);
    assert_eq!(received.group(), Some(b"ipcgroup" as &[u8]));
  }
  ctx.term().await?;
  Ok(())
}

#[cfg(feature = "inproc")]
#[tokio::test]
async fn test_radio_dish_inproc() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = common::unique_inproc_endpoint();

    radio.bind(&endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(&endpoint).await?;
    dish.set_option_raw(JOIN, b"inprocgroup").await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg = Msg::from_static(b"inproc message");
    msg.set_group("inprocgroup")?;
    radio.send(msg).await?;

    let received = common::recv_timeout(&dish, LONG_TIMEOUT).await?;
    assert_eq!(received.data().unwrap(), b"inproc message" as &[u8]);
    assert_eq!(received.group(), Some(b"inprocgroup" as &[u8]));
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
async fn test_invalid_group_rejected_by_join() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let dish = ctx.socket(SocketType::Dish)?;

    let result = dish.set_option_raw(JOIN, b"").await;
    assert!(result.is_err());

    let result2 = dish.set_option_raw(JOIN, b"bad\x00group").await;
    assert!(result2.is_err());

    let long_group = vec![b'a'; 256];
    let result3 = dish.set_option_raw(JOIN, &long_group).await;
    assert!(result3.is_err());
  }
  ctx.term().await?;
  Ok(())
}

#[tokio::test]
#[serial]
async fn test_dish_join_replayed_on_connect() -> Result<(), ZmqError> {
  let ctx = common::test_context();
  {
    let radio = ctx.socket(SocketType::Radio)?;
    let dish = ctx.socket(SocketType::Dish)?;
    let endpoint = "tcp://127.0.0.1:5806";

    dish.set_option_raw(JOIN, b"live").await?;

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    dish.connect(endpoint).await?;
    tokio::time::sleep(SETTLE).await;

    let mut msg = Msg::from_static(b"live content");
    msg.set_group("live")?;
    radio.send(msg).await?;

    let received = common::recv_timeout(&dish, LONG_TIMEOUT).await?;
    assert_eq!(received.group(), Some(b"live" as &[u8]));
    assert_eq!(received.data().unwrap(), b"live content" as &[u8]);
  }
  ctx.term().await?;
  Ok(())
}
