use rzmq::socket::options::JOIN;
use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
use tokio::time::sleep;

const ADDR: &str = "udp://[::1]:5921";

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("--- UDP Radio-Dish IPv6 Example ---");
    let ctx = Context::new()?;

    let dish = ctx.socket(SocketType::Dish)?;
    println!("[DISH] Binding to {}...", ADDR);
    dish.bind(ADDR).await?;
    dish.set_option_raw(JOIN, b"ipv6").await?;
    println!("[DISH] Bound and joined group 'ipv6'.");

    let radio = ctx.socket(SocketType::Radio)?;
    println!("[RADIO] Connecting to {}...", ADDR);
    radio.connect(ADDR).await?;
    println!("[RADIO] Connected.");

    sleep(Duration::from_millis(100)).await;

    println!("[RADIO] Sending 5 messages...");
    for i in 0..5 {
        let mut m = Msg::from_vec(format!("Hello IPv6 #{}", i).into_bytes());
        m.set_group("ipv6".to_string())?;
        radio.send(m).await?;
        println!("[RADIO] Sent #{}", i);
    }

    println!("[DISH] Receiving messages...");
    for _ in 0..5 {
        match dish.recv().await {
            Ok(msg) => {
                let group = msg.group().map(|g| String::from_utf8_lossy(g).to_string()).unwrap_or_else(String::new);
                let data = String::from_utf8_lossy(msg.data().unwrap_or(&[][..]));
                println!("[DISH] Received: group={}, data={}", group, data);
            }
            Err(e) => println!("[DISH] Error: {}", e),
        }
    }

    radio.close().await?;
    dish.close().await?;
    ctx.term().await?;
    println!("--- Radio-Dish IPv6 Example Finished ---");
    Ok(())
}
