use rzmq::socket::options::JOIN;
use rzmq::{Context, SocketType, ZmqError};
use std::time::Duration;
use tokio::time::sleep;

const ADDR: &str = "udp://[::1]:5920";
const TIMEOUT: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("--- UDP Dish IPv6 Example ---");
    let ctx = Context::new()?;

    let dish = ctx.socket(SocketType::Dish)?;
    println!("[DISH] Binding to {}...", ADDR);
    dish.bind(ADDR).await?;
    dish.set_option_raw(JOIN, b"ipv6").await?;
    println!("[DISH] Bound and joined group 'ipv6'.");

    sleep(Duration::from_millis(100)).await;

    println!("[DISH] Waiting for messages...");
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

    dish.close().await?;
    ctx.term().await?;
    println!("--- Dish IPv6 Example Finished ---");
    Ok(())
}
