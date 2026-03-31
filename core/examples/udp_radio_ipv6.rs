use rzmq::{Context, Msg, SocketType, ZmqError};
use std::time::Duration;
use tokio::time::sleep;

const ADDR: &str = "udp://[::1]:5920";

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("--- UDP Radio IPv6 Example ---");
    let ctx = Context::new()?;

    let radio = ctx.socket(SocketType::Radio)?;
    println!("[RADIO] Connecting to {}...", ADDR);
    radio.connect(ADDR).await?;
    println!("[RADIO] Connected.");

    sleep(Duration::from_millis(100)).await;

    println!("[RADIO] Sending messages...");
    for i in 0..5 {
        let msg = Msg::from_vec(format!("Hello from IPv6 #{}", i).into_bytes());
        let mut m = msg;
        m.set_group("ipv6".to_string())?;
        radio.send(m).await?;
        println!("[RADIO] Sent message #{}", i);
    }

    sleep(Duration::from_millis(100)).await;
    radio.close().await?;
    ctx.term().await?;
    println!("--- Radio IPv6 Example Finished ---");
    Ok(())
}
