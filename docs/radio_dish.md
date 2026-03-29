# Radio-Dish Pattern How-To Guide

This guide provides practical examples for using the Radio-Dish socket pattern in `rzmq`.

## Prerequisites

- Rust 1.70+
- `tokio` with `full` features
- `rzmq` crate with at least the default features

## When to Use Radio-Dish

Use Radio-Dish when you need:
- **Thread-safe** pub-sub messaging
- **Message groups** for fine-grained filtering
- Better performance through **sender-side filtering**

Avoid Radio-Dish when you need:
- Multipart messages (use Pub-Sub instead)
- Request-reply patterns (use REQ-REP or DEALER-ROUTER)

## Basic Example: One Publisher, One Subscriber

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use rzmq::socket::options::JOIN;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?;

    // Radio: the publisher
    let radio = ctx.socket(SocketType::Radio)?;
    
    // Dish: the subscriber  
    let dish = ctx.socket(SocketType::Dish)?;

    let endpoint = "tcp://127.0.0.1:5555";

    // Bind the radio (publisher side)
    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect the dish and subscribe to "alerts" group
    dish.connect(endpoint).await?;
    dish.set_option_raw(JOIN, b"alerts").await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a message with a group
    let mut msg = Msg::from_static(b"System alert!");
    msg.set_group("alerts")?;
    radio.send(msg).await?;

    // Receive the message
    let received = dish.recv().await?;
    println!("Data: {}", String::from_utf8_lossy(received.data().unwrap()));
    println!("Group: {}", String::from_utf8_lossy(received.group().unwrap()));

    ctx.term().await?;
    Ok(())
}
```

## Multiple Subscribers with Different Groups

Each Dish can subscribe to different groups independently:

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use rzmq::socket::options::JOIN;

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?;
    let radio = ctx.socket(SocketType::Radio)?;
    
    // Two different subscribers
    let sports_dish = ctx.socket(SocketType::Dish)?;
    let news_dish = ctx.socket(SocketType::Dish)?;

    let endpoint = "tcp://127.0.0.1:5556";

    radio.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Each dish joins a different group
    sports_dish.connect(endpoint).await?;
    sports_dish.set_option_raw(JOIN, b"sports").await?;

    news_dish.connect(endpoint).await?;
    news_dish.set_option_raw(JOIN, b"news").await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send to sports group
    let mut msg1 = Msg::from_static(b"Final score: 3-1");
    msg1.set_group("sports")?;
    radio.send(msg1).await?;

    // Send to news group
    let mut msg2 = Msg::from_static(b"Breaking news story");
    msg2.set_group("news")?;
    radio.send(msg2).await?;

    ctx.term().await?;
    Ok(())
}
```

## One Subscriber, Multiple Groups

A single Dish can subscribe to multiple groups:

```rust
// Dish subscribes to both "sports" and "news"
dish.set_option_raw(JOIN, b"sports").await?;
dish.set_option_raw(JOIN, b"news").await?;

// Now receives messages from both groups
```

## Unsubscribing from a Group

Use `LEAVE` to stop receiving messages from a specific group:

```rust
use rzmq::socket::options::{JOIN, LEAVE};

// Join a group
dish.set_option_raw(JOIN, b"alerts").await?;

// Later, leave the group
dish.set_option_raw(LEAVE, b"alerts").await?;
```

## Error Handling

### Sending without a Group

Radio sockets require every message to have a group. Attempting to send without one returns an error:

```rust
let msg = Msg::from_static(b"no group");
let result = radio.send(msg).await;
// Result: Err(InvalidState("Message does not have group")))
```

### Invalid Group Names

Setting a group with invalid characters fails:

```rust
// Empty group - error
dish.set_option_raw(JOIN, b"").await?; // Err

// Group with null byte - error  
dish.set_option_raw(JOIN, b"bad\x00group").await?; // Err

// Group too long (256+ bytes) - error
let long_group = vec![b'a'; 256];
dish.set_option_raw(JOIN, &long_group).await?; // Err
```

## Complete Example: Event Distribution System

This example demonstrates a typical event distribution system with multiple event types:

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use rzmq::socket::options::JOIN;
use std::time::Duration;

async fn run_event_system() -> Result<(), ZmqError> {
    let ctx = Context::new()?;
    
    // Event broadcaster (Radio)
    let broadcaster = ctx.socket(SocketType::Radio)?;
    
    // Event consumers (Dishes)
    let logging_service = ctx.socket(SocketType::Dish)?;
    let metrics_service = ctx.socket(SocketType::Dish)?;
    let alert_service = ctx.socket(SocketType::Dish)?;

    let endpoint = "tcp://127.0.0.1:5557";
    broadcaster.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Set up subscriptions
    logging_service.connect(endpoint).await?;
    logging_service.set_option_raw(JOIN, b"log").await?;

    metrics_service.connect(endpoint).await?;
    metrics_service.set_option_raw(JOIN, b"metric").await?;

    alert_service.connect(endpoint).await?;
    alert_service.set_option_raw(JOIN, b"alert").await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Broadcast various events
    let events = vec![
        ("log", "Application started"),
        ("metric", "cpu:45"),
        ("alert", "High memory usage!"),
        ("log", "User logged in"),
    ];

    for (group, data) in events {
        let mut msg = Msg::from_static(data.as_bytes());
        msg.set_group(group)?;
        broadcaster.send(msg).await?;
    }

    // Let the system settle
    tokio::time::sleep(Duration::from_millis(100)).await;

    ctx.term().await?;
    Ok(())
}
```

## Using with Different Transports

Radio-Dish supports multiple transports:

### TCP (recommended for network)

```rust
let endpoint = "tcp://127.0.0.1:5558";
radio.bind(endpoint).await?;
dish.connect(endpoint).await?;
```

### IPC (Unix Domain Sockets)

Requires the `ipc` feature:

```toml
rzmq = { version = "...", features = ["ipc"] }
```

```rust
let endpoint = "ipc:///tmp/my_events.ipc";
radio.bind(&endpoint).await?;
dish.connect(&endpoint).await?;
```

### In-Process

Requires the `inproc` feature for same-process communication:

```toml
rzmq = { version = "...", features = ["inproc"] }
```

```rust
let endpoint = "inproc://events";
radio.bind(endpoint).await?;
dish.connect(endpoint).await?;
```

## See Also

- [Radio-Dish RFC Specification](../../radio_dish_rfc.md)
- [Usage Guide](../core/README.USAGE.md)
- API Reference: `SocketType::Radio`, `SocketType::Dish`
