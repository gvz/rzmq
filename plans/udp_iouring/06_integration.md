# UDP io_uring Backend — Socket Layer & Command Processor Integration

## Overview

This document describes how the UDP io_uring path integrates with the existing
socket layer: socket options, command processor wiring, endpoint registration,
and lifecycle management.

## Socket Option: IO_URING_UDP_ENABLED

### Definition

```rust
// core/src/socket/options.rs

/// Enable io_uring for UDP transport on this socket (Linux only).
/// Must be set BEFORE bind()/connect(). Default: false.
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub const IO_URING_UDP_ENABLED: i32 = 1176;

/// Enable zero-copy sendmsg for UDP io_uring (Linux only).
/// Only effective when IO_URING_UDP_ENABLED is true. Default: false.
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub const IO_URING_UDP_SNDZEROCOPY: i32 = 1177;
```

### Storage

Add to the existing `IOURingSocketOptions` or create a new sub-struct:

```rust
// In SocketOptions
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub udp_uring: UdpUringOptions,

#[cfg(all(feature = "udp", feature = "io-uring"))]
#[derive(Debug, Clone)]
pub struct UdpUringOptions {
    pub enabled: bool,          // IO_URING_UDP_ENABLED, default false
    pub send_zerocopy: bool,    // IO_URING_UDP_SNDZEROCOPY, default false
}

#[cfg(all(feature = "udp", feature = "io-uring"))]
impl Default for UdpUringOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            send_zerocopy: false,
        }
    }
}
```

### Option Apply/Retrieve

Wire into `apply_core_option_value()` and `retrieve_core_option_value()`:

```rust
// In apply_core_option_value()
#[cfg(all(feature = "udp", feature = "io-uring"))]
IO_URING_UDP_ENABLED => {
    self.udp_uring.enabled = value != 0;
    Ok(())
}
#[cfg(all(feature = "udp", feature = "io-uring"))]
IO_URING_UDP_SNDZEROCOPY => {
    self.udp_uring.send_zerocopy = value != 0;
    Ok(())
}

// In retrieve_core_option_value()
#[cfg(all(feature = "udp", feature = "io-uring"))]
IO_URING_UDP_ENABLED => Ok(self.udp_uring.enabled as i64),
#[cfg(all(feature = "udp", feature = "io-uring"))]
IO_URING_UDP_SNDZEROCOPY => Ok(self.udp_uring.send_zerocopy as i64),
```

## Command Processor Wiring

The command processor (`core/src/socket/core/command_processor.rs`) dispatches
`bind()` and `connect()` calls. Currently, the `Endpoint::Udp` match arms
are not implemented (this is "Step 8" from the UDP plan). The io_uring path
adds a decision branch within those arms.

### Decision Flow

```
bind("udp://...")
    │
    ▼ parse_endpoint() → Endpoint::Udp(udp_endpoint, uri)
    │
    ▼ udp_socket_type_check(socket_type) → Radio or Dish only
    │
    ▼ Check: options.udp_uring.enabled?
    │
    ├── false → Tokio UDP path (existing code from plans/udp/05)
    │
    └── true → io_uring UDP path (new code)
```

### Radio bind with io_uring

```rust
// In handle_user_bind(), Endpoint::Udp arm, Radio case:
#[cfg(all(feature = "udp", feature = "io-uring"))]
if options.udp_uring.enabled {
    // 1. Create the UDP socket using socket2 (same as Tokio path)
    let (raw_socket, resolved_uri) = udp::create_bound_send_socket(&udp_ep, &options)?;

    // 2. Extract the raw FD before converting to Tokio
    let socket_fd = raw_socket.as_raw_fd();
    // Note: We need the raw FD, but the socket must stay alive.
    // Convert socket2::Socket to std::net::UdpSocket, take the RawFd,
    // and prevent close-on-drop.
    let std_sock = raw_socket.into_raw_fd(); // Takes ownership of FD

    // 3. Spawn UdpUringActor for sending
    let actor_handle = UdpUringActor::spawn(UdpUringActorConfig {
        handle: context.inner().next_handle(),
        socket_fd: std_sock,
        endpoint_uri: resolved_uri.clone(),
        ring_entries: 64,
        recv_buffer_count: 0,  // No recv for Radio
        recv_buffer_size: 0,
        pipe_read_id: 0,
        socket_logic: None,    // No recv delivery for Radio
        send_addr: Some(udp_ep.send_addr),
        send_zerocopy: options.udp_uring.send_zerocopy,
        send_channel_capacity: 1024,
        context: context.clone(),
    })?;

    // 4. Create UdpUringSendConnection
    let connection = Arc::new(UdpUringSendConnection {
        send_tx: actor_handle.send_tx.unwrap(),
        event_fd: actor_handle.event_fd.clone(),
        send_addr: udp_ep.send_addr,
        connection_id: context.inner().next_handle(),
    });

    // 5. Register endpoint
    let pipe_write_id = context.inner().next_handle();
    core_state.endpoints.insert(resolved_uri.clone(), EndpointInfo {
        endpoint_type: EndpointType::Session,
        mailbox: None, // No mailbox for io_uring actor
        task: None,    // Thread handle stored in actor_handle
        connection: Some(connection.clone() as Arc<dyn ISocketConnection>),
        uri: resolved_uri.clone(),
    });

    // 6. Notify socket of new pipe
    socket_logic.pipe_attached(
        pipe_write_id,
        connection as Arc<dyn ISocketConnection>,
    ).await?;

    // 7. Store actor handle for cleanup
    core_state.udp_uring_actors.insert(resolved_uri, actor_handle);

    return Ok(resolved_uri);
}
```

### Dish bind with io_uring

```rust
// In handle_user_bind(), Endpoint::Udp arm, Dish case:
#[cfg(all(feature = "udp", feature = "io-uring"))]
if options.udp_uring.enabled {
    // 1. Create the UDP socket using socket2
    //    (same setup as UdpReceiveActor::create_and_spawn but no Tokio conversion)
    let (socket_fd, resolved_uri) = create_raw_udp_socket_for_uring(&udp_ep, &options)?;

    // 2. Allocate pipe IDs
    let pipe_read_id = context.inner().next_handle();
    let pipe_write_id = context.inner().next_handle();

    // 3. Spawn UdpUringActor for receiving
    let actor_handle = UdpUringActor::spawn(UdpUringActorConfig {
        handle: context.inner().next_handle(),
        socket_fd,
        endpoint_uri: resolved_uri.clone(),
        ring_entries: 64,
        recv_buffer_count: 32,
        recv_buffer_size: 65536,
        pipe_read_id,
        socket_logic: Some(socket_logic.clone()),
        send_addr: None,       // No send for Dish bind
        send_zerocopy: false,
        send_channel_capacity: 0,
        context: context.clone(),
    })?;

    // 4. Register endpoint
    core_state.endpoints.insert(resolved_uri.clone(), EndpointInfo {
        endpoint_type: EndpointType::Listener,
        mailbox: None,
        task: None,
        connection: Some(Arc::new(DummyConnection) as Arc<dyn ISocketConnection>),
        uri: resolved_uri.clone(),
    });

    // 5. Notify socket of new pipe
    socket_logic.pipe_attached(
        pipe_read_id,
        Arc::new(DummyConnection) as Arc<dyn ISocketConnection>,
    ).await?;

    // 6. Store actor handle
    core_state.udp_uring_actors.insert(resolved_uri.clone(), actor_handle);

    return Ok(resolved_uri);
}
```

### Radio connect with io_uring

Similar to Radio bind, but the socket binds to ephemeral port:

```rust
// 1. Create unbound UDP socket (ephemeral port)
let socket_fd = create_raw_connected_udp_socket_for_uring(&udp_ep, &options)?;

// 2. Spawn UdpUringActor with send_addr = udp_ep.send_addr
// 3. Create UdpUringSendConnection
// 4. Register endpoint
// 5. Notify socket
```

### Dish connect with io_uring

Similar to Dish bind, but binds to `0.0.0.0:0`:

```rust
// 1. Create UDP socket bound to ephemeral port
// 2. Spawn UdpUringActor with recv config
// 3. Register as EndpointType::Session (not Listener)
// 4. Notify socket
```

## Socket Creation Helper

Since the io_uring path needs a raw FD (not a Tokio `UdpSocket`), we need
a helper that creates and configures the socket but stops short of converting
to Tokio:

```rust
/// Creates a raw UDP socket configured for io_uring use.
/// Returns (RawFd, resolved_uri). The caller owns the FD.
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) fn create_raw_udp_socket_for_uring(
    endpoint: &UdpEndpoint,
    options: &SocketOptions,
) -> Result<(RawFd, String), ZmqError> {
    // Same socket2 setup as existing create_bound_send_socket()
    // but instead of converting to Tokio:
    //   sock.set_nonblocking(true)
    //   let fd = sock.into_raw_fd()  // Takes ownership, prevents auto-close
    //   return (fd, resolved_uri)
    // ...
}
```

This reuses the existing `socket2` setup code (reuse_addr, reuse_port,
dual-stack, multicast config) but returns a raw FD instead of a Tokio socket.

## Endpoint Storage

Add a new field to `CoreState` for tracking UDP io_uring actor handles:

```rust
// In core/src/socket/core/state.rs or mod.rs
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) udp_uring_actors: HashMap<String, UdpUringActorHandle>,
```

This allows the shutdown path to stop actors when endpoints are unbound or
the socket is closed.

## Shutdown / Cleanup

When a UDP endpoint is unbound or the socket is closed:

```rust
// In the shutdown path
#[cfg(all(feature = "udp", feature = "io-uring"))]
if let Some(actor_handle) = core_state.udp_uring_actors.remove(&uri) {
    // Send Stop command
    let _ = actor_handle.control_tx.try_send(UdpUringCommand::Stop);
    // Signal eventfd to wake the actor
    let _ = actor_handle.event_fd.write(1u64);
    // Join the thread (blocking, should be quick after Stop)
    // Use tokio::task::spawn_blocking to avoid blocking the async runtime
    tokio::task::spawn_blocking(move || {
        let _ = actor_handle.join_handle.join();
    });
}
```

## Pipe Event Delivery: Recv Delivery Task

For Dish (receive) actors, a Tokio task bridges the io_uring thread to the
async socket layer:

```rust
/// Spawned as a Tokio task alongside the UdpUringActor.
/// Receives Commands from the actor's recv channel and delivers
/// them to the DishSocket's ISocket::handle_pipe_event().
#[cfg(all(feature = "udp", feature = "io-uring"))]
async fn udp_uring_recv_delivery_task(
    recv_rx: fibre::mpsc::BoundedAsyncReceiver<Command>,
    socket_logic: Arc<dyn ISocket>,
    pipe_read_id: usize,
    handle: usize,
) {
    tracing::debug!(
        handle = handle,
        "udp_uring_recv_delivery_task started"
    );

    loop {
        match recv_rx.recv().await {
            Ok(cmd) => {
                if let Err(e) = socket_logic.handle_pipe_event(pipe_read_id, cmd).await {
                    tracing::error!(
                        handle = handle,
                        "handle_pipe_event error: {}", e
                    );
                    break;
                }
            }
            Err(_) => {
                // Channel closed — actor has stopped
                tracing::debug!(
                    handle = handle,
                    "Recv delivery channel closed, task stopping"
                );
                break;
            }
        }
    }

    // Notify ISocket that the pipe has closed
    let cmd_closed = Command::PipeClosedByPeer { pipe_id: pipe_read_id };
    let _ = socket_logic.handle_pipe_event(pipe_read_id, cmd_closed).await;

    tracing::info!(handle = handle, "udp_uring_recv_delivery_task stopped");
}
```

The recv delivery channel is created inside `UdpUringActor::spawn()`:

```rust
let (recv_delivery_tx, recv_delivery_rx) = fibre::mpsc::bounded(4096);
// recv_delivery_tx goes to the actor (OS thread)
// recv_delivery_rx is used by the Tokio task
```

The Tokio task is spawned in the command processor right after spawning the
actor, and its `JoinHandle` is stored alongside the actor handle for cleanup.

## Integration with Existing Tokio UDP Path

The io_uring path is a **parallel alternative**, not a replacement:

```rust
// Pseudocode for the Endpoint::Udp bind arm:
Endpoint::Udp(udp_ep, uri) => {
    udp_socket_type_check(socket_type, &uri)?;

    match socket_type {
        SocketType::Radio => {
            #[cfg(all(feature = "udp", feature = "io-uring"))]
            if options.udp_uring.enabled {
                return self.handle_radio_bind_uring(udp_ep, uri, ...);
            }
            // Tokio path (default)
            return self.handle_radio_bind_tokio(udp_ep, uri, ...);
        }
        SocketType::Dish => {
            #[cfg(all(feature = "udp", feature = "io-uring"))]
            if options.udp_uring.enabled {
                return self.handle_dish_bind_uring(udp_ep, uri, ...);
            }
            // Tokio path (default)
            return self.handle_dish_bind_tokio(udp_ep, uri, ...);
        }
        _ => return Err(ZmqError::UnsupportedTransport(...)),
    }
}
```

This structure keeps the Tokio path as the default and only activates the
io_uring path when explicitly opted in via the socket option.
