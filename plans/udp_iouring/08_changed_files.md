# UDP io_uring Backend — Changed Files

## File 1: `core/src/io_uring_backend/mod.rs`

**Changes:** Add module declarations for new UDP io_uring files.

```rust
// Add after existing module declarations:
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) mod udp_uring_actor;
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) mod udp_send_connection;
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) mod udp_recv_delivery;
```

**Line count:** ~4 lines added

---

## File 2: `core/src/socket/options.rs`

**Changes:** Add `IO_URING_UDP_ENABLED` and `IO_URING_UDP_SNDZEROCOPY` options.

### New Constants (~lines 74-78, after existing IO_URING constants)

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub const IO_URING_UDP_ENABLED: i32 = 1176;
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub const IO_URING_UDP_SNDZEROCOPY: i32 = 1177;
```

### New Struct (~after UdpSocketOptions)

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
#[derive(Debug, Clone)]
pub struct UdpUringOptions {
    pub enabled: bool,
    pub send_zerocopy: bool,
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

### New Field on SocketOptions (~line 135)

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub udp_uring: UdpUringOptions,
```

### apply_core_option_value() additions (~line 615)

```rust
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
```

### retrieve_core_option_value() additions (~line 676)

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
IO_URING_UDP_ENABLED => Ok(self.udp_uring.enabled as i64),
#[cfg(all(feature = "udp", feature = "io-uring"))]
IO_URING_UDP_SNDZEROCOPY => Ok(self.udp_uring.send_zerocopy as i64),
```

**Line count:** ~30 lines added

---

## File 3: `core/src/socket/core/command_processor.rs`

**Changes:** Add io_uring UDP path in the `Endpoint::Udp` match arms for
both `handle_user_bind` and `handle_user_connect`.

### In `handle_user_bind`, `Endpoint::Udp` arm:

The existing arm (once Step 8 from the UDP plan is implemented) handles the
Tokio path. Add an io_uring branch **before** the Tokio path:

```rust
Endpoint::Udp(udp_ep, uri) => {
    udp_socket_type_check(core_state.socket_type, &uri)?;

    match core_state.socket_type {
        SocketType::Radio => {
            #[cfg(all(feature = "udp", feature = "io-uring"))]
            if core_state.options.udp_uring.enabled {
                return self.handle_radio_bind_uring(
                    &udp_ep, &uri, core_state, socket_logic, context,
                ).await;
            }
            // ... existing Tokio Radio bind path ...
        }
        SocketType::Dish => {
            #[cfg(all(feature = "udp", feature = "io-uring"))]
            if core_state.options.udp_uring.enabled {
                return self.handle_dish_bind_uring(
                    &udp_ep, &uri, core_state, socket_logic, context,
                ).await;
            }
            // ... existing Tokio Dish bind path ...
        }
        _ => return Err(ZmqError::UnsupportedTransport(uri)),
    }
}
```

### New helper methods on the command processor:

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
async fn handle_radio_bind_uring(...) -> Result<String, ZmqError> { ... }

#[cfg(all(feature = "udp", feature = "io-uring"))]
async fn handle_dish_bind_uring(...) -> Result<String, ZmqError> { ... }

#[cfg(all(feature = "udp", feature = "io-uring"))]
async fn handle_radio_connect_uring(...) -> Result<String, ZmqError> { ... }

#[cfg(all(feature = "udp", feature = "io-uring"))]
async fn handle_dish_connect_uring(...) -> Result<String, ZmqError> { ... }
```

Each method follows the pattern described in `06_integration.md`:
1. Create raw UDP socket (via `udp_uring.rs` helpers)
2. Spawn `UdpUringActor`
3. Create send/recv channels as appropriate
4. Register endpoint in `CoreState`
5. Notify `ISocket` of pipe attachment
6. Store actor handle

**Line count:** ~250 lines added (4 handler methods)

---

## File 4: `core/src/socket/core/state.rs` (or `mod.rs`)

**Changes:** Add storage for UDP io_uring actor handles in `CoreState`.

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
use crate::io_uring_backend::udp_uring_actor::UdpUringActorHandle;

// In CoreState:
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) udp_uring_actors: HashMap<String, UdpUringActorHandle>,
```

Initialize in `CoreState::new()`:
```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
udp_uring_actors: HashMap::new(),
```

**Line count:** ~8 lines added

---

## File 5: `core/src/socket/core/shutdown.rs`

**Changes:** Add cleanup for UDP io_uring actors during socket shutdown.

```rust
// In the shutdown sequence, after cleaning up regular endpoints:
#[cfg(all(feature = "udp", feature = "io-uring"))]
{
    let actors: Vec<(String, UdpUringActorHandle)> =
        core_state.udp_uring_actors.drain().collect();
    for (uri, actor_handle) in actors {
        tracing::debug!("Stopping UDP io_uring actor for {}", uri);
        let _ = actor_handle.control_tx.try_send(UdpUringCommand::Stop);
        let _ = actor_handle.event_fd.write(1u64);
        // Join is blocking; use spawn_blocking
        tokio::task::spawn_blocking(move || {
            match actor_handle.join_handle.join() {
                Ok(Ok(())) => tracing::debug!("UDP uring actor joined cleanly"),
                Ok(Err(e)) => tracing::error!("UDP uring actor error: {}", e),
                Err(_) => tracing::error!("UDP uring actor panicked"),
            }
        });
    }
}
```

**Line count:** ~15 lines added

---

## File 6: `core/src/transport/mod.rs`

**Changes:** Add module declaration for `udp_uring`.

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) mod udp_uring;
```

**Line count:** ~2 lines added

---

## File 7: `core/Cargo.toml`

**Changes:** Add test and benchmark entries.

```toml
# Under [[test]] entries:
[[test]]
name = "udp_iouring_radio_dish"
required-features = ["udp", "io-uring"]

# Under [[bench]] entries:
[[bench]]
name = "udp_throughput"
harness = false
required-features = ["udp", "io-uring"]
```

**Line count:** ~8 lines added

---

## Summary of All Changes

| File | Lines Added | Nature |
|---|---|---|
| `io_uring_backend/mod.rs` | ~4 | Module declarations |
| `socket/options.rs` | ~30 | Socket options + struct |
| `socket/core/command_processor.rs` | ~250 | io_uring bind/connect handlers |
| `socket/core/state.rs` | ~8 | Actor handle storage |
| `socket/core/shutdown.rs` | ~15 | Actor cleanup |
| `transport/mod.rs` | ~2 | Module declaration |
| `Cargo.toml` | ~8 | Test/bench entries |
| **Total** | **~317** | |
