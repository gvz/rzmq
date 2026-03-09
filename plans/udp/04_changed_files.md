# UDP Transport — Changed Files

## `core/src/error.rs`

Add one new variant to `ZmqError`:

```rust
/// The message frame is too large for the transport's maximum datagram size.
/// For UDP, the practical limit is 65507 bytes (IPv4 header math).
/// Fields: (actual_encoded_length, maximum_allowed_length)
#[error("Message too large: {0} bytes exceeds transport maximum of {1} bytes")]
MessageTooLarge(usize, usize),
```

No existing variants change. The `#[non_exhaustive]` attribute already covers
forward-compatibility.

---

## `core/src/transport/mod.rs`

Add two feature-gated module declarations:

```rust
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "udp")]
pub mod udp_endpoint;
```

No other changes.

---

## `core/src/transport/endpoint.rs`

### Add `Udp` variant to `Endpoint`

```rust
#[cfg(feature = "udp")]
Udp(crate::transport::udp_endpoint::UdpEndpoint, String),
//  ^^^^ parsed endpoint struct          ^^^^ original URI string
```

### Extend `parse_endpoint`

Add a new match arm inside the `match scheme { ... }` block:

```rust
#[cfg(feature = "udp")]
"udp" => {
    if address_part.is_empty() {
        Err(invalid_endpoint_err())
    } else {
        crate::transport::udp_endpoint::parse_udp_endpoint(
            address_part,
            endpoint_str,
        )
        .map(|ep| Endpoint::Udp(ep, endpoint_str.to_string()))
    }
}
```

---

## `core/src/socket/options.rs`

### New constants (all feature-gated)

```rust
/// Enable/disable multicast loopback (IP_MULTICAST_LOOP / IPV6_MULTICAST_LOOP).
/// Value: i32, 0 = disabled, 1 = enabled (default). libzmq: ZMQ_MULTICAST_LOOP.
#[cfg(feature = "udp")]
pub const UDP_MULTICAST_LOOP: i32 = 90;

/// Multicast TTL / hop limit (IP_MULTICAST_TTL / IPV6_MULTICAST_HOPS).
/// Value: i32, range 0–255, default 1. libzmq: ZMQ_MULTICAST_HOPS.
#[cfg(feature = "udp")]
pub const UDP_MULTICAST_HOPS: i32 = 91;
```

Values 90 and 91 are unused in the existing options.rs. They intentionally
match libzmq's `ZMQ_MULTICAST_LOOP=96` and `ZMQ_MULTICAST_HOPS=25` in spirit
but use project-local values. If strict libzmq compatibility is later required,
remap to 96 and 25.

### New sub-struct

```rust
#[cfg(feature = "udp")]
#[derive(Debug, Clone)]
pub struct UdpSocketOptions {
    /// Whether multicast datagrams loop back to the sender's host.
    /// Corresponds to IP_MULTICAST_LOOP / IPV6_MULTICAST_LOOP.
    pub multicast_loop: bool,

    /// TTL for IPv4 multicast / hop limit for IPv6 multicast.
    /// Corresponds to IP_MULTICAST_TTL / IPV6_MULTICAST_HOPS.
    pub multicast_hops: u8,
}

#[cfg(feature = "udp")]
impl Default for UdpSocketOptions {
    fn default() -> Self {
        Self {
            multicast_loop: true,  // OS default is enabled
            multicast_hops: 1,     // Same-subnet only by default
        }
    }
}
```

### Add field to `SocketOptions`

```rust
#[cfg(feature = "udp")]
pub udp: UdpSocketOptions,
```

Add `udp: Default::default()` in `SocketOptions::default()`.

### Wire up in `apply_core_option_value`

```rust
#[cfg(feature = "udp")]
UDP_MULTICAST_LOOP => {
    options.udp.multicast_loop = parse_bool_option(value)?;
}
#[cfg(feature = "udp")]
UDP_MULTICAST_HOPS => {
    let v = parse_i32_option(value)?;
    if !(0..=255).contains(&v) {
        return Err(ZmqError::InvalidOptionValue(option_id));
    }
    options.udp.multicast_hops = v as u8;
}
```

### Wire up in `retrieve_core_option_value`

```rust
#[cfg(feature = "udp")]
UDP_MULTICAST_LOOP => Ok((options.udp.multicast_loop as i32).to_ne_bytes().to_vec()),
#[cfg(feature = "udp")]
UDP_MULTICAST_HOPS => Ok((options.udp.multicast_hops as i32).to_ne_bytes().to_vec()),
```

---

## `core/src/socket/connection_iface.rs`

### Add `UdpSendConnection`

```rust
#[cfg(feature = "udp")]
#[derive(Debug)]
pub(crate) struct UdpSendConnection {
    /// The underlying (Tokio) UDP socket.
    /// Arc so it can be shared if the same local socket sends to multiple targets
    /// (future: multiple Radio connect endpoints sharing one bound socket).
    pub(crate) socket: Arc<tokio::net::UdpSocket>,

    /// Destination address: peer (unicast/bcast) or multicast group.
    pub(crate) send_addr: std::net::SocketAddr,

    /// Stable ID for this connection entry. Derived from context handle counter.
    pub(crate) connection_id: usize,
}

#[cfg(feature = "udp")]
impl UdpSendConnection {
    pub(crate) fn new(
        socket: Arc<tokio::net::UdpSocket>,
        send_addr: std::net::SocketAddr,
        connection_id: usize,
    ) -> Self {
        Self { socket, send_addr, connection_id }
    }
}

#[cfg(feature = "udp")]
#[async_trait]
impl ISocketConnection for UdpSendConnection {
    /// Sends a single pre-encoded RADIO-DISH datagram.
    ///
    /// `msgs` must contain exactly one element — the already-encoded frame
    /// produced by `RadioSocket::encode_radio_frame`:
    ///   `[ group_len: u8 | group: bytes | payload: bytes ]`
    ///
    /// Enforces the 65507-byte datagram limit before sending.
    async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
        if msgs.len() != 1 {
            return Err(ZmqError::InvalidState(
                "UDP Radio-Dish requires exactly one frame per send",
            ));
        }
        let data = msgs[0].data().unwrap_or(&[]);
        const UDP_MAX: usize = 65507;
        if data.len() > UDP_MAX {
            return Err(ZmqError::MessageTooLarge(data.len(), UDP_MAX));
        }
        self.socket
            .send_to(data, self.send_addr)
            .await
            .map(|_| ())
            .map_err(ZmqError::from)
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
        // UDP is connectionless; no teardown needed.
        Ok(())
    }

    fn get_connection_id(&self) -> usize {
        self.connection_id
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
```

### Why `send_multipart` receives a pre-encoded blob

The existing `RadioSocket::send` calls `encode_radio_frame` and passes the
resulting single-frame `Msg` to `Distributor::send_to_all`, which in turn
calls `ISocketConnection::send_multipart(vec![encoded_msg])` on each peer.
`UdpSendConnection` plugs into this path unchanged — it just takes those raw
bytes and fires them as a datagram.

### No changes to `DishSocket` or `RadioSocket`

The `ISocket` implementations for Radio and Dish already have the correct
encode/decode logic. No pattern-level changes are needed.

---

## `core/Cargo.toml`

```toml
[features]
udp        = []
full       = ["default", "ipc", "inproc", "plain", "noise_xx", "curve", "udp"]
full-linux = ["full", "io-uring"]
# default remains unchanged — does NOT include udp

[[test]]
name = "udp_radio_dish"
required-features = ["udp"]
```
