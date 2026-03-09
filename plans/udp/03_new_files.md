# UDP Transport — New Files

## `core/src/transport/udp_endpoint.rs`

Complete implementation spec for the endpoint parser.

```rust
use crate::error::ZmqError;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

#[derive(Debug, Clone)]
pub(crate) enum UdpMode {
    Unicast,
    Broadcast,
    Multicast {
        iface: String,
        group: IpAddr,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct UdpEndpoint {
    /// Local address the socket will be bound to.
    /// Dish bind:    0.0.0.0:port (mcast/bcast) or addr:port (unicast)
    /// Radio bind:   0.0.0.0:port (user-specified local port)
    /// Radio connect: 0.0.0.0:0  (OS assigns ephemeral port)
    /// Dish connect:  0.0.0.0:0  (OS assigns ephemeral port; receives from any)
    pub bind_addr: SocketAddr,

    /// Destination for outgoing datagrams (Radio side only).
    /// Unicast/broadcast: peer address.
    /// Multicast: multicast group address.
    pub send_addr: SocketAddr,

    pub mode: UdpMode,
    pub is_ipv6: bool,
    pub original_uri: String,
}

/// Parses the address part of a `udp://` endpoint (scheme already stripped).
///
/// # Formats accepted
/// - `addr:port`              — unicast or broadcast
/// - `[ipv6addr]:port`        — IPv6 unicast
/// - `iface;group:port`       — IPv4 multicast
/// - `iface;[ipv6group]:port` — IPv6 multicast
pub(crate) fn parse_udp_endpoint(address_part: &str, original_uri: &str)
    -> Result<UdpEndpoint, ZmqError>
```

### Parsing Algorithm

```
Step 1 — Check for multicast (';' present)
──────────────────────────────────────────
  Split at first ';':
    iface_str = left of ';'
    rest      = right of ';'

  Parse group_and_port from rest:
    If rest starts with '[' → IPv6 bracket notation
      find closing ']', extract ipv6_str
      expect ':' after ']', parse port
      group = parse ipv6_str as Ipv6Addr
      is_ipv6 = true
      bind_addr = SocketAddr::new(Ipv6Addr::UNSPECIFIED, port)
    Else → IPv4
      split at last ':' → (ipv4_str, port_str)
      group = parse ipv4_str as Ipv4Addr
      is_ipv6 = false
      bind_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED, port)

  send_addr = SocketAddr::new(group, port)
  mode = UdpMode::Multicast { iface: iface_str, group }
  return Ok(UdpEndpoint { bind_addr, send_addr, mode, is_ipv6, original_uri })

Step 2 — No ';' → unicast or broadcast
────────────────────────────────────────
  If address_part starts with '[' → IPv6 bracket notation
    find closing ']', extract ipv6_str
    expect ':' after ']', parse port
    addr = parse ipv6_str as Ipv6Addr
    socket_addr = SocketAddr::new(addr, port)
    is_ipv6 = true
    bind_addr = socket_addr
    send_addr = socket_addr
    mode = UdpMode::Unicast
  Else → IPv4 or hostname
    split at last ':' → (host_str, port_str)
    parse port_str as u16
    Try parsing host_str as Ipv4Addr:
      if addr is 255.255.255.255 → mode = Broadcast
      else                       → mode = Unicast
      is_ipv6 = false
    If IPv4 parse fails → treat as hostname:
      resolve via ToSocketAddrs (blocking DNS)
      detect is_ipv6 from resolved address family
      mode = Unicast
    bind_addr = SocketAddr::new(resolved_addr, port)
    send_addr = SocketAddr::new(resolved_addr, port)

Step 3 — Validation
────────────────────
  port must be non-zero
  for Multicast: group address must be in multicast range
    IPv4: 224.0.0.0/4
    IPv6: ff00::/8
  return Err(ZmqError::InvalidEndpoint(...)) on any failure
```

---

## `core/src/transport/udp.rs`

The receive-side actor. The send side is handled by `UdpSendConnection` in
`connection_iface.rs` (see 04_changed_files.md).

### `UdpReceiveActor`

```rust
use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{ActorDropGuard, ActorType, Command,
                     MailboxReceiver, MailboxSender, mailbox};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::core::state::{EndpointInfo, EndpointType};
use crate::socket::events::MonitorSender;
use crate::transport::udp_endpoint::{UdpEndpoint, UdpMode};

use socket2::{Domain, Protocol, Socket as Socket2, Type};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

const UDP_MAX_DATAGRAM: usize = 65535;

pub(crate) struct UdpReceiveActor {
    handle: usize,
    endpoint_uri: String,
    socket: Arc<UdpSocket>,
    socket_logic: Arc<dyn ISocket>,
    pipe_read_id: usize,
    mailbox_receiver: MailboxReceiver,
    context: Context,
}
```

### `create_and_spawn` signature

```rust
pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: &UdpEndpoint,
    socket_logic: Arc<dyn ISocket>,
    context: Context,
    parent_socket_id: usize,
    monitor_tx: Option<MonitorSender>,
    core_arc: Arc<SocketCore>,
    pipe_read_id: usize,
) -> Result<(MailboxSender, JoinHandle<()>), ZmqError>
```

### Socket Setup Sequence (inside `create_and_spawn`)

```
1. Choose socket domain
   domain = if endpoint.is_ipv6 { Domain::IPV6 } else { Domain::IPV4 }

2. Create raw socket2 socket
   let sock = Socket2::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

3. Set socket options (before bind)
   sock.set_reuse_address(true)?;
   #[cfg(unix)]
   sock.set_reuse_port(true)?;

   if is_ipv6:
     sock.set_only_v6(false)?;   // dual-stack

   match endpoint.mode:
     Broadcast:
       sock.set_broadcast(true)?;
     Multicast { iface, group: IpAddr::V4(g) }:
       let iface_addr = resolve_iface_to_ipv4(iface)?;
       sock.set_multicast_if_v4(&iface_addr)?;
       sock.set_multicast_loop_v4(options.udp.multicast_loop)?;
       sock.set_multicast_ttl_v4(options.udp.multicast_ttl as u32)?;
     Multicast { iface, group: IpAddr::V6(g) }:
       let iface_idx = resolve_iface_to_index(iface)?;
       sock.set_multicast_if_v6(iface_idx)?;
       sock.set_multicast_loop_v6(options.udp.multicast_loop)?;
     Unicast:
       (no extra options)

4. Bind
   let bind_sa: socket2::SockAddr = endpoint.bind_addr.into();
   sock.bind(&bind_sa)?;

5. Join multicast group (after bind)
   match endpoint.mode:
     Multicast { iface, group: IpAddr::V4(g) }:
       let iface_addr = resolve_iface_to_ipv4(iface)?;
       sock.join_multicast_v4(&g, &iface_addr)?;
     Multicast { iface, group: IpAddr::V6(g) }:
       let iface_idx = resolve_iface_to_index(iface)?;
       sock.join_multicast_v6(&g, iface_idx)?;
     _ => {}

6. Convert to tokio UdpSocket
   sock.set_nonblocking(true)?;
   let std_sock = std::net::UdpSocket::from(sock);
   let udp = UdpSocket::from_std(std_sock)?;
   let udp = Arc::new(udp);

7. Resolve actual bound address
   let resolved_addr = udp.local_addr()?;
   let resolved_uri  = format!("udp://{}", resolved_addr);

8. Create actor mailbox
   let (tx, rx) = mailbox(capacity);

9. Register in CoreState
   (done in command_processor before spawning — see 05_command_processor.md)

10. Spawn actor task
    let task = tokio::spawn(actor.run());
    return Ok((tx, task))
```

### Receive Loop (`run` method)

```rust
async fn run(mut self) {
    let mut drop_guard = ActorDropGuard::new(
        self.context.clone(),
        self.handle,
        ActorType::Listener,   // closest semantic match
        Some(self.endpoint_uri.clone()),
        None,
    );

    let mut buf = vec![0u8; UDP_MAX_DATAGRAM];

    loop {
        tokio::select! {
            // --- Incoming datagram ---
            result = self.socket.recv_from(&mut buf) => {
                match result {
                    Ok((len, _src)) => {
                        let datagram = &buf[..len];
                        if datagram.is_empty() {
                            warn!(handle = self.handle, "UDP: empty datagram, skipping");
                            continue;
                        }
                        // Build Msg from raw datagram bytes
                        // (DishSocket::handle_pipe_event will decode)
                        let msg = crate::message::Msg::from_vec(datagram.to_vec());
                        let cmd = Command::PipeMessageReceived {
                            pipe_id: self.pipe_read_id,
                            msg,
                        };
                        if let Err(e) = self.socket_logic
                            .handle_pipe_event(self.pipe_read_id, cmd)
                            .await
                        {
                            error!(handle = self.handle,
                                   "UDP receive actor: handle_pipe_event error: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!(handle = self.handle, "UDP recv_from error: {}", e);
                        drop_guard.set_error(ZmqError::from(e));
                        break;
                    }
                }
            }

            // --- Control commands ---
            cmd = self.mailbox_receiver.recv() => {
                match cmd {
                    Ok(Command::Stop) | Err(_) => {
                        debug!(handle = self.handle, "UdpReceiveActor: Stop received");
                        drop_guard.waive();
                        break;
                    }
                    Ok(_other) => {
                        // Ignore unknown commands
                    }
                }
            }
        }
    }

    // Notify ISocket that the pipe has closed
    let cmd_closed = Command::PipeClosedByPeer { pipe_id: self.pipe_read_id };
    let _ = self.socket_logic
        .handle_pipe_event(self.pipe_read_id, cmd_closed)
        .await;

    info!(handle = self.handle, uri = %self.endpoint_uri, "UdpReceiveActor stopped.");
}
```

### Interface Resolution Helpers (private fns in `udp.rs`)

```rust
/// Resolve an interface name or IPv4 address string to Ipv4Addr.
/// Tries to parse as Ipv4Addr first; if that fails uses libc getifaddrs.
fn resolve_iface_to_ipv4(iface: &str) -> Result<Ipv4Addr, ZmqError>;

/// Resolve an interface name to its OS interface index for IPv6 multicast.
/// Uses libc::if_nametoindex.
fn resolve_iface_to_index(iface: &str) -> Result<u32, ZmqError>;
```

Both return `ZmqError::InvalidEndpoint` if resolution fails.

### `UdpBoundSender` for Radio `bind`

When `Radio` calls `bind("udp://0.0.0.0:5900")`, no receive loop is needed.
A `socket2` socket is created, configured with `SO_REUSEADDR` + `SO_REUSEPORT`,
and bound to the specified address. It is then wrapped in a `UdpSendConnection`
(from `connection_iface.rs`) and stored directly as a `EndpointInfo` of type
`Session` in `CoreState`. No actor task is spawned — the socket is stateless
from the actor perspective.

```rust
/// Creates a bound UDP socket for the Radio side.
/// Returns (UdpSocket, resolved_uri) ready to wrap in UdpSendConnection.
pub(crate) fn create_bound_send_socket(
    endpoint: &UdpEndpoint,
    options: &SocketOptions,
) -> Result<(UdpSocket, String), ZmqError>
```

Setup sequence is identical to `create_and_spawn` steps 1–6 above, minus the
multicast join and minus spawning a task.
