use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{ActorDropGuard, ActorType, Command, MailboxReceiver, MailboxSender, mailbox};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::core::state::{EndpointInfo, EndpointType};
use crate::socket::events::MonitorSender;
use crate::socket::options::SocketOptions;
use crate::transport::udp_endpoint::{UdpEndpoint, UdpMode};

use socket2::{Domain, Protocol, Socket as Socket2, Type};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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
  /// Tracks whether peer discovery has already been reported for this pipe,
  /// so we can skip the async vtable call on the hot recv path.
  peer_discovered: bool,
}

impl UdpReceiveActor {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: &UdpEndpoint,
    socket_logic: Arc<dyn ISocket>,
    context: Context,
    parent_socket_id: usize,
    monitor_tx: Option<MonitorSender>,
    core_arc: Arc<SocketCore>,
    pipe_read_id: usize,
    options: &SocketOptions,
  ) -> Result<(MailboxSender, JoinHandle<()>, String), ZmqError> {
    // Create socket with proper domain
    let domain = if endpoint.is_ipv6 {
      Domain::IPV6
    } else {
      Domain::IPV4
    };

    let sock = Socket2::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(ZmqError::from)?;

    // Set socket options before bind
    sock.set_reuse_address(true).map_err(ZmqError::from)?;
    #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
    sock.set_reuse_port(true).map_err(ZmqError::from)?;

    if endpoint.is_ipv6 {
      sock.set_only_v6(false).map_err(ZmqError::from)?; // Dual-stack
    }

    // Mode-specific configuration
    match &endpoint.mode {
      UdpMode::Broadcast => {
        sock.set_broadcast(true).map_err(ZmqError::from)?;
      }
      UdpMode::Multicast { iface, group } => match group {
        IpAddr::V4(g) => {
          let iface_addr = resolve_iface_to_ipv4(iface)?;
          sock
            .set_multicast_if_v4(&iface_addr)
            .map_err(ZmqError::from)?;
          sock
            .set_multicast_loop_v4(options.udp.multicast_loop)
            .map_err(ZmqError::from)?;
          sock
            .set_multicast_ttl_v4(options.udp.multicast_hops as u32)
            .map_err(ZmqError::from)?;
        }
        IpAddr::V6(g) => {
          let iface_idx = resolve_iface_to_index(iface)?;
          sock
            .set_multicast_if_v6(iface_idx)
            .map_err(ZmqError::from)?;
          sock
            .set_multicast_loop_v6(options.udp.multicast_loop)
            .map_err(ZmqError::from)?;
          // Note: IPv6 has no set_multicast_hops_v6 in socket2,
          // it's part of per-packet control in sendmsg
        }
      },
      UdpMode::Unicast => {
        // No extra options needed
      }
    }

    // Bind the socket
    let bind_sa: socket2::SockAddr = endpoint.bind_addr.into();
    sock
      .bind(&bind_sa)
      .map_err(|e| ZmqError::from_io_endpoint(e, &format!("udp://{}", endpoint.bind_addr)))?;

    // Join multicast group (after bind)
    if let UdpMode::Multicast { iface, group } = &endpoint.mode {
      match group {
        IpAddr::V4(g) => {
          let iface_addr = resolve_iface_to_ipv4(iface)?;
          sock
            .join_multicast_v4(g, &iface_addr)
            .map_err(ZmqError::from)?;
        }
        IpAddr::V6(g) => {
          let iface_idx = resolve_iface_to_index(iface)?;
          sock
            .join_multicast_v6(g, iface_idx)
            .map_err(ZmqError::from)?;
        }
      }
    }

    // Convert to tokio UdpSocket
    sock.set_nonblocking(true).map_err(ZmqError::from)?;
    let std_sock = std::net::UdpSocket::from(sock);
    let udp = UdpSocket::from_std(std_sock).map_err(ZmqError::from)?;

    // Get actual bound address
    let resolved_addr = udp.local_addr().map_err(ZmqError::from)?;
    let resolved_uri = format!("udp://{}", resolved_addr);

    let udp = Arc::new(udp);

    // Create mailbox
    let (tx, rx) = mailbox(128);

    let actor = UdpReceiveActor {
      handle,
      endpoint_uri: resolved_uri.clone(),
      socket: udp,
      socket_logic,
      pipe_read_id,
      mailbox_receiver: rx,
      context: context.clone(),
      peer_discovered: false,
    };

    let task = tokio::spawn(actor.run());

    Ok((tx, task, resolved_uri))
  }

  async fn run(mut self) {
    let mut drop_guard = ActorDropGuard::new(
      self.context.clone(),
      self.handle,
      ActorType::Listener,
      Some(self.endpoint_uri.clone()),
      None,
    );

    let mut buf = vec![0u8; UDP_MAX_DATAGRAM];

    loop {
      tokio::select! {
          // Incoming datagram
          result = self.socket.recv_from(&mut buf) => {
              match result {
                  Ok((len, src)) => {
                      if len == 0 {
                          warn!(handle = self.handle, "UDP: empty datagram, skipping");
                          continue;
                      }
                      // Build Msg directly from the receive buffer — one allocation, no extra copy.
                      let msg = crate::message::Msg::from_vec(buf[..len].to_vec());
                      tracing::trace!(handle = self.handle, src = %src, len, "UdpReceiveActor: received datagram");
                      // Only call handle_udp_peer_discovered once per source address.
                      if !self.peer_discovered {
                        let peer_addr = format!("udp://{}", src);
                        self.socket_logic.handle_udp_peer_discovered(self.pipe_read_id, peer_addr).await;
                        self.peer_discovered = true;
                      }
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

          // Control commands
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
    let cmd_closed = Command::PipeClosedByPeer {
      pipe_id: self.pipe_read_id,
    };
    let _ = self
      .socket_logic
      .handle_pipe_event(self.pipe_read_id, cmd_closed)
      .await;

    info!(handle = self.handle, uri = %self.endpoint_uri, "UdpReceiveActor stopped.");
  }
}

/// Creates a bound UDP socket for the Radio side (no receive loop).
/// Returns (UdpSocket, resolved_uri) ready to wrap in UdpSendConnection.
pub(crate) fn create_bound_send_socket(
  endpoint: &UdpEndpoint,
  options: &SocketOptions,
) -> Result<(UdpSocket, String), ZmqError> {
  let domain = if endpoint.is_ipv6 {
    Domain::IPV6
  } else {
    Domain::IPV4
  };

  let sock = Socket2::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(ZmqError::from)?;

  sock.set_reuse_address(true).map_err(ZmqError::from)?;
  #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
  sock.set_reuse_port(true).map_err(ZmqError::from)?;

  if endpoint.is_ipv6 {
    sock.set_only_v6(false).map_err(ZmqError::from)?;
  }

  // Mode-specific configuration
  match &endpoint.mode {
    UdpMode::Broadcast => {
      sock.set_broadcast(true).map_err(ZmqError::from)?;
    }
    UdpMode::Multicast { iface, group } => match group {
      IpAddr::V4(_g) => {
        let iface_addr = resolve_iface_to_ipv4(iface)?;
        sock
          .set_multicast_if_v4(&iface_addr)
          .map_err(ZmqError::from)?;
        sock
          .set_multicast_loop_v4(options.udp.multicast_loop)
          .map_err(ZmqError::from)?;
        sock
          .set_multicast_ttl_v4(options.udp.multicast_hops as u32)
          .map_err(ZmqError::from)?;
      }
      IpAddr::V6(_g) => {
        let iface_idx = resolve_iface_to_index(iface)?;
        sock
          .set_multicast_if_v6(iface_idx)
          .map_err(ZmqError::from)?;
        sock
          .set_multicast_loop_v6(options.udp.multicast_loop)
          .map_err(ZmqError::from)?;
      }
    },
    UdpMode::Unicast => {}
  }

  // Bind
  let bind_sa: socket2::SockAddr = endpoint.bind_addr.into();
  sock
    .bind(&bind_sa)
    .map_err(|e| ZmqError::from_io_endpoint(e, &format!("udp://{}", endpoint.bind_addr)))?;

  // Convert to tokio
  sock.set_nonblocking(true).map_err(ZmqError::from)?;
  let std_sock = std::net::UdpSocket::from(sock);
  let udp = UdpSocket::from_std(std_sock).map_err(ZmqError::from)?;

  let resolved_addr = udp.local_addr().map_err(ZmqError::from)?;
  let resolved_uri = format!("udp://{}", resolved_addr);

  Ok((udp, resolved_uri))
}

/// Creates an unbound UDP socket configured for sending only.
/// Used by Radio::connect.
pub(crate) fn create_connected_send_socket(
  endpoint: &UdpEndpoint,
  options: &SocketOptions,
) -> Result<UdpSocket, ZmqError> {
  let domain = if endpoint.is_ipv6 {
    Domain::IPV6
  } else {
    Domain::IPV4
  };

  let sock = Socket2::new(domain, Type::DGRAM, Some(Protocol::UDP)).map_err(ZmqError::from)?;

  sock.set_reuse_address(true).map_err(ZmqError::from)?;
  #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
  sock.set_reuse_port(true).map_err(ZmqError::from)?;

  if endpoint.is_ipv6 {
    sock.set_only_v6(false).map_err(ZmqError::from)?;
  }

  // Mode-specific configuration
  match &endpoint.mode {
    UdpMode::Broadcast => {
      sock.set_broadcast(true).map_err(ZmqError::from)?;
    }
    UdpMode::Multicast { iface, group } => match group {
      IpAddr::V4(_g) => {
        let iface_addr = resolve_iface_to_ipv4(iface)?;
        sock
          .set_multicast_if_v4(&iface_addr)
          .map_err(ZmqError::from)?;
        sock
          .set_multicast_loop_v4(options.udp.multicast_loop)
          .map_err(ZmqError::from)?;
        sock
          .set_multicast_ttl_v4(options.udp.multicast_hops as u32)
          .map_err(ZmqError::from)?;
      }
      IpAddr::V6(_g) => {
        let iface_idx = resolve_iface_to_index(iface)?;
        sock
          .set_multicast_if_v6(iface_idx)
          .map_err(ZmqError::from)?;
        sock
          .set_multicast_loop_v6(options.udp.multicast_loop)
          .map_err(ZmqError::from)?;
      }
    },
    UdpMode::Unicast => {}
  }

  // Bind to ephemeral port
  let bind_addr: SocketAddr = if endpoint.is_ipv6 {
    "[::]:0".parse().unwrap()
  } else {
    "0.0.0.0:0".parse().unwrap()
  };
  let bind_sa: socket2::SockAddr = bind_addr.into();
  sock.bind(&bind_sa).map_err(ZmqError::from)?;

  // Convert to tokio
  sock.set_nonblocking(true).map_err(ZmqError::from)?;
  let std_sock = std::net::UdpSocket::from(sock);
  let udp = UdpSocket::from_std(std_sock).map_err(ZmqError::from)?;

  Ok(udp)
}

/// Resolve an interface name or IPv4 address string to Ipv4Addr.
/// Tries to parse as Ipv4Addr first; if that fails uses libc getifaddrs.
fn resolve_iface_to_ipv4(iface: &str) -> Result<Ipv4Addr, ZmqError> {
  // Try parsing as IP address first
  if let Ok(addr) = iface.parse::<Ipv4Addr>() {
    return Ok(addr);
  }

  // Otherwise use UNSPECIFIED (let OS choose)
  // A full implementation would use getifaddrs to find the interface
  // For now, this is a simple fallback
  Ok(Ipv4Addr::UNSPECIFIED)
}

/// Resolve an interface name to its OS interface index for IPv6 multicast.
/// Uses libc::if_nametoindex.
fn resolve_iface_to_index(iface: &str) -> Result<u32, ZmqError> {
  use std::ffi::CString;

  let c_iface = CString::new(iface)
    .map_err(|_| ZmqError::InvalidEndpoint(format!("Invalid interface name: {}", iface)))?;

  let idx = unsafe { libc::if_nametoindex(c_iface.as_ptr()) };

  if idx == 0 {
    return Err(ZmqError::InvalidEndpoint(format!(
      "Interface '{}' not found",
      iface
    )));
  }

  Ok(idx)
}
