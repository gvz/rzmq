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
                  Ok((len, _src)) => {
                      let datagram = &buf[..len];
                      if datagram.is_empty() {
                          warn!(handle = self.handle, "UDP: empty datagram, skipping");
                          continue;
                      }
                      // Build Msg from raw datagram bytes
                      let msg = crate::message::Msg::from_vec(datagram.to_vec());
                      let cmd = Command::PipeMessageReceived {
                          pipe_id: self.pipe_read_id,
                          msg,
                      };
                      match self.socket_logic
                          .handle_pipe_event(self.pipe_read_id, cmd)
                          .await
                      {
                          Ok(()) => {}
                          Err(ZmqError::Shutdown) => {
                              error!(handle = self.handle,
                                     "UDP receive actor: socket shutting down, exiting");
                              break;
                          }
                          Err(e) => {
                              warn!(handle = self.handle,
                                    "UDP receive actor: handle_pipe_event error (datagram dropped): {}", e);
                              // Non-fatal: drop the datagram and keep the loop alive.
                          }
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
/// Tries to parse as Ipv4Addr first; if that fails, walks getifaddrs
/// to find an AF_INET address for the named interface.
fn resolve_iface_to_ipv4(iface: &str) -> Result<Ipv4Addr, ZmqError> {
  use std::ffi::CString;

  if let Ok(addr) = iface.parse::<Ipv4Addr>() {
    return Ok(addr);
  }

  let c_iface = CString::new(iface)
    .map_err(|_| ZmqError::InvalidEndpoint(format!("Invalid interface name: {}", iface)))?;

  let mut ifaddrs: *mut libc::ifaddrs = std::ptr::null_mut();
  if unsafe { libc::getifaddrs(&mut ifaddrs) } != 0 {
    return Err(ZmqError::InvalidEndpoint("getifaddrs failed".to_string()));
  }

  let mut result = None;
  let mut cursor = ifaddrs;
  while !cursor.is_null() {
    let entry = unsafe { &*cursor };
    let name = unsafe { std::ffi::CStr::from_ptr(entry.ifa_name) };
    if name == c_iface.as_c_str() {
      if !entry.ifa_addr.is_null() {
        let sa = unsafe { &*entry.ifa_addr };
        if sa.sa_family == libc::AF_INET as libc::sa_family_t {
          let sin = entry.ifa_addr as *const libc::sockaddr_in;
          let bytes = unsafe { (*sin).sin_addr.s_addr }.to_ne_bytes();
          result = Some(Ipv4Addr::from(bytes));
          break;
        }
      }
    }
    cursor = entry.ifa_next;
  }

  unsafe { libc::freeifaddrs(ifaddrs) };

  result.ok_or_else(|| {
    ZmqError::InvalidEndpoint(format!(
      "Interface '{}' not found or has no IPv4 address",
      iface
    ))
  })
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
