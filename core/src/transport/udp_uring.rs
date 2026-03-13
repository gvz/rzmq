#![cfg(all(feature = "udp", feature = "io-uring"))]

use crate::context::Context;
use crate::error::ZmqError;
use crate::socket::options::SocketOptions;
use crate::transport::udp_endpoint::{UdpEndpoint, UdpMode};

use socket2::{Domain, Protocol, Socket as Socket2, Type};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::io::RawFd;

pub(crate) fn create_bound_raw_udp_socket(
  endpoint: &UdpEndpoint,
  options: &SocketOptions,
) -> Result<(RawFd, String), ZmqError> {
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
      }
    },
    UdpMode::Unicast => {}
  }

  let bind_sa: socket2::SockAddr = endpoint.bind_addr.into();
  sock
    .bind(&bind_sa)
    .map_err(|e| ZmqError::from_io_endpoint(e, &format!("udp://{}", endpoint.bind_addr)))?;

  sock.set_nonblocking(true).map_err(ZmqError::from)?;
  let fd = sock.into_raw_fd();

  let resolved_addr = SocketAddr::new(endpoint.bind_addr.ip(), {
    use std::net::TcpListener;
    let listener = TcpListener::bind(format!("udp://{}", endpoint.bind_addr))?;
    listener.local_addr()?.port()
  });
  let resolved_uri = format!("udp://{}", resolved_addr);

  Ok((fd, resolved_uri))
}

pub(crate) fn create_connected_raw_udp_socket(
  endpoint: &UdpEndpoint,
  options: &SocketOptions,
) -> Result<RawFd, ZmqError> {
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

  let bind_addr = if endpoint.is_ipv6 {
    "[::]:0".parse::<SocketAddr>().unwrap()
  } else {
    "0.0.0.0:0".parse::<SocketAddr>().unwrap()
  };
  let bind_sa: socket2::SockAddr = bind_addr.into();
  sock.bind(&bind_sa).map_err(ZmqError::from)?;

  let dest_sa: socket2::SockAddr = endpoint.send_addr.into();
  sock.connect(&dest_sa).map_err(ZmqError::from)?;

  sock.set_nonblocking(true).map_err(ZmqError::from)?;
  let fd = sock.into_raw_fd();

  Ok(fd)
}

pub(crate) fn create_recv_raw_udp_socket(
  endpoint: &UdpEndpoint,
  options: &SocketOptions,
) -> Result<(RawFd, String), ZmqError> {
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
      }
    },
    UdpMode::Unicast => {}
  }

  let bind_sa: socket2::SockAddr = endpoint.bind_addr.into();
  sock
    .bind(&bind_sa)
    .map_err(|e| ZmqError::from_io_endpoint(e, &format!("udp://{}", endpoint.bind_addr)))?;

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

  sock.set_nonblocking(true).map_err(ZmqError::from)?;
  let fd = sock.into_raw_fd();

  let resolved_addr = SocketAddr::new(endpoint.bind_addr.ip(), {
    use std::net::UdpSocket;
    let sock = UdpSocket::bind(format!("udp://{}", endpoint.bind_addr))?;
    sock.local_addr()?.port()
  });
  let resolved_uri = format!("udp://{}", resolved_addr);

  Ok((fd, resolved_uri))
}

fn resolve_iface_to_ipv4(iface: &Option<String>) -> Result<Ipv4Addr, ZmqError> {
  Ok(Ipv4Addr::new(0, 0, 0, 0))
}

fn resolve_iface_to_index(iface: &Option<String>) -> Result<u32, ZmqError> {
  Ok(0)
}
