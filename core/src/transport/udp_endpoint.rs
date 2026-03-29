use crate::error::ZmqError;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

#[derive(Debug, Clone)]
pub(crate) enum UdpMode {
  Unicast,
  Broadcast,
  Multicast { iface: String, group: IpAddr },
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
pub(crate) fn parse_udp_endpoint(
  address_part: &str,
  original_uri: &str,
) -> Result<UdpEndpoint, ZmqError> {
  if address_part.is_empty() {
    return Err(ZmqError::InvalidEndpoint(format!(
      "UDP endpoint cannot be empty: {}",
      original_uri
    )));
  }

  // Step 1: Check for multicast (';' present)
  if let Some(semicolon_pos) = address_part.find(';') {
    let iface_str = &address_part[..semicolon_pos];
    let rest = &address_part[semicolon_pos + 1..];

    if iface_str.is_empty() || rest.is_empty() {
      return Err(ZmqError::InvalidEndpoint(format!(
        "Invalid multicast format in UDP endpoint: {}",
        original_uri
      )));
    }

    // Parse group_and_port from rest
    let (group, port, is_ipv6) = if rest.starts_with('[') {
      // IPv6 bracket notation: [ipv6]:port
      let close_bracket = rest.find(']').ok_or_else(|| {
        ZmqError::InvalidEndpoint(format!(
          "Missing closing bracket in IPv6 multicast address: {}",
          original_uri
        ))
      })?;

      let ipv6_str = &rest[1..close_bracket];
      let after_bracket = &rest[close_bracket + 1..];

      if !after_bracket.starts_with(':') {
        return Err(ZmqError::InvalidEndpoint(format!(
          "Missing port after IPv6 address: {}",
          original_uri
        )));
      }

      let port_str = &after_bracket[1..];
      let port: u16 = port_str.parse().map_err(|_| {
        ZmqError::InvalidEndpoint(format!("Invalid port in UDP endpoint: {}", original_uri))
      })?;

      let group_ipv6: Ipv6Addr = ipv6_str.parse().map_err(|_| {
        ZmqError::InvalidEndpoint(format!("Invalid IPv6 multicast address: {}", original_uri))
      })?;

      // Verify it's a multicast address (ff00::/8)
      if !group_ipv6.is_multicast() {
        return Err(ZmqError::InvalidEndpoint(format!(
          "IPv6 address is not in multicast range (ff00::/8): {}",
          original_uri
        )));
      }

      (IpAddr::V6(group_ipv6), port, true)
    } else {
      // IPv4: group:port
      let colon_pos = rest.rfind(':').ok_or_else(|| {
        ZmqError::InvalidEndpoint(format!("Missing port in UDP endpoint: {}", original_uri))
      })?;

      let ipv4_str = &rest[..colon_pos];
      let port_str = &rest[colon_pos + 1..];

      let port: u16 = port_str.parse().map_err(|_| {
        ZmqError::InvalidEndpoint(format!("Invalid port in UDP endpoint: {}", original_uri))
      })?;

      let group_ipv4: Ipv4Addr = ipv4_str.parse().map_err(|_| {
        ZmqError::InvalidEndpoint(format!("Invalid IPv4 multicast address: {}", original_uri))
      })?;

      // Verify it's a multicast address (224.0.0.0/4)
      if !group_ipv4.is_multicast() {
        return Err(ZmqError::InvalidEndpoint(format!(
          "IPv4 address is not in multicast range (224.0.0.0/4): {}",
          original_uri
        )));
      }

      (IpAddr::V4(group_ipv4), port, false)
    };

    if port == 0 {
      return Err(ZmqError::InvalidEndpoint(format!(
        "Port cannot be 0 in UDP endpoint: {}",
        original_uri
      )));
    }

    let bind_addr = if is_ipv6 {
      SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), port)
    } else {
      SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)
    };

    let send_addr = SocketAddr::new(group, port);

    Ok(UdpEndpoint {
      bind_addr,
      send_addr,
      mode: UdpMode::Multicast {
        iface: iface_str.to_string(),
        group,
      },
      is_ipv6,
      original_uri: original_uri.to_string(),
    })
  } else {
    // Step 2: No ';' → unicast or broadcast
    let (addr, port, is_ipv6) = if address_part.starts_with('[') {
      // IPv6 bracket notation: [ipv6]:port
      let close_bracket = address_part.find(']').ok_or_else(|| {
        ZmqError::InvalidEndpoint(format!(
          "Missing closing bracket in IPv6 address: {}",
          original_uri
        ))
      })?;

      let ipv6_str = &address_part[1..close_bracket];
      let after_bracket = &address_part[close_bracket + 1..];

      if !after_bracket.starts_with(':') {
        return Err(ZmqError::InvalidEndpoint(format!(
          "Missing port after IPv6 address: {}",
          original_uri
        )));
      }

      let port_str = &after_bracket[1..];
      let port: u16 = port_str.parse().map_err(|_| {
        ZmqError::InvalidEndpoint(format!("Invalid port in UDP endpoint: {}", original_uri))
      })?;

      let ipv6: Ipv6Addr = ipv6_str.parse().map_err(|_| {
        ZmqError::InvalidEndpoint(format!("Invalid IPv6 address: {}", original_uri))
      })?;

      (IpAddr::V6(ipv6), port, true)
    } else {
      // IPv4 or hostname: addr:port
      let colon_pos = address_part.rfind(':').ok_or_else(|| {
        ZmqError::InvalidEndpoint(format!("Missing port in UDP endpoint: {}", original_uri))
      })?;

      let host_str = &address_part[..colon_pos];
      let port_str = &address_part[colon_pos + 1..];

      let port: u16 = port_str.parse().map_err(|_| {
        ZmqError::InvalidEndpoint(format!("Invalid port in UDP endpoint: {}", original_uri))
      })?;

      // Try parsing as IPv4 first
      if let Ok(ipv4) = host_str.parse::<Ipv4Addr>() {
        (IpAddr::V4(ipv4), port, false)
      } else {
        // Try hostname resolution
        let socket_addrs_str = format!("{}:{}", host_str, port);
        let mut addrs =
          std::net::ToSocketAddrs::to_socket_addrs(&socket_addrs_str).map_err(|e| {
            ZmqError::DnsResolutionFailed(format!(
              "Failed to resolve hostname '{}': {}",
              host_str, e
            ))
          })?;

        let resolved = addrs.next().ok_or_else(|| {
          ZmqError::DnsResolutionFailed(format!("No addresses found for hostname '{}'", host_str))
        })?;

        let is_v6 = resolved.is_ipv6();
        (resolved.ip(), port, is_v6)
      }
    };

    if port == 0 {
      return Err(ZmqError::InvalidEndpoint(format!(
        "Port cannot be 0 in UDP endpoint: {}",
        original_uri
      )));
    }

    let mode = if matches!(addr, IpAddr::V4(ipv4) if ipv4.is_broadcast()) {
      UdpMode::Broadcast
    } else {
      UdpMode::Unicast
    };

    let socket_addr = SocketAddr::new(addr, port);

    Ok(UdpEndpoint {
      bind_addr: socket_addr,
      send_addr: socket_addr,
      mode,
      is_ipv6,
      original_uri: original_uri.to_string(),
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_unicast_ipv4() {
    let ep = parse_udp_endpoint("127.0.0.1:5900", "udp://127.0.0.1:5900").unwrap();
    assert!(matches!(ep.mode, UdpMode::Unicast));
    assert_eq!(ep.bind_addr.port(), 5900);
    assert_eq!(ep.send_addr.port(), 5900);
    assert!(!ep.is_ipv6);
  }

  #[test]
  fn test_parse_unicast_ipv6() {
    let ep = parse_udp_endpoint("[::1]:5900", "udp://[::1]:5900").unwrap();
    assert!(matches!(ep.mode, UdpMode::Unicast));
    assert_eq!(ep.bind_addr.port(), 5900);
    assert_eq!(ep.send_addr.port(), 5900);
    assert!(ep.is_ipv6);
  }

  #[test]
  fn test_parse_broadcast() {
    let ep = parse_udp_endpoint("255.255.255.255:5900", "udp://255.255.255.255:5900").unwrap();
    assert!(matches!(ep.mode, UdpMode::Broadcast));
    assert_eq!(ep.bind_addr.port(), 5900);
    assert!(!ep.is_ipv6);
  }

  #[test]
  fn test_parse_ipv4_multicast() {
    let ep = parse_udp_endpoint("lo;239.0.0.1:5900", "udp://lo;239.0.0.1:5900").unwrap();
    match ep.mode {
      UdpMode::Multicast {
        ref iface,
        ref group,
      } => {
        assert_eq!(iface, "lo");
        assert!(matches!(group, IpAddr::V4(ipv4) if ipv4.is_multicast()));
      }
      _ => panic!("Expected multicast mode"),
    }
    assert_eq!(ep.send_addr.port(), 5900);
    assert_eq!(ep.bind_addr.port(), 5900);
    assert!(!ep.is_ipv6);
  }

  #[test]
  fn test_parse_ipv6_multicast() {
    let ep = parse_udp_endpoint("lo;[ff02::1]:5900", "udp://lo;[ff02::1]:5900").unwrap();
    match ep.mode {
      UdpMode::Multicast {
        ref iface,
        ref group,
      } => {
        assert_eq!(iface, "lo");
        assert!(matches!(group, IpAddr::V6(ipv6) if ipv6.is_multicast()));
      }
      _ => panic!("Expected multicast mode"),
    }
    assert_eq!(ep.send_addr.port(), 5900);
    assert!(ep.is_ipv6);
  }

  #[test]
  fn test_reject_empty_address() {
    let result = parse_udp_endpoint("", "udp://");
    assert!(result.is_err());
  }

  #[test]
  fn test_reject_missing_port() {
    let result = parse_udp_endpoint("127.0.0.1", "udp://127.0.0.1");
    assert!(result.is_err());
  }

  #[test]
  fn test_reject_zero_port() {
    let result = parse_udp_endpoint("127.0.0.1:0", "udp://127.0.0.1:0");
    assert!(result.is_err());
  }

  #[test]
  fn test_reject_non_multicast_in_multicast_format() {
    // 192.168.1.1 is not in the multicast range
    let result = parse_udp_endpoint("lo;192.168.1.1:5900", "udp://lo;192.168.1.1:5900");
    assert!(result.is_err());
  }
}
