# UDP Transport — Wire Format & Endpoint Syntax

## Wire Format

Every UDP datagram is a self-contained frame with no framing header beyond
the group prefix already defined by RFC 48:

```
Offset  Length       Field
──────  ───────      ─────────────────────────────────────────────────
0       1 byte       group_len  (0–255; 0 = empty group, allowed by RFC)
1       group_len    group      (raw bytes; each byte must be 1–255)
1+g     remaining    payload    (application data, may be empty)
```

### Size Constraint

IPv4 UDP max payload = **65507 bytes** (65535 − 20 IP − 8 UDP headers).
IPv6 UDP max payload = **65527 bytes** (65535 − 8 IP extension − 8 UDP headers).

To keep the implementation simple and safe across both address families,
enforce the IPv4 limit (65507) for all UDP sends.

```
encoded_len = 1 + group.len() + payload.len()
if encoded_len > 65507 {
    return Err(ZmqError::MessageTooLarge(encoded_len, 65507))
}
```

This check occurs in `UdpSendConnection::send_multipart`, after the frame is
assembled but before calling `socket.send_to`.

### Encoding / Decoding Location

| Side   | Function                        | Location                  |
|--------|---------------------------------|---------------------------|
| Encode | `RadioSocket::encode_radio_frame` | `socket/radio_socket.rs` (unchanged) |
| Decode | `DishSocket::decode_radio_frame`  | `socket/dish_socket.rs`  (unchanged) |

`UdpSendConnection` does **not** re-encode. It receives the pre-encoded `Msg`
blob produced by `encode_radio_frame` and sends its bytes directly.

---

## Endpoint Syntax

Follows libzmq's `udp://` convention exactly.

### Grammar

```
udp-endpoint   = "udp://" ( multicast-ep | unicast-ep )
multicast-ep   = iface ";" group-addr ":" port
unicast-ep     = host-or-addr ":" port

iface          = interface-name          ; e.g. "eth0", "lo"
group-addr     = ipv4-mcast-addr         ; 224.0.0.0/4
               | "[" ipv6-mcast-addr "]" ; ff00::/8
host-or-addr   = ipv4-addr
               | "[" ipv6-addr "]"
               | hostname
port           = 1*DIGIT                 ; 1–65535
```

### Examples

| Mode | Endpoint string |
|------|----------------|
| IPv4 unicast | `udp://127.0.0.1:5900` |
| IPv4 broadcast | `udp://255.255.255.255:5900` |
| IPv4 multicast | `udp://lo;239.0.0.1:5900` |
| IPv6 unicast | `udp://[::1]:5900` |
| IPv6 multicast | `udp://lo;[ff02::1]:5900` |
| Bind any interface | `udp://0.0.0.0:5900` |

### Parsing Rules (implemented in `parse_udp_endpoint`)

1. Strip `udp://` prefix (caller already stripped scheme, receives address part).
2. If `;` is present in the remaining string:
   - Split at first `;` → `iface` (left) and `rest` (right).
   - Parse `rest` as `group_addr:port` (handle `[ipv6]:port` bracket notation).
   - Detect IPv4 vs IPv6 multicast from the group address.
   - Set `mode = UdpMode::Multicast { iface, group }`.
   - `bind_addr` = `0.0.0.0:port` (IPv4) or `[::]:port` (IPv6).
   - `send_addr` = `group:port`.
3. Otherwise, parse as `addr:port`:
   - Handle `[ipv6]:port` bracket notation.
   - If `addr == 255.255.255.255` or is a subnet broadcast → `mode = Broadcast`.
   - Otherwise → `mode = Unicast`.
   - `bind_addr` = the parsed `SocketAddr` (for Dish bind) or `0.0.0.0:0` (for Radio connect).
   - `send_addr` = the parsed `SocketAddr`.
4. Set `is_ipv6` from address family of `send_addr`.

### `UdpMode` Enum

```rust
pub(crate) enum UdpMode {
    Unicast,
    Broadcast,
    Multicast {
        iface: String,       // interface name, e.g. "lo", "eth0"
        group: std::net::IpAddr,
    },
}
```

### `UdpEndpoint` Struct

```rust
pub(crate) struct UdpEndpoint {
    /// Address to bind the local socket to.
    /// For Dish bind:  the user-specified addr:port (or 0.0.0.0:port for mcast).
    /// For Radio connect: 0.0.0.0:0 (OS picks ephemeral port).
    pub bind_addr: std::net::SocketAddr,

    /// Destination address for outgoing datagrams (Radio side).
    /// For unicast/broadcast: the peer address.
    /// For multicast: the multicast group address.
    pub send_addr: std::net::SocketAddr,

    pub mode: UdpMode,
    pub is_ipv6: bool,
    pub original_uri: String,
}
```

---

## Socket Option Behavior per Mode

| Mode | `SO_REUSEADDR` | `SO_REUSEPORT` | `SO_BROADCAST` | Multicast join |
|------|---------------|---------------|----------------|----------------|
| Unicast | ✓ | ✓ | — | — |
| Broadcast | ✓ | ✓ | ✓ | — |
| IPv4 Multicast | ✓ | ✓ | — | `IP_ADD_MEMBERSHIP` |
| IPv6 Multicast | ✓ | ✓ | — | `IPV6_ADD_MEMBERSHIP` |

`SO_REUSEPORT` is set via `socket2::Socket::set_reuse_port(true)` which is
`#[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]`.

Dual-stack: when binding `[::]:port`, call `socket.set_only_v6(false)` so
both IPv4-mapped and native IPv6 traffic is received on the same socket.

---

## Multicast Interface Resolution

For `IP_MULTICAST_IF` and `IP_ADD_MEMBERSHIP`, the interface name must be
converted to an `Ipv4Addr` (IPv4) or interface index (IPv6):

```rust
// IPv4: resolve interface name to address
fn iface_name_to_ipv4(iface: &str) -> Result<Ipv4Addr, ZmqError> {
    // iterate getifaddrs or parse as direct IP address
    // fallback: Ipv4Addr::UNSPECIFIED (OS picks)
}

// IPv6: resolve interface name to index
fn iface_name_to_index(iface: &str) -> Result<u32, ZmqError> {
    // nix::net::if_nametoindex or libc::if_nametoindex
    // returns ZmqError::InvalidEndpoint if name not found
}
```

Use `libc::if_nametoindex` (already a transitive dep via `socket2`) for IPv6.
For IPv4 use `socket2`'s `SockRef` or scan `std::net::InterfaceAddresses`
when available, otherwise parse the iface field as an IP address directly
(libzmq also accepts an IP string in place of an interface name for IPv4).
