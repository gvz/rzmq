# UDP Transport for RADIO-DISH — Overview & Decisions

## Goal

Add a `udp` feature-gated transport to rzmq that lets `Radio` and `Dish`
sockets communicate over raw UDP datagrams (no ZMTP handshake). Supports
unicast, broadcast (`SO_BROADCAST`), and IPv4/IPv6 multicast. One-way
communication only (Radio → Dish). Compatible with libzmq's `udp://` scheme.

## Confirmed Decisions

| Question | Answer |
|---|---|
| ZMTP handshake? | **No** — raw datagrams only |
| Endpoint scheme | **`udp://`** — matches libzmq |
| Feature gate | **`udp = []`** — opt-in only, not in `default` |
| Sockets that may use `udp://` | **Radio and Dish only** |
| Reconnect logic | **No** — UDP is connectionless |
| JOIN/LEAVE ZMTP commands | **No** — Dish filters locally from datagram group prefix |
| Message size enforcement | **Yes** — return `ZmqError::MessageTooLarge` if encoded frame > 65507 bytes (IPv4 max) |
| `SO_REUSEADDR` + `SO_REUSEPORT` | **Both set** on every UDP socket via `socket2` |
| IPv6 dual-stack | **`socket.set_only_v6(false)`** on `[::]:port` binds |
| Dish bind/connect | **Both supported** — libzmq-compatible |

## Feature Flags

```toml
# core/Cargo.toml
[features]
udp       = []
full      = ["default", "ipc", "inproc", "plain", "noise_xx", "curve", "udp"]
full-linux = ["full", "io-uring"]
# default does NOT include udp
```

## Socket × Operation Matrix

| Socket | `bind(udp://...)` | `connect(udp://...)` |
|--------|-------------------|----------------------|
| Radio  | Creates a bound outgoing UDP socket (local port fixed; sends to all connected Dish peers) | Creates an outgoing UDP socket targeting the specified remote address |
| Dish   | Creates a receiving socket bound to port; joins multicast group if applicable | Creates a receiving socket bound to `0.0.0.0:0`; receives from any sender |
| Other  | `ZmqError::UnsupportedTransport` | `ZmqError::UnsupportedTransport` |

## Encoding

Pre-encoding happens in `RadioSocket::send` (existing `encode_radio_frame`):

```
[ group_len: u8 | group: [u8; group_len] | payload: [u8] ]
```

`UdpSendConnection::send_multipart` receives this already-encoded `Msg` blob
and sends the raw bytes via `UdpSocket::send_to`. The size check
(`encoded_len <= 65507`) also happens there.

On the receive side, `UdpReceiveActor` reads raw datagrams and hands the bytes
to `DishSocket::handle_pipe_event` as a `PipeMessageReceived` command, exactly
as the TCP path does. `decode_radio_frame` in `dish_socket.rs` is unchanged.

## New Files

```
core/src/transport/udp_endpoint.rs   — parse udp:// endpoint strings
core/src/transport/udp.rs            — UdpReceiveActor (Dish side)
core/tests/udp_radio_dish.rs         — integration test suite
```

## Changed Files

```
core/src/error.rs                              — add MessageTooLarge variant
core/src/transport/mod.rs                      — pub mod udp / udp_endpoint
core/src/transport/endpoint.rs                 — Endpoint::Udp variant
core/src/socket/options.rs                     — UdpSocketOptions + constants
core/src/socket/connection_iface.rs            — UdpSendConnection
core/src/socket/core/command_processor.rs      — handle Endpoint::Udp in bind/connect
core/Cargo.toml                                — udp feature, test entry
```

## Files With No Changes Needed

```
core/src/socket/radio_socket.rs   — encode_radio_frame + Distributor already correct
core/src/socket/dish_socket.rs    — decode_radio_frame + group filter already correct
```
