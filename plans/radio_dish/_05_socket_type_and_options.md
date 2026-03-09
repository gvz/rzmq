# 05 — SocketType Variants and Option Constants

## File 1: `core/src/socket/types.rs`

### Add `Radio` and `Dish` to the `SocketType` enum

Insert after the `Pull` variant (line 40):

```rust
/// **RADIO:** Broadcasts messages to all connected DISH sockets.
///
/// The thread-safe alternative to PUB. Every message sent by a RADIO socket
/// must have a group attached (via `Msg::set_group`). The group is used by
/// DISH sockets to filter messages they receive.
///
/// - RADIO sockets can only **send** messages; `recv()` returns an error.
/// - Multipart messages are **not** supported (thread-safety requirement).
/// - Messages are broadcast to all connected DISH peers; DISH-side filtering
///   determines which messages are delivered to the application.
Radio,

/// **DISH:** Receives messages from RADIO sockets for joined groups.
///
/// The thread-safe alternative to SUB. A DISH socket must join one or more
/// groups using `set_option(JOIN, group_name)` to receive messages.
///
/// - DISH sockets can only **receive** messages; `send()` returns an error.
/// - Multipart messages are **not** supported (thread-safety requirement).
/// - Each received message has its group accessible via `Msg::group()`.
/// - Messages for groups that have not been joined are silently discarded.
Dish,
```

---

## File 2: `core/src/socket/options.rs`

### Add JOIN and LEAVE option constants

Insert after the existing `SUBSCRIBE`/`UNSUBSCRIBE` constants:

```rust
/// DISH socket option: join a group.
/// Value is the group name as raw bytes (1–255 bytes; bytes must be 1–255).
/// Joining a group causes the DISH socket to receive messages tagged with
/// that group by a connected RADIO socket.
/// Matches libzmq's ZMQ_JOIN = 74.
pub const JOIN: i32 = 74;

/// DISH socket option: leave a group.
/// Value is the group name as raw bytes.
/// Leaving a group stops delivery of messages tagged with that group.
/// Matches libzmq's ZMQ_LEAVE = 75.
pub const LEAVE: i32 = 75;
```

### Re-export via `pub use options::*`

`core/src/socket/mod.rs` already has `pub use options::*;`, so `JOIN` and
`LEAVE` will be automatically visible as `rzmq::socket::options::JOIN` and
`rzmq::socket::options::LEAVE` without any further changes.

---

## Notes

- The libzmq option ID values 74 (`ZMQ_JOIN`) and 75 (`ZMQ_LEAVE`) are used
  for wire-level compatibility with libzmq-based peers.
- No existing option IDs conflict with 74 or 75 in the current codebase.
- `SocketType` is re-exported from `core/src/lib.rs` as
  `pub use socket::types::{Socket, SocketType}`, so the new variants are
  automatically available at the crate root without changes to `lib.rs`.
