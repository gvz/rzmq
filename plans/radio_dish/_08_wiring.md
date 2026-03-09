# 08 — Wiring: Module Declarations and Factory Registration

## File 1: `core/src/socket/mod.rs`

### Add module declarations

After line 22 (`pub mod sub_socket;`), add:

```rust
pub mod radio_socket;
pub mod dish_socket;
```

No other changes to this file are needed. The `pub use options::*` already
re-exports everything from `options.rs`, so `JOIN` and `LEAVE` will be
available automatically.

---

## File 2: `core/src/socket/core/mod.rs`

### Add factory arms to `create_and_spawn`

In the `match socket_type { ... }` block (currently lines 56–65), add two
new arms after the `Pull` arm:

```rust
SocketType::Radio => Arc::new(
    crate::socket::radio_socket::RadioSocket::new(socket_core_arc.clone())
),
SocketType::Dish => Arc::new(
    crate::socket::dish_socket::DishSocket::new(socket_core_arc.clone())
),
```

The exhaustive match will produce a compile error if the new variants are
added to `SocketType` without corresponding factory arms, which is the
desired safety guarantee.

---

## File 3: `core/src/lib.rs`

No changes required. The existing re-exports cover everything:

```rust
pub use socket::types::{Socket, SocketType};  // Radio and Dish included automatically
pub use message::{Blob, Metadata, Msg, MsgFlags};  // group() and set_group() on Msg
```

`JOIN` and `LEAVE` are accessible as `rzmq::socket::options::JOIN` and
`rzmq::socket::options::LEAVE` via the `pub use options::*` in `socket/mod.rs`.

---

## Summary of All Line-Level Edits

| File | Location | Edit |
|---|---|---|
| `socket/mod.rs` | After line 22 | Add two `pub mod` declarations |
| `socket/core/mod.rs` | After line 64 (Pull arm) | Add Radio + Dish factory arms |
| `socket/types.rs` | After line 40 (Pull variant) | Add Radio + Dish variants |
| `socket/options.rs` | After UNSUBSCRIBE constant | Add JOIN + LEAVE constants |
| `message/msg.rs` | Struct + impl block | Add group field + methods |
| `protocol/zmtp/command.rs` | Enum + parse() + impl | Add Join/Leave variants + factories |
| `sessionx/protocol_handler/heartbeat.rs` | Return type + match | New DataCommandResult enum |
| `sessionx/actor.rs` | handle_incoming_from_network | Forward unknown commands |
