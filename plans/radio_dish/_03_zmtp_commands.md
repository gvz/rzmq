# 03 — ZMTP Commands: JOIN and LEAVE

## File

`core/src/protocol/zmtp/command.rs`

## Why

DISH sends JOIN/LEAVE notifications to RADIO peers over the wire using ZMTP
COMMAND frames. This is the same mechanism used by the ZMTP heartbeat
(PING/PONG), but for group membership management.

## Changes

### 1. Add command name constants

```rust
pub const ZMTP_CMD_JOIN_NAME: &[u8]  = b"JOIN";
pub const ZMTP_CMD_LEAVE_NAME: &[u8] = b"LEAVE";
```

### 2. Add variants to `ZmtpCommand`

```rust
pub(crate) enum ZmtpCommand {
  Ping(Bytes),
  Pong(Bytes),
  Ready(ZmtpReady),
  Error,
  Join(Bytes),   // NEW — carries the group name bytes
  Leave(Bytes),  // NEW — carries the group name bytes
  Unknown(Bytes),
}
```

### 3. Add parsing in `ZmtpCommand::parse()`

Insert before the final `else` / `Unknown` fallthrough:

```rust
// JOIN: body = \x04 J O I N <group_bytes>
// name length prefix is 4 (len of "JOIN"), body[0] = \x04, body[1..5] = "JOIN"
} else if body.starts_with(b"\x04JOIN") {
    // group is everything after the 5-byte name header (\x04 + "JOIN")
    let group = Bytes::copy_from_slice(&body[5..]);
    Some(ZmtpCommand::Join(group))

// LEAVE: body = \x05 L E A V E <group_bytes>
// name length prefix is 5 (len of "LEAVE"), body[0] = \x05, body[1..6] = "LEAVE"
} else if body.starts_with(b"\x05LEAVE") {
    let group = Bytes::copy_from_slice(&body[6..]);
    Some(ZmtpCommand::Leave(group))
```

### 4. Add factory methods

```rust
impl ZmtpCommand {
    // ... existing methods ...

    /// Creates a JOIN command message for the given group.
    /// The resulting Msg has MsgFlags::COMMAND set.
    pub fn create_join(group: &[u8]) -> Msg {
        // Frame body: \x04 J O I N <group_bytes>
        let mut body = Vec::with_capacity(5 + group.len());
        body.extend_from_slice(b"\x04JOIN");
        body.extend_from_slice(group);
        let mut msg = Msg::from_vec(body);
        msg.set_flags(MsgFlags::COMMAND);
        msg
    }

    /// Creates a LEAVE command message for the given group.
    /// The resulting Msg has MsgFlags::COMMAND set.
    pub fn create_leave(group: &[u8]) -> Msg {
        // Frame body: \x05 L E A V E <group_bytes>
        let mut body = Vec::with_capacity(6 + group.len());
        body.extend_from_slice(b"\x05LEAVE");
        body.extend_from_slice(group);
        let mut msg = Msg::from_vec(body);
        msg.set_flags(MsgFlags::COMMAND);
        msg
    }
}
```

## Wire Format Reference

ZMTP command frame body structure (same as PING/PONG):
```
<name_len: u8> <name: name_len bytes> <payload: remainder>
```

| Command | name_len | name  | payload          |
|---------|----------|-------|------------------|
| JOIN    | 0x04     | JOIN  | group bytes      |
| LEAVE   | 0x05     | LEAVE | group bytes      |
| PING    | 0x04     | PING  | TTL(2) + context |
| PONG    | 0x04     | PONG  | context          |

## Notes

- An empty group in a JOIN/LEAVE command is technically valid at the parse level
  (zero-length payload after the name). The receiver (RADIO's `handle_pipe_event`)
  should validate and ignore malformed groups rather than crashing.
- The `Unknown(Bytes)` variant already handles any command name not listed,
  so no fallthrough changes are needed there.
