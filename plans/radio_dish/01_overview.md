# 01 — Overview: RADIO-DISH RFC 48 Implementation

## Goal

Implement RFC 48 (RADIO-DISH) as a thread-safe alternative to PUB-SUB, using
exact-match group filtering instead of prefix-based topic filtering.

## Scope

9 source files total: 2 new socket implementations, 1 test file, 6 modified files.

## Confirmed Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Group storage on `Msg` | `group: Option<Bytes>` field directly on `Msg` | Zero-copy, sync access, minimal API change |
| RADIO filtering strategy | RADIO broadcasts to all; DISH filters on receive | Simpler RADIO; valid per RFC §RADIO |
| JOIN/LEAVE wire encoding | ZMTP COMMAND frames (`MsgFlags::COMMAND`) | Clean protocol separation; matches spec intent |
| DISH unmatched messages | Silently dropped | Mandated by RFC §DISH |

## Wire Format: Group in Data Messages (RADIO → DISH)

Messages travel as a **single frame** (multipart is forbidden per RFC for thread-safety).

Frame layout sent by RADIO:
```
[ group_len: u8 ][ group_bytes: group_len ][ payload_bytes: remainder ]
```

On DISH receive side in `handle_pipe_event`:
1. Read `group_len = frame[0]`
2. Extract `group = frame[1 .. 1+group_len]`
3. Extract `payload = frame[1+group_len ..]`
4. Check `joined_groups.contains(group)` — exact match
5. If matched: build `Msg` from payload, attach group via `msg.set_group(group)`, enqueue
6. If not matched: silently drop

## Wire Format: JOIN/LEAVE Commands (DISH → RADIO)

DISH sends ZMTP COMMAND frames to RADIO peers when joining or leaving groups:

```
JOIN frame body:  \x04 J O I N <group_bytes>
LEAVE frame body: \x05 L E A V E <group_bytes>
```

These are standard ZMTP command frames (flag byte `MsgFlags::COMMAND` set in the
ZMTP framing layer).

### Session Actor Change Required

The session actor (`sessionx/actor.rs`) currently silently ignores unknown COMMAND
frames in the data phase. JOIN/LEAVE must be forwarded to `ISocket::handle_pipe_event`
so RADIO can process them. This requires:

1. Change the return type of `process_heartbeat_command_impl` in
   `sessionx/protocol_handler/heartbeat.rs` to a richer enum.
2. Update the call site in `actor.rs` to forward unknown commands.

See `04_session_actor_change.md` for details.

## Group Validation Rules (RFC §Group)

- Length: **1–255 bytes** (zero length = "no group", represented as `None`)
- Each byte: **values 1–255** (NUL byte `\x00` is forbidden)

## File Map

| # | File | Action |
|---|---|---|
| 1 | `core/src/message/msg.rs` | Modify — add `group` field |
| 2 | `core/src/protocol/zmtp/command.rs` | Modify — add JOIN/LEAVE commands |
| 3 | `core/src/sessionx/protocol_handler/heartbeat.rs` | Modify — change return type |
| 4 | `core/src/sessionx/actor.rs` | Modify — forward unknown commands |
| 5 | `core/src/socket/types.rs` | Modify — add Radio/Dish variants |
| 6 | `core/src/socket/options.rs` | Modify — add JOIN/LEAVE constants |
| 7 | `core/src/socket/radio_socket.rs` | **New** |
| 8 | `core/src/socket/dish_socket.rs` | **New** |
| 9 | `core/src/socket/mod.rs` | Modify — declare new modules |
| 10 | `core/src/socket/core/mod.rs` | Modify — factory arms |
| 11 | `core/tests/radio_dish.rs` | **New** — integration tests |
