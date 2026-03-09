# 07 — DishSocket Implementation

## File

`core/src/socket/dish_socket.rs` (new file)

## Struct Definition

```rust
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::command::ZmtpCommand;
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::{JOIN, LEAVE};
use crate::socket::patterns::IncomingMessageOrchestrator;
use crate::{Blob, delegate_to_core};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Implements the DISH socket pattern (RFC 48).
///
/// DISH is the thread-safe group subscriber. It joins groups via
/// `set_option(JOIN, group_name)` and receives only messages tagged with
/// joined groups by connected RADIO sockets.
#[derive(Debug)]
pub(crate) struct DishSocket {
    core: Arc<SocketCore>,
    /// Set of joined group names (exact-match, per RFC §Group).
    joined_groups: RwLock<HashSet<Bytes>>,
    /// Fair-queue for incoming messages. QItem = Vec<Msg> with one element
    /// (the decoded message with group attached). Vec<Msg> matches the
    /// orchestrator's generic interface used by SubSocket.
    incoming_orchestrator: IncomingMessageOrchestrator<Vec<Msg>>,
    /// Maps pipe_read_id → endpoint_uri for connection tracking.
    pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl DishSocket {
    pub fn new(core: Arc<SocketCore>) -> Self {
        let orchestrator =
            IncomingMessageOrchestrator::new(core.handle, core.core_state.read().options.rcvhwm);
        Self {
            core,
            joined_groups: RwLock::new(HashSet::new()),
            incoming_orchestrator: orchestrator,
            pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
        }
    }

    fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
        self.core.core_state.read()
    }

    fn get_connection(&self, endpoint_uri: &str) -> Option<Arc<dyn ISocketConnection>> {
        self.core_state_read()
            .endpoints
            .get(endpoint_uri)
            .map(|ep| ep.connection_iface.clone())
    }

    /// Sends a JOIN or LEAVE command to a single peer connection.
    async fn send_group_command_to_peer(
        &self,
        conn: &Arc<dyn ISocketConnection>,
        uri: &str,
        is_join: bool,
        group: &[u8],
    ) {
        let cmd_msg = if is_join {
            ZmtpCommand::create_join(group)
        } else {
            ZmtpCommand::create_leave(group)
        };
        if let Err(e) = conn.send_message(cmd_msg).await {
            tracing::warn!(
                handle = self.core.handle,
                uri,
                is_join,
                group = ?String::from_utf8_lossy(group),
                error = %e,
                "DISH: failed to send group command to peer"
            );
        }
    }

    /// Broadcasts a JOIN or LEAVE command to all currently connected peers.
    async fn send_group_command_to_all(&self, is_join: bool, group: &[u8]) {
        let peer_uris: Vec<String> = self
            .pipe_read_to_endpoint_uri
            .read()
            .values()
            .cloned()
            .collect();

        for uri in peer_uris {
            if let Some(conn) = self.get_connection(&uri) {
                self.send_group_command_to_peer(&conn, &uri, is_join, group).await;
            }
        }
    }
}
```

## ISocket Method Implementations

### `core()` and `mailbox()`

Standard delegation — identical to all other socket types.

### `bind`, `connect`, `disconnect`, `unbind`

Standard delegation via `delegate_to_core!`.

### `send(_msg)`

```rust
Err(ZmqError::InvalidState("DISH sockets cannot send messages"))
```

### `recv()`

```
1. Check core.is_running() — return InvalidState if closing
2. Get rcvtimeo from core_state_read().options.rcvtimeo
3. Call incoming_orchestrator.recv_message(rcvtimeo_opt, |item: Vec<Msg>| item)
   — returns Vec<Msg> with exactly one element
4. Extract the single Msg: result.into_iter().next()
   (unwrap is safe — orchestrator always queues non-empty vecs)
5. Return Ok(msg)
```

### `send_multipart(_frames)`

```rust
Err(ZmqError::InvalidState(
    "DISH sockets do not support multipart messages (RFC 48 thread-safety requirement)"
))
```

### `recv_multipart()`

```rust
Err(ZmqError::InvalidState(
    "DISH sockets do not support multipart messages (RFC 48 thread-safety requirement)"
))
```

### `set_option(option, value)`

```rust
match option {
    JOIN | LEAVE => self.set_pattern_option(option, value).await,
    _ => delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec()),
}
```

### `get_option(option)` and `close()`

Standard delegation via `delegate_to_core!`.

### `set_pattern_option(option, value)`

```
JOIN:
  1. Validate group:
     - length 1–255
     - all bytes in range 1–255 (no NUL)
     - return InvalidArgument if invalid
  2. Insert Bytes::copy_from_slice(value) into joined_groups
  3. Call send_group_command_to_all(true, value).await
  4. Return Ok(())

LEAVE:
  1. Validate group (same as JOIN)
  2. Remove from joined_groups (no-op if not present — matches ZMQ convention)
  3. Call send_group_command_to_all(false, value).await
  4. Return Ok(())

_:
  Err(ZmqError::UnsupportedOption(option))
```

### `get_pattern_option(option)`

```rust
Err(ZmqError::UnsupportedOption(option))
```

### `process_command(_command)`

```rust
Ok(false)
```

### `handle_pipe_event(pipe_id, event)`

```
PipeMessageReceived { msg }:
  A. If msg.is_command():
       Parse with ZmtpCommand::parse(&msg).
       Ignore all commands — RADIO should not send commands to DISH,
       but be defensive and silently discard anything received.
       Log at trace level.
       Return Ok(())

  B. Data frame (msg.is_command() == false):
       1. Get frame bytes: frame = msg.data().unwrap_or(&[])
       2. If frame.is_empty(): log warning, return Ok(())
       3. group_len = frame[0] as usize
       4. If 1 + group_len > frame.len(): log protocol warning, return Ok(())
          (malformed frame — group_len exceeds available bytes)
       5. group_bytes = &frame[1 .. 1 + group_len]
       6. payload_bytes = &frame[1 + group_len ..]
       7. Check: joined_groups.read().contains(group_bytes as &[u8])
          (parking_lot RwLock — sync, no await needed)
       8. If NOT in joined_groups: silently drop, return Ok(())
       9. If matched:
          a. Build decoded_msg = Msg::from_bytes(Bytes::copy_from_slice(payload_bytes))
          b. decoded_msg.set_group(Bytes::copy_from_slice(group_bytes))
             (this cannot fail — group came from RADIO which already validated it)
          c. incoming_orchestrator.queue_item(pipe_id, vec![decoded_msg]).await?
       10. Return Ok(())

PipeClosedByPeer { .. }:
  incoming_orchestrator.clear_pipe_state(pipe_id).await
  Return Ok(())

_:
  Ok(())
```

### `pipe_attached(pipe_read_id, _pipe_write_id, _peer_identity)`

```
1. Look up endpoint_uri from core.core_state.pipe_read_id_to_endpoint_uri
2. If not found: log warning, return
3. Insert into pipe_read_to_endpoint_uri map
4. Replay existing group joins to the new peer:
   a. Collect current joined groups from joined_groups.read()
   b. Get connection via get_connection(&endpoint_uri)
   c. For each group: send_group_command_to_peer(&conn, &uri, true, &group).await
      (fire-and-forget; break on first send error and log warning)
```

This replay is essential so that a DISH socket that connects after already
having called JOIN still gets its subscription delivered to the newly connected
RADIO peer — mirroring the behaviour of SUB's `pipe_attached`.

### `update_peer_identity(pipe_read_id, identity)`

Log at trace level + ignore. DISH does not use peer identities.

### `pipe_detached(pipe_read_id)`

```
1. Remove from pipe_read_to_endpoint_uri
2. Call incoming_orchestrator.clear_pipe_state(pipe_read_id).await
```

## Wire Decode Helper (internal)

A private function `decode_radio_frame` reverses the encoding done by RADIO:

```rust
/// Decodes a RADIO wire frame into (group_bytes, payload_bytes).
/// Returns None if the frame is malformed.
fn decode_radio_frame(frame: &[u8]) -> Option<(&[u8], &[u8])> {
    if frame.is_empty() {
        return None;
    }
    let group_len = frame[0] as usize;
    if 1 + group_len > frame.len() {
        return None; // group_len claims more bytes than exist
    }
    let group = &frame[1 .. 1 + group_len];
    let payload = &frame[1 + group_len ..];
    Some((group, payload))
}
```

Used inside `handle_pipe_event` for the data frame path.

## joined_groups Lookup Note

The `joined_groups` lookup in `handle_pipe_event` uses `parking_lot::RwLock`
(synchronous), which is held only briefly for the `contains` check. No `await`
occurs while the lock is held, satisfying parking_lot's requirements and
keeping the hot receive path efficient.

The check requires `HashSet<Bytes>` to support lookup by `&[u8]`. Since
`Bytes` implements `Borrow<[u8]>` and `HashSet::contains` accepts `Q: Hash +
Eq` where `K: Borrow<Q>`, this works directly:
```rust
joined_groups.read().contains(group_bytes)  // group_bytes: &[u8]
```
