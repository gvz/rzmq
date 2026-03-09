# 06 — RadioSocket Implementation

## File

`core/src/socket/radio_socket.rs` (new file)

## Struct Definition

```rust
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::command::ZmtpCommand;
use crate::runtime::{Command, MailboxSender};
use crate::socket::core::SocketCore;
use crate::socket::patterns::Distributor;
use crate::socket::ISocket;
use crate::{Blob, MsgFlags, delegate_to_core};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Implements the RADIO socket pattern (RFC 48).
///
/// RADIO is the thread-safe broadcast sender. Every message must have a group
/// attached. Messages are sent to all connected DISH peers; DISH-side filtering
/// determines delivery.
#[derive(Debug)]
pub(crate) struct RadioSocket {
    core: Arc<SocketCore>,
    distributor: Distributor,
    pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl RadioSocket {
    pub fn new(core: Arc<SocketCore>) -> Self {
        Self {
            core,
            distributor: Distributor::new(),
            pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
        }
    }
}
```

## ISocket Method Implementations

### `core()` and `mailbox()`

Standard delegation — identical to `PubSocket`.

### `bind`, `connect`, `disconnect`, `unbind`

Standard delegation via `delegate_to_core!` macro — identical to all other
socket types.

### `send(msg: Msg)`

```
1. Check core.is_running() — return InvalidState if closing
2. Validate msg.group() is Some(_) — return InvalidState("Message has no group") if None
3. Extract group bytes (already validated by set_group, but defensively check len 1–255)
4. Build wire frame:
     group_len = group.len() as u8
     wire_frame = [group_len] + group + payload
   Use Bytes::copy_from_slice or a Vec for the encoding.
5. Construct a new Msg from the wire_frame bytes (MsgFlags empty — single frame, no MORE)
6. Call distributor.send_to_all(&wire_msg, core.handle, &core.core_state)
7. On Err(failed_uris):
     - For each (uri, ZmqError::ConnectionClosed | ZmqError::Internal): remove from distributor
     - ResourceLimitReached/Timeout: already handled internally by distributor (silent drop)
8. Always return Ok(()) — RADIO SHALL NOT block, per RFC
```

Key implementation note: `Distributor::send_to_all` already silently drops on
`ResourceLimitReached` and `Timeout` (per RFC "SHALL silently drop the message
if the queue for a dish is full"), so no additional handling is needed for HWM.

### `recv()`

```rust
Err(ZmqError::InvalidState("RADIO sockets cannot receive messages"))
```

### `send_multipart()`

```rust
Err(ZmqError::InvalidState(
    "RADIO sockets do not support multipart messages (RFC 48 thread-safety requirement)"
))
```

### `recv_multipart()`

```rust
Err(ZmqError::UnsupportedFeature("RADIO sockets cannot receive messages"))
```

### `set_option(option, value)`

```rust
// RADIO has no pattern-specific options; delegate everything to core
delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
```

### `get_option(option)` and `close()`

Standard delegation via `delegate_to_core!`.

### `set_pattern_option(option, _value)`

```rust
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

```rust
match event {
    Command::PipeMessageReceived { msg, .. } => {
        if msg.is_command() {
            // Parse JOIN/LEAVE from DISH peers.
            // With broadcast-all strategy, RADIO doesn't need to track membership,
            // so just log and discard.
            match ZmtpCommand::parse(&msg) {
                Some(ZmtpCommand::Join(group)) => {
                    tracing::debug!(
                        handle = self.core.handle,
                        pipe_id,
                        group = ?String::from_utf8_lossy(&group),
                        "RADIO received JOIN command (broadcast mode, ignoring)"
                    );
                }
                Some(ZmtpCommand::Leave(group)) => {
                    tracing::debug!(
                        handle = self.core.handle,
                        pipe_id,
                        group = ?String::from_utf8_lossy(&group),
                        "RADIO received LEAVE command (broadcast mode, ignoring)"
                    );
                }
                _ => {
                    tracing::trace!(
                        handle = self.core.handle,
                        pipe_id,
                        "RADIO received unknown command from DISH, ignoring"
                    );
                }
            }
        } else {
            // Per RFC: "SHALL silently discard any messages that dishes send it"
            tracing::trace!(
                handle = self.core.handle,
                pipe_id,
                "RADIO discarding data message received from DISH"
            );
        }
        Ok(())
    }
    Command::PipeClosedByPeer { .. } => {
        // Cleanup is handled by pipe_detached; nothing to do here.
        Ok(())
    }
    _ => Ok(()),
}
```

### `pipe_attached(pipe_read_id, _pipe_write_id, _peer_identity)`

```
1. Read endpoint_uri from core.core_state.pipe_read_id_to_endpoint_uri
2. If found:
   a. Insert into pipe_read_to_endpoint_uri map
   b. Call distributor.add_peer_uri(endpoint_uri)
3. If not found: log warning
```

(Identical pattern to `PubSocket::pipe_attached`.)

### `update_peer_identity(pipe_read_id, identity)`

Log at trace level + ignore. RADIO does not use peer identities.

### `pipe_detached(pipe_read_id)`

```
1. Remove from pipe_read_to_endpoint_uri — get the removed URI
2. If URI was found: call distributor.remove_peer_uri(&uri)
3. If not found: log warning
```

(Identical pattern to `PubSocket::pipe_detached`.)

## Wire Encoding Helper (internal)

A private function `encode_radio_frame(group: &[u8], payload: Option<&[u8]>) -> Msg`
builds the combined wire frame:

```rust
fn encode_radio_frame(group: &[u8], payload: Option<&[u8]>) -> Msg {
    let payload = payload.unwrap_or(&[]);
    let mut frame = Vec::with_capacity(1 + group.len() + payload.len());
    frame.push(group.len() as u8);
    frame.extend_from_slice(group);
    frame.extend_from_slice(payload);
    Msg::from_vec(frame)
}
```

This is called inside `send()` after group validation.
