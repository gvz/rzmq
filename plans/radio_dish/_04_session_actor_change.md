# 04 — Session Actor Change: Forward Unknown Commands to ISocket

## Problem

The session actor (`core/src/sessionx/actor.rs`, line 543) currently handles
incoming COMMAND frames from the network like this:

```rust
if msg.is_command() {
    match self.zmtp_handler.process_incoming_data_command_frame(&msg) {
        Ok(Some(pong_reply)) => { /* send pong */ }
        Ok(None) => { /* PONG received, or unknown — silently dropped */ }
        Err(e) => self.set_fatal_error(e).await,
    }
}
```

`process_incoming_data_command_frame` calls `process_heartbeat_command_impl` in
`heartbeat.rs`, which returns `Ok(None)` for any command it doesn't recognise
(line 121-122):

```rust
Some(other) => {
    Ok(None) // Ignore other commands  <-- JOIN/LEAVE silently dropped here
}
```

This means JOIN and LEAVE commands sent by DISH never reach RADIO's
`ISocket::handle_pipe_event`, so RADIO can never learn about group membership.

## Solution

Change `process_heartbeat_command_impl` to return a richer enum that
distinguishes "handled" from "forward to socket logic". Update the call site
in `actor.rs` to forward unrecognised commands.

## Files Changed

- `core/src/sessionx/protocol_handler/heartbeat.rs`
- `core/src/sessionx/actor.rs`

---

## Change 1: `heartbeat.rs` — new return enum and updated function

### Add the enum (near top of file or in `mod.rs`/`types.rs`)

```rust
/// Result of processing an incoming ZMTP command frame during the data phase.
pub(crate) enum DataCommandResult {
    /// A reply frame should be sent back to the peer (e.g. PONG in response to PING).
    SendReply(Msg),
    /// The command was fully handled internally; no further action needed.
    Handled,
    /// The command was not recognised by the session layer and should be
    /// forwarded to the socket pattern logic via ISocket::handle_pipe_event.
    ForwardToSocket(Msg),
}
```

### Update `process_heartbeat_command_impl` signature and body

Old signature:
```rust
pub(crate) fn process_heartbeat_command_impl<S: ZmtpStdStream>(
    handler: &mut ZmtpProtocolHandlerX<S>,
    cmd_msg: &Msg,
) -> Result<Option<Msg>, ZmqError>
```

New signature:
```rust
pub(crate) fn process_heartbeat_command_impl<S: ZmtpStdStream>(
    handler: &mut ZmtpProtocolHandlerX<S>,
    cmd_msg: &Msg,
) -> Result<DataCommandResult, ZmqError>
```

Updated match arms:

```rust
match ZmtpCommand::parse(cmd_msg) {
    Some(ZmtpCommand::Ping(ping_context)) => {
        let pong = ZmtpCommand::create_pong(&ping_context);
        Ok(DataCommandResult::SendReply(pong))
    }
    Some(ZmtpCommand::Pong(_)) => {
        handler.heartbeat_state.pong_received();
        Ok(DataCommandResult::Handled)
    }
    Some(ZmtpCommand::Error) => {
        Err(ZmqError::ProtocolViolation("Received ZMTP ERROR from peer".into()))
    }
    // JOIN, LEAVE, Ready, Unknown — forward to the socket pattern logic
    Some(_) | None => {
        // Clone the msg so it can be forwarded
        Ok(DataCommandResult::ForwardToSocket(cmd_msg.clone()))
    }
}
```

Note: `None` from `ZmtpCommand::parse` previously returned a
`ProtocolViolation` error. Changing it to forward is safer — malformed
commands become a socket-layer concern rather than a connection-fatal error.
If desired, keep the `None` arm as an error for strict mode.

---

## Change 2: `actor.rs` — update call site

Old code (around line 543):
```rust
if msg.is_command() {
    match self.zmtp_handler.process_incoming_data_command_frame(&msg) {
        Ok(Some(pong_reply)) => {
            if let Err(e) = self.zmtp_handler.write_data_msg(pong_reply, true).await {
                self.set_fatal_error(e).await;
            }
        }
        Ok(None) => { /* PONG received and handled */ }
        Err(e) => self.set_fatal_error(e).await,
    }
}
```

New code:
```rust
if msg.is_command() {
    match self.zmtp_handler.process_incoming_data_command_frame(&msg) {
        Ok(DataCommandResult::SendReply(pong_reply)) => {
            if let Err(e) = self.zmtp_handler.write_data_msg(pong_reply, true).await {
                self.set_fatal_error(e).await;
            }
        }
        Ok(DataCommandResult::Handled) => {}
        Ok(DataCommandResult::ForwardToSocket(cmd_msg)) => {
            // Forward JOIN/LEAVE (and any other unrecognised commands) to
            // the socket pattern logic.
            let pipe_read_id = self
                .core_pipe_manager
                .state
                .core_pipe_read_id_for_incoming_routing
                .expect("Cannot forward command without core_pipe_read_id");
            let command_for_isocket = Command::PipeMessageReceived {
                pipe_id: pipe_read_id,
                msg: cmd_msg,
            };
            if let Err(e) = self
                .socket_logic
                .handle_pipe_event(pipe_read_id, command_for_isocket)
                .await
            {
                tracing::warn!(
                    sca_handle = self.handle,
                    error = %e,
                    "Error forwarding unknown ZMTP command to ISocket. Ignoring."
                );
                // Not fatal — unknown commands should not kill the connection.
            }
        }
        Err(e) => self.set_fatal_error(e).await,
    }
}
```

## Impact on Existing Socket Types

All existing socket types (`PubSocket`, `SubSocket`, etc.) already handle
`PipeMessageReceived` in their `handle_pipe_event` implementations. Their
existing code paths check `msg.is_command()` or just process data frames —
forwarded commands will be silently ignored by patterns that don't care
(they fall through to the `_ => {}` arm or similar).

Specifically, `PubSocket::handle_pipe_event` already returns `Ok(())` for
all events, so forwarding JOIN/LEAVE to it is a safe no-op.
