# UDP io_uring Backend — Receive Path (Multishot recvmsg)

## Overview

The receive path uses `IORING_OP_RECVMSG` with the `IOSQE_BUFFER_SELECT` flag
and multishot mode to receive multiple UDP datagrams from a single SQE
submission. Each incoming datagram produces a CQE that references a buffer
from the provided buffer ring. This is the Dish side of the Radio-Dish pattern.

## Kernel Requirements

- **Multishot recvmsg**: Linux 6.0+ (`io_uring` feature `IORING_FEAT_RECVMSG_MULTISHOT`)
- **Provided buffer rings**: Linux 5.19+ (`io_uring_buf_ring`)

The `io-uring` crate v0.7 supports these opcodes.

## SQE Submission: Multishot recvmsg

### msghdr Setup

For multishot recvmsg with provided buffers, the kernel uses the `msg_name`
and `msg_namelen` fields to return source address info, but the actual data
goes into a buffer selected from the buffer ring. The `msg_iov` is ignored
when `IOSQE_BUFFER_SELECT` is set.

```rust
fn submit_multishot_recvmsg(&mut self) -> Result<(), ZmqError> {
    let bgid = self.recv_bgid
        .ok_or(ZmqError::InvalidState("No buffer ring configured"))?;

    // Set up the msghdr for recvmsg
    let ctx = self.recv_msghdr.as_mut()
        .ok_or(ZmqError::InvalidState("RecvMsgContext not initialized"))?;

    // Zero out the address storage
    ctx.addr_storage = unsafe { std::mem::zeroed() };
    ctx.addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    // Configure msghdr
    ctx.msghdr = unsafe { std::mem::zeroed() };
    ctx.msghdr.msg_name = &mut ctx.addr_storage as *mut _ as *mut libc::c_void;
    ctx.msghdr.msg_namelen = ctx.addr_len;
    // msg_iov and msg_iovlen are ignored with BUFFER_SELECT
    ctx.msghdr.msg_iov = std::ptr::null_mut();
    ctx.msghdr.msg_iovlen = 0;
    // No control messages needed for basic UDP
    ctx.msghdr.msg_control = std::ptr::null_mut();
    ctx.msghdr.msg_controllen = 0;
    ctx.msghdr.msg_flags = 0;

    // Build the SQE
    let ud = self.next_ud();
    let sqe = opcode::RecvMsg::new(
        types::Fd(self.socket_fd),
        &mut ctx.msghdr as *mut libc::msghdr,
    )
    .buf_group(bgid)
    .build()
    .flags(squeue::Flags::BUFFER_SELECT)
    // Set multishot flag via IOSQE_IO_LINK is NOT what we want.
    // Multishot for recvmsg is set via IORING_RECV_MULTISHOT in the op flags.
    // In io-uring crate, this is set via the RecvMsg builder:
    //   .flags(squeue::Flags::BUFFER_SELECT)
    // and the multishot behavior is controlled by the IORING_RECVMSG_MULTISHOT
    // flag in the SQE's ioprio field. In io-uring v0.7:
    //   .ioprio(IORING_RECV_MULTISHOT as u16)
    ;

    // For multishot, we need to set IORING_RECV_MULTISHOT in ioprio.
    // The io-uring crate may expose this differently. Check the RecvMsg builder.
    // If not directly exposed, we can set it on the raw entry:
    let sqe = sqe.user_data(ud);

    // Track the operation
    self.pending_ops.insert(ud, UdpUringOpType::RecvMsgMultishot);
    self.multishot_recvmsg_ud = Some(ud);

    // Submit
    let mut sq = unsafe { self.ring.submission_shared() };
    unsafe {
        sq.push(&sqe).map_err(|_| ZmqError::ResourceLimitReached)?;
    }

    tracing::debug!(
        "UdpUringActor: Submitted multishot recvmsg SQE, fd={}, bgid={}, ud={}",
        self.socket_fd, bgid, ud
    );

    Ok(())
}
```

### Setting Multishot Flag

The `IORING_RECV_MULTISHOT` flag (value `2`) must be set in the SQE's `ioprio`
field. The `io-uring` crate v0.7 may or may not expose a builder method for
this. Implementation options:

**Option A: Use RecvMsg builder if it has `.multi(true)` method**
```rust
let sqe = opcode::RecvMsg::new(fd, msghdr_ptr)
    .buf_group(bgid)
    .build()
    .flags(squeue::Flags::BUFFER_SELECT)
    .user_data(ud);
// Then modify the raw entry's ioprio:
// sqe.0.ioprio |= IORING_RECV_MULTISHOT;
```

**Option B: Manually construct the entry**
```rust
const IORING_RECV_MULTISHOT: u16 = 2; // From linux/io_uring.h

let mut sqe = opcode::RecvMsg::new(fd, msghdr_ptr)
    .build()
    .flags(squeue::Flags::BUFFER_SELECT)
    .user_data(ud);

// Access the raw SQE and set ioprio for multishot
unsafe {
    let raw: &mut io_uring::squeue::Entry = &mut sqe;
    // ioprio field carries IORING_RECV_MULTISHOT
    // Need to OR with buf_group which is also in ioprio-adjacent field
}
```

During implementation, verify the exact API. The `io-uring` crate v0.7 may
have added explicit multishot support for `RecvMsg`.

## CQE Processing: Received Datagram

Each successful recvmsg CQE contains:

| Field | Content |
|---|---|
| `result` | Number of bytes received (datagram size) |
| `flags` | `IORING_CQE_F_MORE` if multishot continues; `IORING_CQE_F_BUFFER` with buffer ID |
| Buffer ID | `(flags >> IORING_CQE_BUFFER_SHIFT) & 0xFFFF` |

### CQE Layout for Multishot recvmsg with Provided Buffers

When using multishot recvmsg with provided buffers, the CQE result contains
the total bytes written to the buffer. The buffer layout is:

```
┌──────────────────────────────────┐
│  io_uring_recvmsg_out header     │  (struct io_uring_recvmsg_out)
│    namelen: u32                  │  (size of source address)
│    controllen: u32               │  (size of control data, usually 0)
│    payloadlen: u32               │  (actual datagram payload length)
│    flags: u32                    │  (msg_flags from recvmsg)
├──────────────────────────────────┤
│  Source address (namelen bytes)   │  (sockaddr_in or sockaddr_in6)
│  [padding to alignment]          │
├──────────────────────────────────┤
│  Control data (controllen bytes)  │  (usually empty for UDP)
│  [padding to alignment]          │
├──────────────────────────────────┤
│  Payload data (payloadlen bytes)  │  (the actual UDP datagram)
└──────────────────────────────────┘
```

### Extracting the Datagram

```rust
fn handle_recv_cqe(&mut self, bytes_received: usize, cqe_flags: u32) {
    // Extract buffer ID from CQE flags
    let buffer_id = (cqe_flags >> 16) as u16; // IORING_CQE_BUFFER_SHIFT = 16

    let buf_mgr = match &self.recv_buffer_manager {
        Some(bm) => bm,
        None => return,
    };

    // Borrow the kernel-filled buffer
    let borrowed_buf = match unsafe { buf_mgr.borrow_kernel_filled_buffer(buffer_id, bytes_received) } {
        Ok(buf) => buf,
        Err(e) => {
            tracing::error!("Failed to borrow recv buffer {}: {}", buffer_id, e);
            return;
        }
    };

    let buf_slice = &borrowed_buf[..bytes_received];

    // Parse the io_uring_recvmsg_out header
    // The header is 16 bytes: namelen(4) + controllen(4) + payloadlen(4) + flags(4)
    if bytes_received < 16 {
        tracing::warn!("recvmsg CQE too small for header: {} bytes", bytes_received);
        return;
    }

    let namelen = u32::from_ne_bytes(buf_slice[0..4].try_into().unwrap()) as usize;
    let controllen = u32::from_ne_bytes(buf_slice[4..8].try_into().unwrap()) as usize;
    let payloadlen = u32::from_ne_bytes(buf_slice[8..12].try_into().unwrap()) as usize;
    let _msg_flags = u32::from_ne_bytes(buf_slice[12..16].try_into().unwrap());

    // Calculate payload offset (after header + name + control, with alignment)
    let header_size = 16;
    let name_end = header_size + namelen;
    let name_padded = (name_end + 3) & !3; // Align to 4 bytes
    let control_end = name_padded + controllen;
    let control_padded = (control_end + 3) & !3;
    let payload_start = control_padded;
    let payload_end = payload_start + payloadlen;

    if payload_end > bytes_received {
        tracing::warn!(
            "recvmsg payload exceeds buffer: payload_end={}, received={}",
            payload_end, bytes_received
        );
        return;
    }

    let datagram = &buf_slice[payload_start..payload_end];

    if datagram.is_empty() {
        tracing::warn!("Empty UDP datagram received, skipping");
        return;
    }

    // Build Msg and deliver to ISocket (same as Tokio path)
    let msg = crate::message::Msg::from_vec(datagram.to_vec());
    let cmd = crate::runtime::Command::PipeMessageReceived {
        pipe_id: self.pipe_read_id,
        msg,
    };

    // We're on a dedicated OS thread, so we need to use a blocking runtime
    // call to deliver to the async ISocket. Use a pre-created tokio Runtime
    // handle or a channel-based approach.
    self.deliver_to_socket(cmd);

    // Buffer is automatically returned to the ring when `borrowed_buf` is dropped
}
```

## Delivery to ISocket: Thread Bridging

The `UdpUringActor` runs on a dedicated OS thread (not a Tokio task), but
`ISocket::handle_pipe_event()` is an async method. We need a bridging
mechanism:

### Option A: Channel-based delivery (Recommended)

Instead of calling `ISocket::handle_pipe_event()` directly from the OS thread,
send the received `Command` through a bounded async channel. A small Tokio task
drains the channel and calls `handle_pipe_event()`.

```rust
// In the actor:
fn deliver_to_socket(&self, cmd: Command) {
    if let Err(e) = self.recv_delivery_tx.try_send(cmd) {
        tracing::error!("Failed to deliver datagram to socket: {}", e);
    }
}

// A Tokio task spawned alongside the actor:
async fn udp_uring_recv_delivery_task(
    mut rx: fibre::mpsc::BoundedAsyncReceiver<Command>,
    socket_logic: Arc<dyn ISocket>,
    pipe_read_id: usize,
) {
    while let Ok(cmd) = rx.recv().await {
        if let Err(e) = socket_logic.handle_pipe_event(pipe_read_id, cmd).await {
            tracing::error!("handle_pipe_event error: {}", e);
            break;
        }
    }
}
```

This decouples the io_uring thread from async socket processing and avoids
blocking the io_uring loop waiting for async delivery.

### Channel Sizing

The recv delivery channel should be bounded (e.g., 4096 entries) to provide
backpressure if the socket processing can't keep up. When the channel is full,
datagrams are dropped — which is acceptable for UDP.

## Multishot Lifecycle

### Normal Operation

1. Submit multishot recvmsg SQE → kernel processes it
2. For each incoming datagram → kernel produces CQE with `IORING_CQE_F_MORE`
3. Actor processes CQE, extracts datagram, delivers upstream
4. Buffer automatically returned to ring on `BorrowedBuffer` drop
5. Loop continues until multishot terminates

### Multishot Termination

The kernel terminates multishot when:
- An error occurs (CQE result < 0)
- The provided buffer ring runs out of buffers
- An explicit `IORING_OP_ASYNC_CANCEL` is submitted

When multishot terminates (CQE without `IORING_CQE_F_MORE`):
1. Remove the tracking entry for the multishot UD
2. Set `self.multishot_recvmsg_ud = None`
3. On the next loop iteration, `gather_work()` detects this and re-submits

### Shutdown

1. If multishot is active, submit `IORING_OP_ASYNC_CANCEL` targeting its UD
2. Wait for the cancellation CQE (result = `-ECANCELED` or `-EALREADY`)
3. Process any final CQEs that arrived before cancellation took effect
4. Clean up buffer ring

## Fallback: Single-shot recvmsg

If multishot recvmsg is not supported (older kernel), fall back to single-shot:

```rust
fn submit_singleshot_recvmsg(&mut self) -> Result<(), ZmqError> {
    // Same as multishot but without IORING_RECV_MULTISHOT in ioprio
    // After each CQE, must re-submit a new SQE
    let ud = self.next_ud();
    let sqe = opcode::RecvMsg::new(
        types::Fd(self.socket_fd),
        &mut self.recv_msghdr.as_mut().unwrap().msghdr,
    )
    .buf_group(self.recv_bgid.unwrap())
    .build()
    .flags(squeue::Flags::BUFFER_SELECT)
    .user_data(ud);

    self.pending_ops.insert(ud, UdpUringOpType::RecvMsg);

    let mut sq = unsafe { self.ring.submission_shared() };
    unsafe { sq.push(&sqe).map_err(|_| ZmqError::ResourceLimitReached)?; }

    Ok(())
}
```

The CQE processing for single-shot is identical except it does NOT check for
`IORING_CQE_F_MORE` and always re-submits after processing.

## Performance Considerations

1. **Buffer ring sizing**: 32 buffers of 65536 bytes = 2MB total. This is a
   good default for most workloads. Can be made configurable.

2. **Delivery channel backpressure**: When the channel fills up, datagrams
   are dropped (logged at WARN level). This prevents the io_uring thread from
   blocking.

3. **Zero-copy recv**: The `BorrowedBuffer` from the buffer ring avoids one
   copy (kernel → user buffer is the only copy). The `datagram.to_vec()` call
   creates a second copy for the `Msg`. Future optimization: use a `Msg`
   variant that holds a reference to the borrowed buffer (requires careful
   lifetime management).
