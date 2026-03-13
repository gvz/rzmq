# UDP io_uring Backend — Send Path (sendmsg / sendmsg_zc)

## Overview

The send path handles outgoing datagrams from RadioSocket to the network via
io_uring. Two modes are supported:

1. **Standard `sendmsg`** (`IORING_OP_SENDMSG`): Default, simpler
2. **Zero-copy `sendmsg_zc`** (`IORING_OP_SENDMSG_ZC`): Optional, for high-throughput

## Data Flow

```
RadioSocket::send(msg)
    │
    ▼ encode_radio_frame()
    │
    ▼ Distributor::send_to_all()
    │
    ▼ UdpUringSendConnection::send_multipart(msgs)
    │
    ▼ send_tx.try_send(UdpSendRequest { data, target_addr })
    │                                     (signals eventfd)
    ▼
UdpUringActor (OS thread)
    │ gather_work() → drain send_rx
    │
    ▼ queue_sendmsg() or queue_sendmsg_zc()
    │
    ▼ Submit SQEs to io_uring
    │
    ▼ Kernel sends UDP datagram
    │
    ▼ CQE: send complete (or ZC notify)
```

## Standard sendmsg

### SQE Construction

```rust
fn queue_sendmsg(&mut self, req: UdpSendRequest) {
    // Allocate a SendMsgContext on the heap (must live until CQE)
    let ctx = Box::new(SendMsgContext::new(req.data, req.target_addr));
    let ud = self.next_ud();

    let sqe = opcode::SendMsg::new(
        types::Fd(self.socket_fd),
        ctx.msghdr_ptr(),
    )
    .build()
    .user_data(ud);

    // Store the context so it stays alive until the CQE
    self.pending_ops.insert(ud, UdpUringOpType::SendMsg);
    self.pending_send_contexts.insert(ud, ctx);

    let mut sq = unsafe { self.ring.submission_shared() };
    match unsafe { sq.push(&sqe) } {
        Ok(()) => {
            tracing::trace!("Queued sendmsg SQE, ud={}, {} bytes", ud, req.data.len());
        }
        Err(_) => {
            // SQ full — drop the send (UDP is lossy)
            self.pending_ops.remove(&ud);
            self.pending_send_contexts.remove(&ud);
            tracing::warn!("SQ full, dropping sendmsg");
        }
    }
}
```

### SendMsgContext

Holds the libc structures that must remain valid until the CQE completes:

```rust
struct SendMsgContext {
    /// The datagram data (kept alive)
    data: Bytes,
    /// iovec pointing to the data
    iovec: libc::iovec,
    /// Destination address
    addr_storage: libc::sockaddr_storage,
    addr_len: libc::socklen_t,
    /// The msghdr itself
    msghdr: libc::msghdr,
}

impl SendMsgContext {
    fn new(data: Bytes, target_addr: SocketAddr) -> Self {
        let mut ctx = Self {
            data,
            iovec: unsafe { std::mem::zeroed() },
            addr_storage: unsafe { std::mem::zeroed() },
            addr_len: 0,
            msghdr: unsafe { std::mem::zeroed() },
        };

        // Set up iovec pointing to data
        ctx.iovec.iov_base = ctx.data.as_ptr() as *mut libc::c_void;
        ctx.iovec.iov_len = ctx.data.len();

        // Set up destination address
        ctx.addr_len = socket_addr_to_sockaddr_storage(&target_addr, &mut ctx.addr_storage);

        // Set up msghdr
        ctx.msghdr.msg_name = &mut ctx.addr_storage as *mut _ as *mut libc::c_void;
        ctx.msghdr.msg_namelen = ctx.addr_len;
        ctx.msghdr.msg_iov = &mut ctx.iovec as *mut libc::iovec;
        ctx.msghdr.msg_iovlen = 1;
        ctx.msghdr.msg_control = std::ptr::null_mut();
        ctx.msghdr.msg_controllen = 0;
        ctx.msghdr.msg_flags = 0;

        ctx
    }

    fn msghdr_ptr(&mut self) -> *const libc::msghdr {
        // IMPORTANT: Must call after all fields are set, as msghdr contains
        // pointers to iovec and addr_storage within this same struct.
        // The struct must NOT be moved after this point.
        //
        // Since we Box the SendMsgContext, the heap allocation is stable.
        // But the internal pointers (msg_iov, msg_name) point to fields
        // within this same Box — which is safe because Box doesn't move
        // its contents after allocation.

        // Re-establish internal pointers (defensive)
        self.msghdr.msg_name = &mut self.addr_storage as *mut _ as *mut libc::c_void;
        self.msghdr.msg_namelen = self.addr_len;
        self.msghdr.msg_iov = &mut self.iovec as *mut libc::iovec;
        self.msghdr.msg_iovlen = 1;
        self.iovec.iov_base = self.data.as_ptr() as *mut libc::c_void;
        self.iovec.iov_len = self.data.len();

        &self.msghdr as *const libc::msghdr
    }
}
```

### CQE Processing: sendmsg

```rust
UdpUringOpType::SendMsg => {
    // Remove tracking
    self.pending_ops.remove(&ud);
    let _ctx = self.pending_send_contexts.remove(&ud);
    // ctx is dropped here, freeing the data and msghdr

    if result < 0 {
        let err = std::io::Error::from_raw_os_error(-result);
        tracing::error!("sendmsg failed: {}", err);
        // No retry for UDP — it's best-effort
    } else {
        tracing::trace!("sendmsg complete: {} bytes sent", result);
    }
}
```

## Zero-Copy sendmsg (sendmsg_zc)

### When to Use

Zero-copy is beneficial when:
- Datagram payload is large (>1KB)
- High send rate (reduces CPU from memcpy)
- The kernel and hardware support it

It's controlled by the `IO_URING_UDP_SNDZEROCOPY` socket option (default: `false`).

### How It Works

`IORING_OP_SENDMSG_ZC` avoids copying the data buffer into kernel space.
Instead, the kernel pins the user-space buffer pages and sends directly from
them. This produces **two CQEs per operation**:

1. **First CQE**: The sendmsg operation has been submitted to the network
   stack. `result >= 0` means success, `result < 0` means error.
2. **Second CQE**: The kernel is done with the user-space buffer and it's
   safe to free. This CQE has the `IORING_CQE_F_NOTIF` flag (bit 3, value 8).

### SQE Construction

```rust
fn queue_sendmsg_zc(&mut self, req: UdpSendRequest) {
    let ctx = Box::new(SendMsgContext::new(req.data, req.target_addr));
    let ud = self.next_ud();

    // Use SendMsgZc opcode instead of SendMsg
    let sqe = opcode::SendMsgZc::new(
        types::Fd(self.socket_fd),
        ctx.msghdr_ptr(),
    )
    .build()
    .user_data(ud);

    self.pending_ops.insert(ud, UdpUringOpType::SendMsgZc { notify_pending: false });
    self.pending_send_contexts.insert(ud, ctx);
    self.pending_zc_sends += 1;

    let mut sq = unsafe { self.ring.submission_shared() };
    match unsafe { sq.push(&sqe) } {
        Ok(()) => {
            tracing::trace!("Queued sendmsg_zc SQE, ud={}", ud);
        }
        Err(_) => {
            self.pending_ops.remove(&ud);
            self.pending_send_contexts.remove(&ud);
            self.pending_zc_sends -= 1;
            tracing::warn!("SQ full, dropping sendmsg_zc");
        }
    }
}
```

### CQE Processing: sendmsg_zc

```rust
UdpUringOpType::SendMsgZc { notify_pending } => {
    const CQE_F_NOTIFY: u32 = 1 << 3; // IORING_CQE_F_NOTIF

    if (flags & CQE_F_NOTIFY) != 0 {
        // This is the notification CQE — kernel is done with our buffer
        self.pending_ops.remove(&ud);
        let _ctx = self.pending_send_contexts.remove(&ud);
        self.pending_zc_sends -= 1;
        tracing::trace!("sendmsg_zc notify received, ud={}, buffer freed", ud);
    } else {
        // This is the initial completion CQE
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            tracing::error!("sendmsg_zc failed: {}", err);
            // Still need to wait for notify CQE before freeing buffer
            if let Some(op) = self.pending_ops.get_mut(&ud) {
                *op = UdpUringOpType::SendMsgZc { notify_pending: true };
            }
        } else {
            tracing::trace!("sendmsg_zc submitted: {} bytes, waiting for notify", result);
            if let Some(op) = self.pending_ops.get_mut(&ud) {
                *op = UdpUringOpType::SendMsgZc { notify_pending: true };
            }
        }
    }
}
```

## UdpUringSendConnection

The `UdpUringSendConnection` implements `ISocketConnection` and bridges
the RadioSocket (async Tokio) to the io_uring actor (OS thread):

```rust
#[cfg(all(feature = "udp", feature = "io-uring"))]
pub(crate) struct UdpUringSendConnection {
    /// Channel to send data to the UdpUringActor
    send_tx: fibre::mpsc::BoundedAsyncSender<UdpSendRequest>,
    /// eventfd to wake the actor after queuing data
    event_fd: eventfd::EventFD,
    /// Target address for sends
    send_addr: SocketAddr,
    /// Stable connection ID
    connection_id: usize,
}

#[cfg(all(feature = "udp", feature = "io-uring"))]
#[async_trait]
impl ISocketConnection for UdpUringSendConnection {
    async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
        if msgs.len() != 1 {
            return Err(ZmqError::InvalidState(
                "UDP Radio-Dish requires exactly one frame per send",
            ));
        }
        let data = msgs[0].data().unwrap_or(&[]);
        const UDP_MAX: usize = 65507;
        if data.len() > UDP_MAX {
            return Err(ZmqError::MessageTooLarge(data.len(), UDP_MAX));
        }

        let req = UdpSendRequest {
            data: Bytes::copy_from_slice(data),
            target_addr: self.send_addr,
        };

        // Non-blocking send to the actor's channel
        match self.send_tx.try_send(req) {
            Ok(()) => {
                // Signal the actor to wake up
                let _ = self.event_fd.write(1u64);
                Ok(())
            }
            Err(fibre::TrySendError::Full(_)) => {
                // Backpressure: channel full, drop the datagram
                tracing::warn!("UDP io_uring send channel full, dropping datagram");
                Err(ZmqError::ResourceLimitReached)
            }
            Err(fibre::TrySendError::Closed(_)) => {
                Err(ZmqError::ConnectionClosed)
            }
            _ => unreachable!()
        }
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
        // UDP is connectionless; no teardown needed.
        Ok(())
    }

    fn get_connection_id(&self) -> usize {
        self.connection_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
```

## Batched Sends

When multiple datagrams are queued in the send channel, the actor drains
them all in a single `gather_work()` call and submits multiple sendmsg SQEs
at once. This amortizes the `submit()` syscall overhead across many sends.

```rust
// In gather_work():
if let Some(ref send_rx) = self.send_rx {
    let mut batch_count = 0;
    while let Ok(req) = send_rx.try_recv() {
        if self.send_zerocopy_enabled {
            self.queue_sendmsg_zc(req);
        } else {
            self.queue_sendmsg(req);
        }
        batch_count += 1;
        // Budget to avoid starving receives
        if batch_count >= 64 {
            break;
        }
    }
}
```

## Self-Referential Struct Safety

The `SendMsgContext` contains a `msghdr` that points to `iovec` and
`addr_storage` within the same struct. This is safe because:

1. The context is `Box`-allocated (heap, stable address)
2. Internal pointers are re-established in `msghdr_ptr()` before use
3. The context is not moved after `msghdr_ptr()` is called
4. The context lives in `pending_send_contexts` until the CQE arrives

An alternative approach using `Pin<Box<SendMsgContext>>` could make these
guarantees more explicit, but the current design is equivalent since we
never move the Box contents.

## Performance Notes

- **Send channel sizing**: 1024 entries default. At ~1500 bytes per datagram,
  this represents ~1.5MB of queued data.
- **SQE batching**: Draining multiple sends per loop iteration reduces
  syscall overhead (one `submit()` for many SQEs).
- **Zero-copy threshold**: Only use ZC for datagrams >1KB. For small datagrams,
  the page-pinning overhead exceeds the copy cost.
- **Backpressure**: When the send channel is full, datagrams are dropped
  immediately (UDP semantics). The RadioSocket receives
  `ZmqError::ResourceLimitReached`.
