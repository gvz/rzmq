# UDP io_uring Backend — UdpUringActor Design

## Overview

The `UdpUringActor` is a standalone actor running on a dedicated OS thread,
owning a small `IoUring` instance. It manages one or more UDP file descriptors
for a single Radio or Dish socket, handling both send and receive operations
through io_uring kernel calls.

## Struct Definition

```rust
// core/src/io_uring_backend/udp_uring_actor.rs
#![cfg(all(feature = "udp", feature = "io-uring"))]

pub(crate) struct UdpUringActor {
    // --- io_uring core ---
    ring: IoUring,
    state: UdpUringActorState,

    // --- Socket FD management ---
    /// The UDP socket file descriptor (owned by this actor after handoff).
    /// For Dish: the recv socket FD.
    /// For Radio: the send socket FD (may be shared across multiple targets).
    socket_fd: RawFd,

    // --- Receive path (Dish side) ---
    /// Buffer ring for provided-buffer multishot recvmsg
    recv_buffer_manager: Option<BufferRingManager>,
    recv_bgid: Option<u16>,
    /// Tracks the active multishot recvmsg operation
    multishot_recvmsg_ud: Option<u64>,
    /// Pre-allocated msghdr for recvmsg
    recv_msghdr: Option<Box<RecvMsgContext>>,

    // --- Send path (Radio side) ---
    /// Channel to receive outgoing datagrams from RadioSocket
    send_rx: Option<fibre::mpsc::BoundedReceiver<UdpSendRequest>>,
    /// Whether zero-copy sendmsg is enabled
    send_zerocopy_enabled: bool,
    /// Tracks in-flight send operations for ZC notification handling
    pending_zc_sends: u32,

    // --- Signaling ---
    /// eventfd for waking the actor from async context
    event_fd: eventfd::EventFD,
    /// eventfd poll operation user_data
    eventfd_poll_ud: u64,

    // --- Control ---
    /// Channel to receive Stop commands
    control_rx: fibre::mpsc::BoundedReceiver<UdpUringCommand>,

    // --- Upstream delivery ---
    /// Reference to the ISocket for delivering received datagrams (Dish side)
    socket_logic: Option<Arc<dyn ISocket>>,
    /// Synthetic pipe_read_id for PipeMessageReceived commands
    pipe_read_id: usize,

    // --- Operation tracking ---
    next_user_data: u64,
    pending_ops: HashMap<u64, UdpUringOpType>,

    // --- Configuration ---
    endpoint_uri: String,
    send_addr: Option<SocketAddr>,  // Destination for sends (Radio)
    context: Context,
    handle: usize,
}
```

## State Machine

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpUringActorState {
    /// Actor is initializing (setting up ring, buffers, submitting initial SQEs)
    Initializing,
    /// Actor is running normally
    Running,
    /// Shutdown initiated, draining in-flight CQEs
    Draining,
    /// All operations complete, actor will exit
    Stopped,
}
```

## Supporting Types

```rust
/// Commands that can be sent to the actor's control channel
enum UdpUringCommand {
    Stop,
}

/// A request to send a datagram (from RadioSocket to actor)
struct UdpSendRequest {
    /// Pre-encoded datagram bytes (group_len | group | payload)
    data: Bytes,
    /// Destination address
    target_addr: SocketAddr,
}

/// Tracks what each in-flight SQE is for
enum UdpUringOpType {
    /// Multishot recvmsg operation
    RecvMsgMultishot,
    /// Single-shot recvmsg (fallback)
    RecvMsg,
    /// sendmsg operation
    SendMsg,
    /// Zero-copy sendmsg operation (awaiting CQE_F_NOTIFY)
    SendMsgZc { notify_pending: bool },
    /// eventfd poll
    EventFdPoll,
    /// Cancel operation
    AsyncCancel { target_ud: u64 },
}
```

## Context for recvmsg

The `recvmsg` io_uring operation requires a persistent `msghdr` structure that
lives for the duration of the operation. For multishot recvmsg with provided
buffers, the kernel fills in the buffer from the provided buffer ring.

```rust
/// Holds the libc structures needed for recvmsg SQE submission.
/// Must be pinned / heap-allocated so the kernel can reference them.
struct RecvMsgContext {
    /// Source address storage (filled by kernel on each datagram)
    addr_storage: libc::sockaddr_storage,
    addr_len: libc::socklen_t,
    /// iovec pointing to provided buffer (for non-multishot fallback)
    iovec: libc::iovec,
    /// The msghdr structure itself
    msghdr: libc::msghdr,
    /// Control message buffer (for ancillary data, if needed in future)
    control_buf: [u8; 64],
}
```

For **multishot recvmsg with provided buffers**, the kernel ignores the
`msg_iov` field and instead fills a buffer from the provided buffer ring.
The CQE carries the buffer ID and length. We still need the `msghdr` for
the source address (`msg_name`).

## Main Loop

The actor's main loop follows a simplified 3-phase pattern (compared to the
TCP worker's 4-phase loop):

```
loop {
    // PHASE 1: GATHER WORK
    //   1a. Check control channel for Stop command
    //   1b. Drain send channel (Radio side) → queue sendmsg SQEs
    //   1c. Ensure multishot recvmsg is active (Dish side)

    // PHASE 2: SUBMIT
    //   Submit all queued SQEs to kernel

    // PHASE 3: WAIT + PROCESS CQEs
    //   Wait for completions (with adaptive timeout)
    //   Process each CQE:
    //     - RecvMsg: extract datagram, deliver to ISocket
    //     - SendMsg: mark send complete
    //     - SendMsgZc: handle notification
    //     - EventFdPoll: drain eventfd, re-arm poll
}
```

### Phase 1: Gather Work

```rust
fn gather_work(&mut self) -> bool {
    let mut work_available = false;

    // 1a. Check control channel
    if let Ok(UdpUringCommand::Stop) = self.control_rx.try_recv() {
        self.transition_to_draining();
        return false;
    }

    // 1b. Drain send requests (Radio side)
    if let Some(ref send_rx) = self.send_rx {
        while let Ok(req) = send_rx.try_recv() {
            self.queue_sendmsg(req);
            work_available = true;
        }
    }

    // 1c. Ensure multishot recvmsg is active (Dish side)
    if self.recv_buffer_manager.is_some() && self.multishot_recvmsg_ud.is_none() {
        self.submit_multishot_recvmsg();
        work_available = true;
    }

    work_available
}
```

### Phase 3: Process CQEs

```rust
fn process_cqes(&mut self) {
    let cq = unsafe { self.ring.completion_shared() };
    cq.sync();

    for cqe in cq {
        let ud = cqe.user_data();
        let result = cqe.result();
        let flags = cqe.flags();

        if ud == self.eventfd_poll_ud {
            self.handle_eventfd_cqe(result);
            continue;
        }

        match self.pending_ops.get(&ud) {
            Some(UdpUringOpType::RecvMsgMultishot) => {
                let has_more = (flags & io_uring::cqueue::MORE) != 0;
                if result >= 0 {
                    self.handle_recv_cqe(result as usize, flags);
                }
                if !has_more {
                    // Multishot terminated; re-submit
                    self.multishot_recvmsg_ud = None;
                }
            }
            Some(UdpUringOpType::SendMsg) => {
                self.pending_ops.remove(&ud);
                if result < 0 {
                    tracing::error!("sendmsg failed: {}", io::Error::from_raw_os_error(-result));
                }
            }
            Some(UdpUringOpType::SendMsgZc { .. }) => {
                // Zero-copy: two CQEs per send
                // First CQE: result == 0 or bytes_sent (submission ack)
                // Second CQE: CQE_F_NOTIFY flag (kernel done with buffer)
                let is_notify = (flags & 8) != 0; // CQE_F_NOTIFY = 1 << 3
                if is_notify {
                    self.pending_ops.remove(&ud);
                    self.pending_zc_sends -= 1;
                }
            }
            _ => {
                tracing::warn!("Unknown CQE ud={}, result={}", ud, result);
            }
        }
    }
}
```

## Spawn Pattern

```rust
impl UdpUringActor {
    /// Spawns the actor on a dedicated OS thread.
    /// Returns handles for sending data and controlling the actor.
    pub(crate) fn spawn(
        config: UdpUringActorConfig,
    ) -> Result<UdpUringActorHandle, ZmqError> {
        let (control_tx, control_rx) = fibre::mpsc::bounded(16);
        let (send_tx, send_rx) = fibre::mpsc::bounded(1024);

        let event_fd = eventfd::EventFD::new(
            0,
            eventfd::EfdFlags::EFD_CLOEXEC | eventfd::EfdFlags::EFD_NONBLOCK,
        )?;

        let event_fd_clone = event_fd.clone();
        let join_handle = std::thread::Builder::new()
            .name(format!("rzmq-udp-uring-{}", config.handle))
            .spawn(move || {
                let ring = IoUring::new(config.ring_entries)
                    .map_err(|e| ZmqError::Internal(format!("IoUring init: {}", e)))?;

                let mut actor = UdpUringActor {
                    ring,
                    state: UdpUringActorState::Initializing,
                    socket_fd: config.socket_fd,
                    // ... initialize all fields ...
                };

                actor.initialize()?;
                actor.run_loop()
            })?;

        Ok(UdpUringActorHandle {
            control_tx,
            send_tx: Some(send_tx),
            event_fd: event_fd_clone,
            join_handle,
        })
    }
}
```

## Actor Handle

The `UdpUringActorHandle` is held by the socket layer and provides the
interface for controlling the actor:

```rust
pub(crate) struct UdpUringActorHandle {
    /// Send Stop command to the actor
    control_tx: fibre::mpsc::BoundedAsyncSender<UdpUringCommand>,
    /// Send datagrams to the actor (Radio side only; None for Dish)
    send_tx: Option<fibre::mpsc::BoundedAsyncSender<UdpSendRequest>>,
    /// eventfd to wake the actor thread after queuing work
    event_fd: eventfd::EventFD,
    /// OS thread join handle
    join_handle: std::thread::JoinHandle<Result<(), ZmqError>>,
}
```

## Initialization

When the actor starts:

1. **Set up buffer ring** (Dish only): Create `BufferRingManager` with 32 buffers
   of 65536 bytes (enough for max UDP datagram).
2. **Submit eventfd poll**: `IORING_OP_POLL_ADD` on the eventfd FD for wakeup.
3. **Submit initial multishot recvmsg** (Dish only): Start receiving.
4. **Transition to Running**.

## Shutdown

1. **Stop command received** → transition to `Draining`
2. **Cancel multishot recvmsg** (if active): submit `IORING_OP_ASYNC_CANCEL`
3. **Wait for all in-flight CQEs** to complete (with timeout)
4. **Close socket FD** via `IORING_OP_CLOSE` or `libc::close()`
5. **Drop buffer ring manager** (unregisters from kernel)
6. **Transition to Stopped**, thread exits

## Error Handling

- **recvmsg errors**: Log and continue (re-submit multishot if terminated)
- **sendmsg errors**: Log and report via completion channel (if any)
- **Ring full**: Back-pressure on send channel (bounded mpsc will block sender)
- **eventfd errors**: Log, actor continues (worst case: delayed wakeup)
- **Fatal ring errors**: Actor stops, notifies socket via pipe close event
