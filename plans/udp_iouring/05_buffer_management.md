# UDP io_uring Backend — Buffer Management

## Overview

The `UdpUringActor` manages its own buffer ring, independent of the TCP
`UringWorker`'s buffer infrastructure. This is a natural consequence of the
standalone actor architecture: each `IoUring` instance has its own buffer
group registry.

## Receive Buffer Ring

### Design

The receive path uses a **provided buffer ring** (`io_uring_buf_ring`) for
multishot `recvmsg`. The kernel selects a free buffer from the ring for each
incoming datagram and returns the buffer ID in the CQE.

| Parameter | Default | Rationale |
|---|---|---|
| Buffer count | 32 | Enough to absorb burst traffic without running out |
| Buffer size | 65536 bytes | Max UDP datagram size (65535) + 1 for alignment |
| Buffer group ID (bgid) | 0 | Single group per actor; no collision with TCP worker |
| Crate | `io_uring_buf_ring` v0.2 | Same as TCP backend |

### Initialization

```rust
fn initialize_recv_buffers(&mut self) -> Result<(), ZmqError> {
    let bgid = 0u16;
    let buf_count = self.config.recv_buffer_count as u16;
    let buf_size = self.config.recv_buffer_size;

    let buf_mgr = BufferRingManager::new(&self.ring, buf_count, bgid, buf_size)?;

    tracing::info!(
        "UdpUringActor: Recv buffer ring initialized (bgid={}, count={}, size={})",
        bgid, buf_count, buf_size
    );

    self.recv_buffer_manager = Some(buf_mgr);
    self.recv_bgid = Some(bgid);
    Ok(())
}
```

### Buffer Lifecycle

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Free in     │────▶│  Kernel fills │────▶│  User borrows│
│  buffer ring │     │  on recvmsg  │     │  via CQE     │
└─────────────┘     └──────────────┘     └──────┬───────┘
       ▲                                         │
       │         ┌──────────────┐                │
       └─────────│  Auto-return │◀───────────────┘
                 │  on drop     │    (BorrowedBuffer dropped
                 └──────────────┘     after datagram copied to Msg)
```

1. Buffer starts in the ring (available to kernel)
2. Kernel selects a free buffer for incoming datagram
3. CQE arrives with buffer_id and data length
4. Actor calls `borrow_kernel_filled_buffer(buffer_id, len)` → `BorrowedBuffer`
5. Actor copies datagram bytes from `BorrowedBuffer` into `Msg::from_vec()`
6. `BorrowedBuffer` is dropped → buffer automatically returned to ring

### Reuse of Existing `BufferRingManager`

The actor reuses the existing `BufferRingManager` struct from
`core/src/io_uring_backend/buffer_manager.rs`. No modifications needed.
The manager wraps `IoUringBufRing<BytesMut>` and provides:

- `new(ring, ring_entries, bgid, buffer_capacity)` — creates and registers
- `borrow_kernel_filled_buffer(buffer_id, available_len)` — borrows filled buffer
- `group_id()` — returns the bgid

On drop, the `IoUringBufRing` automatically unregisters from the kernel.

### Memory Budget

| Config | Buffers | Size Each | Total |
|---|---|---|---|
| Default (32 × 64KB) | 32 | 65536 | 2 MB |
| Compact (16 × 64KB) | 16 | 65536 | 1 MB |
| Large (64 × 64KB) | 64 | 65536 | 4 MB |

For most UDP workloads, 2 MB is a reasonable default. The buffer count and
size can be configured via `UdpUringActorConfig`.

### Buffer Exhaustion

If all buffers are in use (e.g., the recv delivery channel is backed up and
`BorrowedBuffer`s haven't been dropped yet), the kernel has no buffer
available for the next datagram. This causes:

1. The multishot recvmsg CQE arrives with `-ENOBUFS` result
2. The multishot operation terminates (no `IORING_CQE_F_MORE`)
3. The actor detects termination and re-submits multishot on next loop

To minimize this:
- Size the buffer count to exceed the delivery channel depth
- Ensure the delivery Tokio task processes datagrams promptly
- Log buffer exhaustion at WARN level for monitoring

## Send Buffer Management

### Standard sendmsg (No Special Buffers)

For standard (non-zero-copy) sendmsg, no registered buffers are needed.
The `SendMsgContext` holds a `Bytes` reference to the datagram data, and
the kernel copies from user-space during the sendmsg syscall. The context
is freed when the CQE arrives.

```
Bytes (from Msg) → SendMsgContext → msghdr.msg_iov → kernel copies → CQE → free context
```

### Zero-Copy sendmsg_zc

For zero-copy sends, the kernel pins the user-space buffer pages and sends
directly from them. The buffer must remain valid until the `CQE_F_NOTIF`
CQE arrives.

**Two approaches for ZC buffer management:**

#### Approach A: Hold Bytes in SendMsgContext (Recommended)

The simplest approach: the `SendMsgContext` holds the `Bytes` object, and
the context lives in `pending_send_contexts` until the notification CQE.

```rust
// Buffer lifetime for ZC:
// 1. Bytes::copy_from_slice(data) — copies from Msg into new Bytes
// 2. SendMsgContext holds Bytes
// 3. msghdr points to Bytes data
// 4. submit SQE → kernel pins pages
// 5. First CQE (submission ack) — buffer still pinned
// 6. Second CQE (CQE_F_NOTIF) — kernel done, safe to free
// 7. Remove from pending_send_contexts → context dropped → Bytes freed
```

This avoids the complexity of a registered buffer pool. The trade-off is
that each ZC send allocates a `Bytes` copy, but since the Msg data already
exists as an allocation, and `Bytes::copy_from_slice` is cheap, this is
acceptable for UDP datagrams (max 65507 bytes).

#### Approach B: Registered Buffer Pool (Not Recommended for UDP)

The TCP backend uses `SendBufferPool` with pre-registered buffers for ZC.
This is more efficient for large TCP streams but adds complexity for UDP:

- Need to acquire/release buffers from pool
- Pool size limits concurrent ZC sends
- UDP datagrams are small, so copy cost is minimal

**Decision: Use Approach A.** The `SendBufferPool` is not needed for UDP.
The `SendMsgContext` holding `Bytes` is sufficient.

## Configuration

```rust
pub(crate) struct UdpUringActorConfig {
    /// Handle ID for logging
    pub handle: usize,
    /// The pre-created UDP socket FD
    pub socket_fd: RawFd,
    /// Endpoint URI for logging
    pub endpoint_uri: String,
    /// io_uring ring size
    pub ring_entries: u32,            // default: 64

    // --- Receive config (Dish side) ---
    /// Number of buffers in the recv buffer ring
    pub recv_buffer_count: usize,     // default: 32
    /// Size of each recv buffer
    pub recv_buffer_size: usize,      // default: 65536
    /// Synthetic pipe_read_id for message delivery
    pub pipe_read_id: usize,
    /// Reference to the socket logic (DishSocket)
    pub socket_logic: Option<Arc<dyn ISocket>>,

    // --- Send config (Radio side) ---
    /// Target address for sends
    pub send_addr: Option<SocketAddr>,
    /// Enable zero-copy sendmsg
    pub send_zerocopy: bool,          // default: false
    /// Send channel capacity
    pub send_channel_capacity: usize, // default: 1024

    /// Context for handle allocation
    pub context: Context,
}
```

## Comparison with TCP Backend Buffer Management

| Aspect | TCP (UringWorker) | UDP (UdpUringActor) |
|---|---|---|
| Recv buffer ring | Shared across all TCP FDs | Per-actor, dedicated |
| Recv bgid | Global bgid 0 | Per-actor bgid 0 (no conflict) |
| Send buffer pool | `SendBufferPool` with registered buffers | `Bytes` held in `SendMsgContext` |
| Buffer count | 16 (default) | 32 (default, UDP needs more due to bursty traffic) |
| Buffer size | 65536 | 65536 |
| Buffer ring crate | `io_uring_buf_ring` v0.2 | Same |
| Buffer ring manager | `BufferRingManager` | Same struct, new instance |
