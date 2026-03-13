# UDP io_uring Backend — New Files

## File 1: `core/src/io_uring_backend/udp_uring_actor.rs`

**Purpose:** Main `UdpUringActor` struct, spawn logic, main loop, and CQE processing.

**Feature gate:** `#![cfg(all(feature = "udp", feature = "io-uring"))]`

**Estimated size:** ~700 lines

### Contents

```rust
// --- Types ---
pub(crate) struct UdpUringActor { ... }
pub(crate) struct UdpUringActorConfig { ... }
pub(crate) struct UdpUringActorHandle { ... }
pub(crate) enum UdpUringCommand { Stop }
pub(crate) struct UdpSendRequest { data: Bytes, target_addr: SocketAddr }
enum UdpUringActorState { Initializing, Running, Draining, Stopped }
enum UdpUringOpType { RecvMsgMultishot, RecvMsg, SendMsg, SendMsgZc{..}, EventFdPoll, AsyncCancel{..} }

// --- RecvMsg context (for msghdr) ---
struct RecvMsgContext { addr_storage, addr_len, msghdr }

// --- SendMsg context (holds data alive for kernel) ---
struct SendMsgContext { data: Bytes, iovec, addr_storage, addr_len, msghdr }

// --- Spawn ---
impl UdpUringActor {
    pub(crate) fn spawn(config: UdpUringActorConfig)
        -> Result<UdpUringActorHandle, ZmqError>;
}

// --- Initialization ---
impl UdpUringActor {
    fn initialize(&mut self) -> Result<(), ZmqError>;
    fn initialize_recv_buffers(&mut self) -> Result<(), ZmqError>;
    fn submit_eventfd_poll(&mut self) -> Result<(), ZmqError>;
    fn submit_multishot_recvmsg(&mut self) -> Result<(), ZmqError>;
}

// --- Main loop ---
impl UdpUringActor {
    fn run_loop(&mut self) -> Result<(), ZmqError>;
    fn gather_work(&mut self) -> bool;
    fn process_cqes(&mut self);
    fn transition_to_draining(&mut self);
}

// --- CQE handlers ---
impl UdpUringActor {
    fn handle_recv_cqe(&mut self, bytes_received: usize, cqe_flags: u32);
    fn handle_eventfd_cqe(&mut self, result: i32);
    fn deliver_to_socket(&self, cmd: Command);
}

// --- Send SQE builders ---
impl UdpUringActor {
    fn queue_sendmsg(&mut self, req: UdpSendRequest);
    fn queue_sendmsg_zc(&mut self, req: UdpSendRequest);
}

// --- Helpers ---
impl UdpUringActor {
    fn next_ud(&mut self) -> u64;
}

impl SendMsgContext {
    fn new(data: Bytes, target_addr: SocketAddr) -> Self;
    fn msghdr_ptr(&mut self) -> *const libc::msghdr;
}
```

### Key Dependencies

- `io_uring::{IoUring, opcode, types, squeue, cqueue}`
- `crate::io_uring_backend::buffer_manager::BufferRingManager`
- `crate::socket::ISocket`
- `crate::runtime::Command`
- `crate::message::Msg`
- `fibre::mpsc`
- `eventfd::EventFD`
- `bytes::Bytes`
- `libc`

---

## File 2: `core/src/io_uring_backend/udp_send_connection.rs`

**Purpose:** `UdpUringSendConnection` implementing `ISocketConnection`.

**Feature gate:** `#![cfg(all(feature = "udp", feature = "io-uring"))]`

**Estimated size:** ~100 lines

### Contents

```rust
pub(crate) struct UdpUringSendConnection {
    send_tx: fibre::mpsc::BoundedAsyncSender<UdpSendRequest>,
    event_fd: eventfd::EventFD,
    send_addr: SocketAddr,
    connection_id: usize,
}

impl UdpUringSendConnection {
    pub(crate) fn new(...) -> Self;
}

#[async_trait]
impl ISocketConnection for UdpUringSendConnection {
    async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError>;
    async fn close_connection(&self) -> Result<(), ZmqError>;
    fn get_connection_id(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}

impl fmt::Debug for UdpUringSendConnection { ... }
```

### Key Dependencies

- `crate::socket::connection_iface::ISocketConnection`
- `crate::io_uring_backend::udp_uring_actor::UdpSendRequest`
- `fibre::mpsc::BoundedAsyncSender`
- `eventfd::EventFD`
- `async_trait`

---

## File 3: `core/src/transport/udp_uring.rs`

**Purpose:** Socket creation helpers for the io_uring UDP path. Thin wrappers
around the existing `socket2` setup that return raw FDs instead of Tokio sockets.

**Feature gate:** `#![cfg(all(feature = "udp", feature = "io-uring"))]`

**Estimated size:** ~150 lines

### Contents

```rust
use crate::transport::udp_endpoint::{UdpEndpoint, UdpMode};

/// Creates a bound raw UDP socket for io_uring use (Radio bind).
/// Returns (RawFd, resolved_uri). Caller owns the FD.
pub(crate) fn create_bound_raw_udp_socket(
    endpoint: &UdpEndpoint,
    options: &SocketOptions,
) -> Result<(RawFd, String), ZmqError>;

/// Creates a raw UDP socket bound to ephemeral port (Radio connect).
/// Returns RawFd. Caller owns the FD.
pub(crate) fn create_connected_raw_udp_socket(
    endpoint: &UdpEndpoint,
    options: &SocketOptions,
) -> Result<RawFd, ZmqError>;

/// Creates a bound raw UDP socket for receiving (Dish bind).
/// Joins multicast group if applicable. Returns (RawFd, resolved_uri).
pub(crate) fn create_recv_raw_udp_socket(
    endpoint: &UdpEndpoint,
    options: &SocketOptions,
) -> Result<(RawFd, String), ZmqError>;
```

These functions mirror `udp.rs::create_bound_send_socket()`,
`create_connected_send_socket()`, and the socket setup in
`UdpReceiveActor::create_and_spawn()`, but:
- Set `O_NONBLOCK` via `socket2` (for io_uring compatibility)
- Return `RawFd` via `into_raw_fd()` instead of converting to `UdpSocket`
- Do NOT create a Tokio socket

---

## File 4: `core/tests/udp_iouring_radio_dish.rs`

**Purpose:** Integration tests for the UDP io_uring path.

**Feature gate:** `#[cfg(all(feature = "udp", feature = "io-uring"))]`

**Estimated size:** ~400 lines

### Test Cases

See `09_tests.md` for detailed test specifications.

---

## File 5: `core/benches/udp_throughput.rs`

**Purpose:** Criterion benchmarks comparing Tokio vs io_uring UDP throughput.

**Feature gate:** `#[cfg(all(feature = "udp", feature = "io-uring"))]`

**Estimated size:** ~200 lines

### Benchmark Scenarios

1. **Small datagram throughput**: 64-byte payloads, measure msgs/sec
2. **Large datagram throughput**: 8KB payloads, measure MB/sec
3. **Latency**: Round-trip time for single datagrams
4. **Burst**: Send N datagrams as fast as possible, measure completion time

Each scenario runs with both Tokio and io_uring backends for comparison.

---

## File 6: `core/src/io_uring_backend/udp_recv_delivery.rs`

**Purpose:** The Tokio task that bridges the io_uring recv thread to the
async socket layer.

**Feature gate:** `#![cfg(all(feature = "udp", feature = "io-uring"))]`

**Estimated size:** ~80 lines

### Contents

```rust
/// Spawns a Tokio task that drains the recv delivery channel and calls
/// ISocket::handle_pipe_event() for each received datagram.
pub(crate) fn spawn_recv_delivery_task(
    recv_rx: fibre::mpsc::BoundedAsyncReceiver<Command>,
    socket_logic: Arc<dyn ISocket>,
    pipe_read_id: usize,
    handle: usize,
    context: Context,
) -> tokio::task::JoinHandle<()>;
```

This could alternatively be inlined in `udp_uring_actor.rs`, but a separate
file keeps the async Tokio code cleanly separated from the sync io_uring code.
