#![cfg(all(feature = "udp", feature = "io-uring"))]

use crate::context::Context;
use crate::error::ZmqError;
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::message::Msg;
use crate::runtime::Command;
use crate::socket::ISocket;

use bytes::Bytes;

use io_uring::opcode::{RecvMsg, SendMsg, SendMsgZc};
use io_uring::squeue::Flags as SqeFlags;
use io_uring::types::Fd;
use io_uring::{cqueue, IoUring, squeue};

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, IntoRawFd, RawFd};

use fibre::mpsc;
use std::sync::Arc;

const IORING_CQE_F_MORE: u32 = 1;
const IORING_CQE_F_BUFFER: u32 = 1 << 16;
const IORING_CQE_F_NOTIF: u32 = 1 << 3;
const CQE_BUFFER_SHIFT: u32 = 16;

pub(crate) struct UdpUringActor {
    ring: IoUring,
    state: UdpUringActorState,

    socket_fd: RawFd,

    recv_buffer_manager: Option<BufferRingManager>,
    recv_bgid: Option<u16>,
    recv_buffer_count: u16,
    recv_buffer_size: usize,
    multishot_recvmsg_ud: Option<u64>,
    recv_msghdr: Option<Box<RecvMsgContext>>,

    send_rx: Option<mpsc::BoundedReceiver<UdpSendRequest>>,
    send_zerocopy_enabled: bool,
    pending_zc_sends: u32,

    event_fd: eventfd::EventFD,
    eventfd_poll_ud: u64,

    control_rx: Option<mpsc::BoundedReceiver<UdpUringCommand>>,

    socket_logic: Option<Arc<dyn ISocket>>,
    recv_delivery_tx: Option<mpsc::BoundedSender<Command>>,
    pipe_read_id: usize,

    next_user_data: u64,
    pending_ops: HashMap<u64, UdpUringOpType>,
    pending_send_contexts: HashMap<u64, Box<SendMsgContext>>,

    endpoint_uri: String,
    send_addr: Option<SocketAddr>,
    context: Context,
    handle: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpUringActorState {
    Initializing,
    Running,
    Draining,
    Stopped,
}

pub(crate) enum UdpUringCommand {
    Stop,
}

pub(crate) struct UdpSendRequest {
    pub data: Bytes,
    pub target_addr: SocketAddr,
}

impl std::fmt::Debug for UdpSendRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpSendRequest")
            .field("data_len", &self.data.len())
            .field("target_addr", &self.target_addr)
            .finish()
    }
}

enum UdpUringOpType {
    RecvMsgMultishot,
    RecvMsg,
    SendMsg,
    SendMsgZc { notify_pending: bool },
    EventFdPoll,
    AsyncCancel { target_ud: u64 },
}

struct RecvMsgContext {
    addr_storage: libc::sockaddr_storage,
    addr_len: libc::socklen_t,
    msghdr: libc::msghdr,
}

struct SendMsgContext {
    data: Bytes,
    iovec: libc::iovec,
    addr_storage: libc::sockaddr_storage,
    addr_len: libc::socklen_t,
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

        ctx.iovec.iov_base = ctx.data.as_ptr() as *mut libc::c_void;
        ctx.iovec.iov_len = ctx.data.len();

        ctx.addr_len = socket_addr_to_sockaddr_storage(&target_addr, &mut ctx.addr_storage);

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
        self.msghdr.msg_name = &mut self.addr_storage as *mut _ as *mut libc::c_void;
        self.msghdr.msg_namelen = self.addr_len;
        self.msghdr.msg_iov = &mut self.iovec as *mut libc::iovec;
        self.msghdr.msg_iovlen = 1;
        self.iovec.iov_base = self.data.as_ptr() as *mut libc::c_void;
        self.iovec.iov_len = self.data.len();

        &self.msghdr as *const libc::msghdr
    }
}

pub(crate) struct UdpUringActorConfig {
    pub handle: usize,
    pub socket_fd: RawFd,
    pub endpoint_uri: String,
    pub ring_entries: u32,
    pub recv_buffer_count: usize,
    pub recv_buffer_size: usize,
    pub pipe_read_id: usize,
    pub socket_logic: Option<Arc<dyn ISocket>>,
    pub send_addr: Option<SocketAddr>,
    pub send_zerocopy: bool,
    pub send_channel_capacity: usize,
    pub context: Context,
    pub recv_delivery_tx: Option<mpsc::BoundedSender<Command>>,
}

pub(crate) struct UdpUringActorHandle {
    pub control_tx: mpsc::BoundedSender<UdpUringCommand>,
    pub send_tx: Option<mpsc::BoundedSender<UdpSendRequest>>,
    pub event_fd: eventfd::EventFD,
    pub join_handle: std::thread::JoinHandle<Result<(), ZmqError>>,
}

impl std::fmt::Debug for UdpUringActorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpUringActorHandle")
            .field("control_tx", &"...")
            .field("send_tx", &self.send_tx.as_ref().map(|_| "..."))
            .field("event_fd", &self.event_fd.as_raw_fd())
            .finish()
    }
}

impl UdpUringActor {
    pub(crate) fn spawn(
        config: UdpUringActorConfig,
    ) -> Result<UdpUringActorHandle, ZmqError> {
        let (control_tx, control_rx) = mpsc::bounded(16);
        let (send_tx, send_rx) = mpsc::bounded(config.send_channel_capacity.max(1));

        let event_fd = eventfd::EventFD::new(
            0,
            eventfd::EfdFlags::EFD_CLOEXEC | eventfd::EfdFlags::EFD_NONBLOCK,
        )?;

        let event_fd_clone = event_fd.clone();
        let handle = config.handle;
        let join_handle = std::thread::Builder::new()
            .name(format!("rzmq-udp-uring-{}", handle))
            .spawn(move || {
                let ring = IoUring::new(config.ring_entries)
                    .map_err(|e| ZmqError::Internal(format!("IoUring init: {}", e)))?;

                let mut actor = UdpUringActor {
                    ring,
                    state: UdpUringActorState::Initializing,
                    socket_fd: config.socket_fd,
                    recv_buffer_manager: None,
                    recv_bgid: None,
                    recv_buffer_count: config.recv_buffer_count as u16,
                    recv_buffer_size: config.recv_buffer_size,
                    multishot_recvmsg_ud: None,
                    recv_msghdr: None,
                    send_rx: Some(send_rx),
                    send_zerocopy_enabled: config.send_zerocopy,
                    pending_zc_sends: 0,
                    event_fd: event_fd_clone,
                    eventfd_poll_ud: 0,
                    control_rx: Some(control_rx),
                    socket_logic: config.socket_logic,
                    recv_delivery_tx: config.recv_delivery_tx,
                    pipe_read_id: config.pipe_read_id,
                    next_user_data: 1,
                    pending_ops: HashMap::new(),
                    pending_send_contexts: HashMap::new(),
                    endpoint_uri: config.endpoint_uri,
                    send_addr: config.send_addr,
                    context: config.context,
                    handle,
                };

                if let Err(e) = actor.initialize() {
                    tracing::error!("UdpUringActor initialization failed: {}", e);
                    return Err(e);
                }

                actor.run_loop()
            })?;

        Ok(UdpUringActorHandle {
            control_tx,
            send_tx: Some(send_tx),
            event_fd,
            join_handle,
        })
    }

    fn initialize(&mut self) -> Result<(), ZmqError> {
        if self.pipe_read_id > 0 {
            self.initialize_recv_buffers()?;
        }
        self.submit_eventfd_poll()?;
        
        if self.pipe_read_id > 0 {
            self.submit_multishot_recvmsg()?;
        }

        self.state = UdpUringActorState::Running;
        tracing::info!("UdpUringActor initialized for fd={}", self.socket_fd);
        Ok(())
    }

    fn initialize_recv_buffers(&mut self) -> Result<(), ZmqError> {
        let bgid = 0u16;
        let buf_count = self.recv_buffer_count;
        let buf_size = self.recv_buffer_size;

        let buf_mgr = BufferRingManager::new(&self.ring, buf_count, bgid, buf_size)?;

        tracing::info!(
            "UdpUringActor: Recv buffer ring initialized (bgid={}, count={}, size={})",
            bgid, buf_count, buf_size
        );

        self.recv_buffer_manager = Some(buf_mgr);
        self.recv_bgid = Some(bgid);

        let mut ctx = Box::new(RecvMsgContext {
            addr_storage: unsafe { std::mem::zeroed() },
            addr_len: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            msghdr: unsafe { std::mem::zeroed() },
        });

        ctx.msghdr.msg_name = &mut ctx.addr_storage as *mut _ as *mut libc::c_void;
        ctx.msghdr.msg_namelen = ctx.addr_len;
        ctx.msghdr.msg_iov = std::ptr::null_mut();
        ctx.msghdr.msg_iovlen = 0;
        ctx.msghdr.msg_control = std::ptr::null_mut();
        ctx.msghdr.msg_controllen = 0;
        ctx.msghdr.msg_flags = 0;

        self.recv_msghdr = Some(ctx);

        Ok(())
    }

    fn submit_eventfd_poll(&mut self) -> Result<(), ZmqError> {
        let ud = self.next_ud();
        self.eventfd_poll_ud = ud;

        let sqe = io_uring::opcode::PollAdd::new(Fd(self.event_fd.as_raw_fd()), libc::POLLIN as u32)
            .build()
            .user_data(ud);

        self.pending_ops.insert(ud, UdpUringOpType::EventFdPoll);

        let mut sq = unsafe { self.ring.submission_shared() };
        unsafe {
            sq.push(&sqe).map_err(|_| ZmqError::ResourceLimitReached)?;
        }

        Ok(())
    }

    fn submit_multishot_recvmsg(&mut self) -> Result<(), ZmqError> {
        let bgid = self.recv_bgid
            .ok_or(ZmqError::InvalidState("No buffer ring configured"))?;

        let ud = self.next_ud();

        let ctx = self.recv_msghdr.as_mut()
            .ok_or(ZmqError::InvalidState("RecvMsgContext not initialized"))?;

        ctx.addr_storage = unsafe { std::mem::zeroed() };
        ctx.addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        ctx.msghdr.msg_name = &mut ctx.addr_storage as *mut _ as *mut libc::c_void;
        ctx.msghdr.msg_namelen = ctx.addr_len;
        ctx.msghdr.msg_iov = std::ptr::null_mut();
        ctx.msghdr.msg_iovlen = 0;

        let mut sqe = RecvMsg::new(
            Fd(self.socket_fd),
            &mut ctx.msghdr as *mut libc::msghdr,
        )
        .buf_group(bgid)
        .build()
        .flags(SqeFlags::BUFFER_SELECT)
        .user_data(ud);

        self.pending_ops.insert(ud, UdpUringOpType::RecvMsgMultishot);
        self.multishot_recvmsg_ud = Some(ud);

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

    fn submit_singleshot_recvmsg(&mut self) -> Result<(), ZmqError> {
        let bgid = self.recv_bgid
            .ok_or(ZmqError::InvalidState("No buffer ring configured"))?;

        let ud = self.next_ud();

        let ctx = self.recv_msghdr.as_mut()
            .ok_or(ZmqError::InvalidState("RecvMsgContext not initialized"))?;

        ctx.addr_storage = unsafe { std::mem::zeroed() };
        ctx.addr_len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        ctx.msghdr.msg_name = &mut ctx.addr_storage as *mut _ as *mut libc::c_void;
        ctx.msghdr.msg_namelen = ctx.addr_len;
        ctx.msghdr.msg_iov = std::ptr::null_mut();
        ctx.msghdr.msg_iovlen = 0;

        let sqe = RecvMsg::new(
            Fd(self.socket_fd),
            &mut ctx.msghdr as *mut libc::msghdr,
        )
        .buf_group(bgid)
        .build()
        .flags(SqeFlags::BUFFER_SELECT)
        .user_data(ud);

        self.pending_ops.insert(ud, UdpUringOpType::RecvMsg);

        let mut sq = unsafe { self.ring.submission_shared() };
        unsafe {
            sq.push(&sqe).map_err(|_| ZmqError::ResourceLimitReached)?;
        }

        tracing::debug!(
            "UdpUringActor: Submitted single-shot recvmsg SQE, fd={}, bgid={}, ud={}",
            self.socket_fd, bgid, ud
        );

        Ok(())
    }

    fn run_loop(&mut self) -> Result<(), ZmqError> {
        loop {
            match self.state {
                UdpUringActorState::Running => {
                    let work_available = self.gather_work();
                    
                    {
                        let mut sq = unsafe { self.ring.submission_shared() };
                        if !sq.is_empty() {
                            drop(sq);
                            self.ring.submit().map_err(|e| {
                                ZmqError::Internal(format!("io_uring submit failed: {}", e))
                            })?;
                        }
                    }

                    if !work_available && self.pending_ops.is_empty() {
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }

                    self.process_cqes();
                }
                UdpUringActorState::Draining => {
                    {
                        let mut sq = unsafe { self.ring.submission_shared() };
                        if !sq.is_empty() {
                            drop(sq);
                            self.ring.submit().map_err(|e| {
                                ZmqError::Internal(format!("io_uring submit failed: {}", e))
                            })?;
                        }
                    }

                    self.process_cqes();

                    if self.pending_ops.is_empty() {
                        self.state = UdpUringActorState::Stopped;
                        break;
                    }
                    
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
                UdpUringActorState::Stopped => break,
                UdpUringActorState::Initializing => {
                    return Err(ZmqError::InvalidState("Actor in Initializing state in run_loop"));
                }
            }
        }

        tracing::info!("UdpUringActor stopped for fd={}", self.socket_fd);
        Ok(())
    }

    fn gather_work(&mut self) -> bool {
        let mut work_available = false;

        if let Some(ref mut rx) = self.control_rx {
            while let Ok(cmd) = rx.try_recv() {
                match cmd {
                    UdpUringCommand::Stop => {
                        self.transition_to_draining();
                        return false;
                    }
                }
            }
        }

        if let Some(ref send_rx) = self.send_rx {
            let mut batch_count = 0;
            let mut requests = Vec::new();
            while let Ok(req) = send_rx.try_recv() {
                requests.push(req);
                batch_count += 1;
                work_available = true;
                if batch_count >= 64 {
                    break;
                }
            }
            for req in requests {
                if self.send_zerocopy_enabled {
                    self.queue_sendmsg_zc(req);
                } else {
                    self.queue_sendmsg(req);
                }
            }
        }

        if self.recv_buffer_manager.is_some() && self.multishot_recvmsg_ud.is_none() {
            match self.submit_multishot_recvmsg() {
                Ok(()) => work_available = true,
                Err(e) => {
                    tracing::warn!("Multishot recvmsg failed (falling back to single-shot): {}", e);
                    if let Err(e2) = self.submit_singleshot_recvmsg() {
                        tracing::error!("Failed to submit single-shot recvmsg: {}", e2);
                    } else {
                        work_available = true;
                    }
                }
            }
        }

        work_available
    }

    fn process_cqes(&mut self) {
        let cqe_list = {
            let mut cq = unsafe { self.ring.completion_shared() };
            cq.sync();

            let mut list = Vec::new();
            for cqe in &mut cq {
                list.push((cqe.user_data(), cqe.result(), cqe.flags()));
            }
            list
        };

        for (ud, result, flags) in cqe_list {
            if ud == self.eventfd_poll_ud {
                self.handle_eventfd_cqe(result);
                continue;
            }

            match self.pending_ops.get(&ud) {
                Some(UdpUringOpType::RecvMsgMultishot) => {
                    let has_more = (flags & IORING_CQE_F_MORE) != 0;
                    if result >= 0 {
                        self.handle_recv_cqe(result as usize, flags);
                    } else if result == -libc::ECANCELED {
                        tracing::debug!("Multishot recvmsg cancelled");
                    } else if result == -libc::EOPNOTSUPP || result == -libc::EINVAL {
                        tracing::warn!("Multishot not supported (kernel < 6.0?), falling back to single-shot");
                    } else {
                        tracing::error!("recvmsg error: {}", io::Error::from_raw_os_error(-result));
                    }
                    if !has_more || result < 0 {
                        self.multishot_recvmsg_ud = None;
                    }
                }
                Some(UdpUringOpType::RecvMsg) => {
                    if result >= 0 {
                        self.handle_recv_cqe(result as usize, flags);
                    } else if result != -libc::ECANCELED {
                        tracing::error!("recvmsg error: {}", io::Error::from_raw_os_error(-result));
                    }
                    self.multishot_recvmsg_ud = None;
                }
                Some(UdpUringOpType::SendMsg) => {
                    self.pending_ops.remove(&ud);
                    let _ctx = self.pending_send_contexts.remove(&ud);
                    if result < 0 {
                        tracing::error!("sendmsg failed: {}", io::Error::from_raw_os_error(-result));
                    } else {
                        tracing::trace!("sendmsg complete: {} bytes sent", result);
                    }
                }
                Some(UdpUringOpType::SendMsgZc { notify_pending }) => {
                    if (flags & IORING_CQE_F_NOTIF) != 0 {
                        self.pending_ops.remove(&ud);
                        let _ctx = self.pending_send_contexts.remove(&ud);
                        self.pending_zc_sends = self.pending_zc_sends.saturating_sub(1);
                        tracing::trace!("sendmsg_zc notify received, ud={}, buffer freed", ud);
                    } else {
                        if result < 0 {
                            tracing::error!("sendmsg_zc failed: {}", io::Error::from_raw_os_error(-result));
                        } else {
                            tracing::trace!("sendmsg_zc submitted: {} bytes, waiting for notify", result);
                        }
                        if let Some(op) = self.pending_ops.get_mut(&ud) {
                            *op = UdpUringOpType::SendMsgZc { notify_pending: true };
                        }
                    }
                }
                Some(UdpUringOpType::EventFdPoll) => {
                    if result < 0 {
                        tracing::warn!("eventfd poll error: {}", io::Error::from_raw_os_error(-result));
                    }
                }
                Some(UdpUringOpType::AsyncCancel { .. }) => {
                    self.pending_ops.remove(&ud);
                    tracing::debug!("Async cancel completed for ud={}", ud);
                }
                None => {
                    tracing::warn!("Unknown CQE ud={}, result={}", ud, result);
                }
            }
        }
    }

    fn handle_recv_cqe(&mut self, bytes_received: usize, cqe_flags: u32) {
        let buffer_id = (cqe_flags >> CQE_BUFFER_SHIFT) as u16;

        let buf_mgr = match &self.recv_buffer_manager {
            Some(bm) => bm,
            None => return,
        };

        let borrowed_buf = match unsafe { buf_mgr.borrow_kernel_filled_buffer(buffer_id, bytes_received) } {
            Ok(buf) => buf,
            Err(e) => {
                tracing::error!("Failed to borrow recv buffer {}: {}", buffer_id, e);
                return;
            }
        };

        let buf_slice = &borrowed_buf[..bytes_received];

        if bytes_received < 16 {
            tracing::warn!("recvmsg CQE too small for header: {} bytes", bytes_received);
            return;
        }

        let namelen = u32::from_ne_bytes(buf_slice[0..4].try_into().unwrap()) as usize;
        let controllen = u32::from_ne_bytes(buf_slice[4..8].try_into().unwrap()) as usize;
        let payloadlen = u32::from_ne_bytes(buf_slice[8..12].try_into().unwrap()) as usize;

        let header_size = 16;
        let name_end = header_size + namelen;
        let name_padded = (name_end + 3) & !3;
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

        let peer_addr = self.extract_peer_addr_from_buf(buf_slice, namelen);
        if let Some(peer_addr) = peer_addr {
            let peer_addr_str = format!("udp://{}", peer_addr);
            self.notify_peer_discovered(peer_addr_str);
        }

        let msg = Msg::from_vec(datagram.to_vec());
        let cmd = Command::PipeMessageReceived {
            pipe_id: self.pipe_read_id,
            msg,
        };

        self.deliver_to_socket(cmd);
    }

    fn deliver_to_socket(&self, cmd: Command) {
        if let Some(ref tx) = self.recv_delivery_tx {
            if let Err(e) = tx.try_send(cmd) {
                tracing::warn!("Failed to deliver datagram to socket channel: {}", e);
            }
        } else if let Some(ref socket_logic) = self.socket_logic {
            let socket_logic = socket_logic.clone();
            let handle = self.handle;
            let pipe_read_id = self.pipe_read_id;
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    if let Err(e) = socket_logic.handle_pipe_event(pipe_read_id, cmd).await {
                        tracing::error!(handle = handle, "handle_pipe_event error: {}", e);
                    }
                });
            });
        }
    }

    fn extract_peer_addr_from_buf(&self, buf_slice: &[u8], namelen: usize) -> Option<SocketAddr> {
        if namelen == 0 || namelen > buf_slice.len().saturating_sub(16) {
            return None;
        }

        let name_start = 16;
        let name_end = name_start + namelen;
        if name_end > buf_slice.len() {
            return None;
        }

        let name_data = &buf_slice[name_start..name_end];
        
        let sockaddr: &libc::sockaddr = unsafe { &*(name_data.as_ptr() as *const libc::sockaddr) };
        
        match sockaddr.sa_family as libc::c_int {
            libc::AF_INET => {
                if namelen >= std::mem::size_of::<libc::sockaddr_in>() {
                    let addr_in: &libc::sockaddr_in = unsafe { &*(sockaddr as *const _ as *const libc::sockaddr_in) };
                    let port = u16::from_be(addr_in.sin_port);
                    let ip_bytes = addr_in.sin_addr.s_addr.to_ne_bytes();
                    let ip = std::net::Ipv4Addr::new(ip_bytes[0], ip_bytes[1], ip_bytes[2], ip_bytes[3]);
                    Some(SocketAddr::new(std::net::IpAddr::V4(ip), port))
                } else {
                    None
                }
            }
            libc::AF_INET6 => {
                if namelen >= std::mem::size_of::<libc::sockaddr_in6>() {
                    let addr_in6: &libc::sockaddr_in6 = unsafe { &*(sockaddr as *const _ as *const libc::sockaddr_in6) };
                    let port = u16::from_be(addr_in6.sin6_port);
                    let ip_bytes = addr_in6.sin6_addr.s6_addr;
                    let ip = std::net::Ipv6Addr::new(
                        u16::from_be_bytes([ip_bytes[0], ip_bytes[1]]),
                        u16::from_be_bytes([ip_bytes[2], ip_bytes[3]]),
                        u16::from_be_bytes([ip_bytes[4], ip_bytes[5]]),
                        u16::from_be_bytes([ip_bytes[6], ip_bytes[7]]),
                        u16::from_be_bytes([ip_bytes[8], ip_bytes[9]]),
                        u16::from_be_bytes([ip_bytes[10], ip_bytes[11]]),
                        u16::from_be_bytes([ip_bytes[12], ip_bytes[13]]),
                        u16::from_be_bytes([ip_bytes[14], ip_bytes[15]]),
                    );
                    Some(SocketAddr::new(std::net::IpAddr::V6(ip), port))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn notify_peer_discovered(&self, peer_addr: String) {
        if let Some(ref socket_logic) = self.socket_logic {
            let socket_logic = socket_logic.clone();
            let pipe_read_id = self.pipe_read_id;
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    socket_logic.handle_udp_peer_discovered(pipe_read_id, peer_addr).await;
                });
            });
        }
    }

    fn handle_eventfd_cqe(&mut self, result: i32) {
        if result < 0 {
            tracing::warn!("eventfd poll error: {}", io::Error::from_raw_os_error(-result));
            return;
        }

        if let Ok(_) = self.event_fd.read() {
            tracing::trace!("UdpUringActor: eventfd wakeup received");
        }

        let ud = self.eventfd_poll_ud;
        let sqe = io_uring::opcode::PollAdd::new(Fd(self.event_fd.as_raw_fd()), libc::POLLIN as u32)
            .build()
            .user_data(ud);

        let mut sq = unsafe { self.ring.submission_shared() };
        if let Ok(_) = unsafe { sq.push(&sqe) } {
            self.pending_ops.insert(ud, UdpUringOpType::EventFdPoll);
        }
    }

    fn queue_sendmsg(&mut self, req: UdpSendRequest) {
        let data_len = req.data.len();
        let mut ctx = Box::new(SendMsgContext::new(req.data, req.target_addr));
        let ud = self.next_ud();

        let sqe = SendMsg::new(
            Fd(self.socket_fd),
            ctx.msghdr_ptr(),
        )
        .build()
        .user_data(ud);

        self.pending_ops.insert(ud, UdpUringOpType::SendMsg);
        self.pending_send_contexts.insert(ud, ctx);

        let mut sq = unsafe { self.ring.submission_shared() };
        match unsafe { sq.push(&sqe) } {
            Ok(()) => {
                tracing::trace!("Queued sendmsg SQE, ud={}, {} bytes", ud, data_len);
            }
            Err(_) => {
                self.pending_ops.remove(&ud);
                self.pending_send_contexts.remove(&ud);
                tracing::warn!("SQ full, dropping sendmsg");
            }
        }
    }

    fn queue_sendmsg_zc(&mut self, req: UdpSendRequest) {
        let data_len = req.data.len();
        let mut ctx = Box::new(SendMsgContext::new(req.data, req.target_addr));
        let ud = self.next_ud();

        let sqe = SendMsgZc::new(
            Fd(self.socket_fd),
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
                tracing::trace!("Queued sendmsg_zc SQE, ud={}, {} bytes", ud, data_len);
            }
            Err(_) => {
                self.pending_ops.remove(&ud);
                self.pending_send_contexts.remove(&ud);
                self.pending_zc_sends = self.pending_zc_sends.saturating_sub(1);
                tracing::warn!("SQ full, dropping sendmsg_zc");
            }
        }
    }

    fn next_ud(&mut self) -> u64 {
        let ud = self.next_user_data;
        self.next_user_data = self.next_user_data.wrapping_add(1);
        ud
    }

    fn transition_to_draining(&mut self) {
        tracing::info!("UdpUringActor: transitioning to Draining state");
        self.state = UdpUringActorState::Draining;
        
        if let Some(ud) = self.multishot_recvmsg_ud {
            let cancel_sqe = io_uring::opcode::AsyncCancel::new(ud)
                .build()
                .user_data(self.next_ud());
            
            let mut sq = unsafe { self.ring.submission_shared() };
            let _ = unsafe { sq.push(&cancel_sqe) };
        }
    }
}

fn socket_addr_to_sockaddr_storage(addr: &SocketAddr, storage: &mut libc::sockaddr_storage) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            let sin = libc::sockaddr_in {
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                },
                sin_zero: [0; 8],
            };
            unsafe {
                std::ptr::copy_nonoverlapping(&sin, storage as *mut _ as *mut libc::sockaddr_in, 1);
            }
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let sin6 = libc::sockaddr_in6 {
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            };
            unsafe {
                std::ptr::copy_nonoverlapping(&sin6, storage as *mut _ as *mut libc::sockaddr_in6, 1);
            }
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}
