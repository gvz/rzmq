#![cfg(all(feature = "udp", feature = "io-uring"))]

use crate::error::ZmqError;
use crate::message::Msg;
use crate::socket::connection_iface::ISocketConnection;

use bytes::Bytes;

use std::any::Any;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use fibre::mpsc;
use fibre::TrySendError;

pub(crate) struct UdpUringSendConnection {
    send_tx: mpsc::BoundedSender<crate::io_uring_backend::udp_uring_actor::UdpSendRequest>,
    event_fd: eventfd::EventFD,
    send_addr: SocketAddr,
    connection_id: usize,
}

impl UdpUringSendConnection {
    pub(crate) fn new(
        send_tx: mpsc::BoundedSender<crate::io_uring_backend::udp_uring_actor::UdpSendRequest>,
        event_fd: eventfd::EventFD,
        send_addr: SocketAddr,
        connection_id: usize,
    ) -> Self {
        Self {
            send_tx,
            event_fd,
            send_addr,
            connection_id,
        }
    }
}

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

        let req = crate::io_uring_backend::udp_uring_actor::UdpSendRequest {
            data: Bytes::copy_from_slice(data),
            target_addr: self.send_addr,
        };

        match self.send_tx.try_send(req) {
            Ok(()) => {
                let _ = self.event_fd.write(1u64);
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                tracing::warn!("UDP io_uring send channel full, dropping datagram");
                Err(ZmqError::ResourceLimitReached)
            }
            Err(TrySendError::Closed(_)) => {
                Err(ZmqError::ConnectionClosed)
            }
            _ => unreachable!()
        }
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
        Ok(())
    }

    fn get_connection_id(&self) -> usize {
        self.connection_id
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl std::fmt::Debug for UdpUringSendConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpUringSendConnection")
            .field("send_addr", &self.send_addr)
            .field("connection_id", &self.connection_id)
            .finish()
    }
}
