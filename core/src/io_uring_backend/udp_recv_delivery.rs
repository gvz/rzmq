#![cfg(all(feature = "udp", feature = "io-uring"))]

use crate::context::Context;
use crate::runtime::Command;
use crate::socket::ISocket;

use fibre::mpsc::BoundedAsyncReceiver;
use fibre::RecvError;
use std::sync::Arc;

pub(crate) fn spawn_recv_delivery_task(
    recv_rx: BoundedAsyncReceiver<Command>,
    socket_logic: Arc<dyn ISocket>,
    pipe_read_id: usize,
    handle: usize,
    _context: Context,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tracing::debug!(
            "Starting UDP io_uring recv delivery task for pipe_id={}, handle={}",
            pipe_read_id,
            handle
        );

        loop {
            match recv_rx.recv().await {
                Ok(cmd) => {
                    match &cmd {
                        Command::PipeMessageReceived { pipe_id, .. } => {
                            tracing::trace!("Delivering message to socket for pipe_id={}", pipe_id);
                        }
                        Command::PipeClosedByPeer { pipe_id, .. } => {
                            tracing::debug!("Pipe closed by peer for pipe_id={}", pipe_id);
                        }
                        _ => {}
                    }

                    if let Err(e) = socket_logic.handle_pipe_event(pipe_read_id, cmd).await {
                        tracing::error!(
                            handle = handle,
                            pipe_id = pipe_read_id,
                            "handle_pipe_event error: {}",
                            e
                        );
                    }
                }
                Err(RecvError::Disconnected) => {
                    tracing::debug!(
                        "UDP io_uring recv delivery channel closed for handle={}",
                        handle
                    );
                    break;
                }
            }
        }

        tracing::info!(
            "UDP io_uring recv delivery task exiting for handle={}, pipe_id={}",
            handle,
            pipe_read_id
        );
    })
}
