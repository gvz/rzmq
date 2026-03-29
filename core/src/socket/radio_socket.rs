use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::command::ZmtpCommand;
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::patterns::Distributor;
use crate::{Blob, delegate_to_core};

use async_trait::async_trait;
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

  fn encode_radio_frame(group: &[u8], payload: Option<&[u8]>) -> Msg {
    let payload = payload.unwrap_or(&[]);
    let mut frame = Vec::with_capacity(1 + group.len() + payload.len());
    frame.push(group.len() as u8);
    frame.extend_from_slice(group);
    frame.extend_from_slice(payload);
    Msg::from_vec(frame)
  }
}

#[async_trait]
impl ISocket for RadioSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }

  fn mailbox(&self) -> MailboxSender {
    self.core.command_sender()
  }

  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserBind, endpoint: endpoint.to_string())
  }
  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserConnect, endpoint: endpoint.to_string())
  }
  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserDisconnect, endpoint: endpoint.to_string())
  }
  async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserUnbind, endpoint: endpoint.to_string())
  }
  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
  }
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let group = msg
      .group()
      .ok_or_else(|| ZmqError::InvalidState("Message has no group".into()))?;

    if group.is_empty() || group.len() > 255 {
      return Err(ZmqError::InvalidState(
        "Group length must be 1-255 bytes".into(),
      ));
    }

    let payload = msg.data();
    let wire_msg = Self::encode_radio_frame(group, payload);

    tracing::debug!(
        handle = self.core.handle,
        group = ?String::from_utf8_lossy(group),
        msg_size = msg.size(),
        "RadioSocket::send broadcasting to all peers"
    );

    match self
      .distributor
      .send_to_all(&wire_msg, self.core.handle, &self.core.core_state)
      .await
    {
      Ok(()) => Ok(()),
      Err(failed_uris_with_errors) => {
        for (uri, error_detail) in failed_uris_with_errors {
          tracing::debug!(
              handle = self.core.handle,
              uri = %uri,
              error = %error_detail,
              "RADIO removing disconnected/errored peer URI found during send"
          );
          self.distributor.remove_peer_uri(&uri);
        }
        Ok(())
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    Err(ZmqError::InvalidState(
      "RADIO sockets cannot receive messages".into(),
    ))
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState(
      "RADIO sockets do not support multipart messages (RFC 48 thread-safety requirement)".into(),
    ))
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    Err(ZmqError::UnsupportedFeature(
      "RADIO sockets cannot receive messages".into(),
    ))
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        if msg.is_command() {
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
          tracing::trace!(
            handle = self.core.handle,
            pipe_id,
            "RADIO discarding data message received from DISH"
          );
        }
        Ok(())
      }
      Command::PipeClosedByPeer { .. } => Ok(()),
      _ => Ok(()),
    }
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    let endpoint_uri_option = self
      .core
      .core_state
      .read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "RADIO attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());
      self.distributor.add_peer_uri(endpoint_uri);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "RADIO pipe_attached: Could not find endpoint_uri for pipe_read_id. Distributor not updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "RADIO",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but RADIO socket does not use peer identities. Ignoring."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "RADIO detaching connection"
    );

    let maybe_endpoint_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);

    if let Some(endpoint_uri) = maybe_endpoint_uri {
      self.distributor.remove_peer_uri(&endpoint_uri);
      tracing::trace!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "RADIO removed detached connection from distributor");
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "RADIO detach: Endpoint URI not found for read ID in local map. Distributor may not be updated."
      );
    }
  }
}
