use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::command::ZmtpCommand;
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::{JOIN, LEAVE};
use crate::socket::patterns::IncomingMessageOrchestrator;
use crate::{Blob, delegate_to_core};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Implements the DISH socket pattern (RFC 48).
///
/// DISH is the thread-safe group subscriber. It joins groups via
/// `set_option(JOIN, group_name)` and receives only messages tagged with
/// joined groups by connected RADIO sockets.
#[derive(Debug)]
pub(crate) struct DishSocket {
  core: Arc<SocketCore>,
  joined_groups: RwLock<HashSet<Bytes>>,
  incoming_orchestrator: IncomingMessageOrchestrator<Vec<Msg>>,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl DishSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let orchestrator =
      IncomingMessageOrchestrator::new(core.handle, core.core_state.read().options.rcvhwm);
    Self {
      core,
      joined_groups: RwLock::new(HashSet::new()),
      incoming_orchestrator: orchestrator,
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn get_connection(&self, endpoint_uri: &str) -> Option<Arc<dyn ISocketConnection>> {
    self
      .core_state_read()
      .endpoints
      .get(endpoint_uri)
      .map(|ep| ep.connection_iface.clone())
  }

  async fn send_group_command_to_peer(
    &self,
    conn: &Arc<dyn ISocketConnection>,
    uri: &str,
    is_join: bool,
    group: &[u8],
  ) {
    let cmd_msg = if is_join {
      ZmtpCommand::create_join(group)
    } else {
      ZmtpCommand::create_leave(group)
    };
    if let Err(e) = conn.send_message(cmd_msg).await {
      tracing::warn!(
          handle = self.core.handle,
          uri,
          is_join,
          group = ?String::from_utf8_lossy(group),
          error = %e,
          "DISH: failed to send group command to peer"
      );
    }
  }

  async fn send_group_command_to_all(&self, is_join: bool, group: &[u8]) {
    let peer_uris: Vec<String> = self
      .pipe_read_to_endpoint_uri
      .read()
      .values()
      .cloned()
      .collect();

    for uri in peer_uris {
      if let Some(conn) = self.get_connection(&uri) {
        self
          .send_group_command_to_peer(&conn, &uri, is_join, group)
          .await;
      }
    }
  }

  fn decode_radio_frame(frame: &[u8]) -> Option<(&[u8], &[u8])> {
    if frame.is_empty() {
      return None;
    }
    let group_len = frame[0] as usize;
    if 1 + group_len > frame.len() {
      return None;
    }
    let group = &frame[1..1 + group_len];
    let payload = &frame[1 + group_len..];
    Some((group, payload))
  }

  fn validate_group(group: &[u8]) -> Result<(), ZmqError> {
    if group.is_empty() || group.len() > 255 {
      return Err(ZmqError::InvalidArgument(
        "Group length must be 1-255 bytes".into(),
      ));
    }
    if group.iter().any(|&b| b == 0) {
      return Err(ZmqError::InvalidArgument(
        "Group bytes must be in range 1-255 (NUL is forbidden)".into(),
      ));
    }
    Ok(())
  }
}

#[async_trait]
impl ISocket for DishSocket {
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
    match option {
      JOIN | LEAVE => self.set_pattern_option(option, value).await,
      _ => delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec()),
    }
  }
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState(
      "DISH sockets cannot send messages".into(),
    ))
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = self.core_state_read().options.rcvtimeo;
    let transform_fn = |q_item: Vec<Msg>| q_item;
    self
      .incoming_orchestrator
      .recv_message(rcvtimeo_opt, transform_fn)
      .await
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::InvalidState(
      "DISH sockets do not support multipart messages (RFC 48 thread-safety requirement)".into(),
    ))
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    Err(ZmqError::InvalidState(
      "DISH sockets do not support multipart messages (RFC 48 thread-safety requirement)".into(),
    ))
  }

  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      JOIN => {
        Self::validate_group(value)?;
        let group_bytes = Bytes::copy_from_slice(value);
        tracing::debug!(
            handle = self.core.handle,
            group = ?String::from_utf8_lossy(value),
            "DISH joining group"
        );
        self.joined_groups.write().insert(group_bytes);
        self.send_group_command_to_all(true, value).await;
        Ok(())
      }
      LEAVE => {
        Self::validate_group(value)?;
        tracing::debug!(
            handle = self.core.handle,
            group = ?String::from_utf8_lossy(value),
            "DISH leaving group"
        );
        let group_bytes = Bytes::copy_from_slice(value);
        self.joined_groups.write().remove(&group_bytes);
        self.send_group_command_to_all(false, value).await;
        Ok(())
      }
      _ => Err(ZmqError::UnsupportedOption(option)),
    }
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
            Some(_) => {
              tracing::trace!(
                handle = self.core.handle,
                pipe_id,
                "DISH received command from RADIO, ignoring"
              );
            }
            None => {
              tracing::trace!(
                handle = self.core.handle,
                pipe_id,
                "DISH received unparseable command, ignoring"
              );
            }
          }
          return Ok(());
        }

        let frame = msg.data().unwrap_or(&[]);
        if frame.is_empty() {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id,
            "DISH received empty frame, dropping"
          );
          return Ok(());
        }

        let (group_bytes, payload_bytes) = match Self::decode_radio_frame(frame) {
          Some((g, p)) => (g, p),
          None => {
            tracing::warn!(
              handle = self.core.handle,
              pipe_id,
              "DISH received malformed RADIO frame, dropping"
            );
            return Ok(());
          }
        };

        if !self.joined_groups.read().contains(group_bytes) {
          tracing::trace!(
              handle = self.core.handle,
              pipe_id,
              group = ?String::from_utf8_lossy(group_bytes),
              "DISH dropping message for non-joined group"
          );
          return Ok(());
        }

        let mut decoded_msg = Msg::from_bytes(Bytes::copy_from_slice(payload_bytes));
        decoded_msg
          .set_group(Bytes::copy_from_slice(group_bytes))
          .unwrap();
        self
          .incoming_orchestrator
          .queue_item(pipe_id, vec![decoded_msg])
          .await?;
        Ok(())
      }
      Command::PipeClosedByPeer { .. } => {
        self.incoming_orchestrator.clear_pipe_state(pipe_id).await;
        Ok(())
      }
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
      .core_state_read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(
          handle = self.core.handle,
          pipe_read_id,
          uri = %endpoint_uri,
          "DISH attaching connection"
      );
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());

      let current_groups: Vec<Bytes> = self.joined_groups.read().iter().cloned().collect();
      if !current_groups.is_empty() {
        if let Some(conn) = self.get_connection(&endpoint_uri) {
          for group in current_groups {
            self
              .send_group_command_to_peer(&conn, &endpoint_uri, true, &group)
              .await;
          }
        } else {
          tracing::warn!(
              handle = self.core.handle,
              uri = %endpoint_uri,
              "DISH pipe_attached: Connection interface not found for URI. Skipping group sync."
          );
        }
      }
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "DISH pipe_attached: Could not find endpoint_uri for pipe_read_id. Cannot update map or send initial groups."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "DISH",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but DISH socket does not use peer identities. Ignoring."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "DISH detaching pipe"
    );

    let removed_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);
    if let Some(uri) = removed_uri {
      tracing::trace!(
          handle = self.core.handle,
          pipe_read_id,
          uri = %uri,
          "DISH removed endpoint_uri mapping for detached pipe"
      );
    }

    self
      .incoming_orchestrator
      .clear_pipe_state(pipe_read_id)
      .await;
  }
}
