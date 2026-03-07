use crate::ZmqError;
use crate::message::flags::MsgFlags;
use crate::message::metadata::Metadata;
use bytes::Bytes;
use std::fmt;

/// Represents a single message part (frame).
#[derive(Clone, Default)]
pub struct Msg {
  // Use Bytes for efficient slicing and cloning (reference counted)
  data: Option<Bytes>,
  flags: MsgFlags,
  metadata: Metadata,   // Cloning Metadata is cheap (Arc)
  group: Option<Bytes>, // Group for RADIO-DISH pattern
}

impl Msg {
  /// Creates an empty message with no data.
  pub fn new() -> Self {
    Self::default()
  }

  /// Creates a message from a `Vec<u8>`, taking ownership.
  pub fn from_vec(data: Vec<u8>) -> Self {
    Self {
      data: Some(Bytes::from(data)),
      ..Default::default()
    }
  }

  /// Creates a message from `bytes::Bytes`.
  pub fn from_bytes(data: Bytes) -> Self {
    Self {
      data: Some(data),
      ..Default::default()
    }
  }

  /// Creates a message from a static byte slice (zero-copy).
  pub fn from_static(data: &'static [u8]) -> Self {
    Self {
      data: Some(Bytes::from_static(data)),
      ..Default::default()
    }
  }

  /// Returns a reference to the message payload bytes, if any.
  pub fn data(&self) -> Option<&[u8]> {
    self.data.as_deref()
  }

  /// Returns the group name attached to this message, if any.
  /// Used by the RADIO-DISH pattern. Returns `None` for messages created
  /// outside of the RADIO-DISH context.
  pub fn group(&self) -> Option<&[u8]> {
    self.group.as_deref()
  }

  /// Attaches a group name to this message.
  ///
  /// # Errors
  /// Returns `Err(ZmqError::InvalidArgument)` if:
  /// - `group` is empty (0 bytes)
  /// - `group` exceeds 255 bytes
  /// - any byte in `group` is `\x00` (NUL is forbidden per RFC §Group)
  pub fn set_group(&mut self, group: impl Into<Bytes>) -> Result<(), ZmqError> {
    let bytes: Bytes = group.into();
    if bytes.is_empty() || bytes.len() > 255 {
      return Err(ZmqError::InvalidArgument(
        "Group length must be 1–255 bytes".into(),
      ));
    }
    if bytes.iter().any(|&b| b == 0) {
      return Err(ZmqError::InvalidArgument(
        "Group bytes must be in range 1–255 (NUL is forbidden)".into(),
      ));
    }
    self.group = Some(bytes);
    Ok(())
  }

  /// Removes the group from this message.
  pub fn clear_group(&mut self) {
    self.group = None;
  }

  /// Returns the size of the message payload in bytes.
  pub fn size(&self) -> usize {
    self.data.as_ref().map_or(0, |d| d.len())
  }

  /// Returns the flags associated with the message.
  pub fn flags(&self) -> MsgFlags {
    self.flags
  }

  /// Sets the flags for the message (e.g., `MsgFlags::MORE`).
  pub fn set_flags(&mut self, flags: MsgFlags) {
    self.flags = flags;
  }

  /// Returns an immutable reference to the message metadata map.
  pub fn metadata(&self) -> &Metadata {
    &self.metadata
  }

  /// Returns a mutable reference to the message metadata map.
  /// Note: Modifying metadata requires awaiting async lock internally.
  pub fn metadata_mut(&mut self) -> &mut Metadata {
    &mut self.metadata
  }

  // --- Flag Helpers ---

  /// Checks if the `MORE` flag is set.
  pub fn is_more(&self) -> bool {
    self.flags.contains(MsgFlags::MORE)
  }

  /// Checks if the `COMMAND` flag is set.
  pub fn is_command(&self) -> bool {
    self.flags.contains(MsgFlags::COMMAND)
  }

  /// Returns the internal `Bytes` object if data is present.
  ///
  /// This is useful for operations that need to take ownership or a clone
  /// of the underlying `Bytes` object, such as for zerocopy send operations.
  /// Cloning `Bytes` is cheap as it is reference-counted.
  pub fn data_bytes(&self) -> Option<Bytes> {
    self.data.clone()
  }
}

impl fmt::Debug for Msg {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Msg")
      .field("size", &self.size())
      .field("flags", &self.flags)
      .field("data", &self.data().map(|d| format!("{} bytes", d.len()))) // Avoid printing large data
      .field(
        "group",
        &self
          .group()
          .map(|g| String::from_utf8_lossy(g).into_owned()),
      )
      .field("metadata", &self.metadata) // Relies on Metadata::Debug
      .finish()
  }
}
