# 02 — Msg Changes: Add Group Field

## File

`core/src/message/msg.rs`

## Why

The RFC requires that:
- RADIO attaches a group to every outgoing message
- DISH retrieves the group from every received message
- Both operations must be thread-safe and single-API-call (no async needed)

Storing the group directly on `Msg` as `Option<Bytes>` satisfies all three
requirements: it is sync, zero-copy (Bytes is reference-counted), and
survives the full send/receive path without async locking.

## Changes

### 1. Add `group` field to the `Msg` struct

```rust
pub struct Msg {
  data: Option<Bytes>,
  flags: MsgFlags,
  metadata: Metadata,
  group: Option<Bytes>,  // NEW — group for RADIO/DISH pattern
}
```

`Option<Bytes>` defaults to `None`, so the derived `Default` impl and all
existing constructor helpers (`from_vec`, `from_bytes`, `from_static`) remain
correct without changes — they use `..Default::default()` which will
zero-initialise the new field.

### 2. Add public getter

```rust
/// Returns the group name attached to this message, if any.
/// Used by the RADIO-DISH pattern. Returns `None` for messages created
/// outside of the RADIO-DISH context.
pub fn group(&self) -> Option<&[u8]> {
    self.group.as_deref()
}
```

### 3. Add validated setter

```rust
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
```

### 4. Add clear helper

```rust
/// Removes the group from this message.
pub fn clear_group(&mut self) {
    self.group = None;
}
```

### 5. Update the `Debug` impl

Add the `group` field to the existing manual `Debug` implementation:

```rust
impl fmt::Debug for Msg {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Msg")
      .field("size", &self.size())
      .field("flags", &self.flags)
      .field("data", &self.data().map(|d| format!("{} bytes", d.len())))
      .field("group", &self.group().map(|g| String::from_utf8_lossy(g).into_owned()))  // NEW
      .field("metadata", &self.metadata)
      .finish()
  }
}
```

### 6. ZmqError::InvalidArgument

Check whether `ZmqError` already has an `InvalidArgument` variant. If not,
add it to `core/src/error.rs`:

```rust
/// An argument passed to a function was invalid.
InvalidArgument(String),
```

And ensure it implements `Display` similarly to other variants.

## Impact on Existing Code

- All existing `Msg` construction continues to work unchanged.
- `Msg::clone()` automatically clones `group` (Bytes clone is O(1)).
- No changes needed in any transport, session, or other socket code for
  existing patterns — they never set or read `group`.
- The `Default` impl produces `group: None`, which is correct.
