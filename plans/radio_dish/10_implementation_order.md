# 10 — Implementation Order and Risk Notes

## Step-by-Step Order

Each step lists the file(s) to edit and their dependencies.

### Step 1 — `core/src/error.rs`
- Check if `ZmqError::InvalidArgument` variant exists.
- If not, add it (needed by `Msg::set_group` and `DishSocket::set_pattern_option`).
- **No dependencies.**

### Step 2 — `core/src/message/msg.rs`
- Add `group: Option<Bytes>` field to `Msg`.
- Add `group()`, `set_group()`, `clear_group()` methods.
- Update `Debug` impl.
- **Depends on**: Step 1 (`ZmqError::InvalidArgument`).

### Step 3 — `core/src/protocol/zmtp/command.rs`
- Add `ZMTP_CMD_JOIN_NAME`, `ZMTP_CMD_LEAVE_NAME` constants.
- Add `Join(Bytes)` and `Leave(Bytes)` variants to `ZmtpCommand`.
- Add parsing branches in `ZmtpCommand::parse()`.
- Add `create_join()` and `create_leave()` factory methods.
- **Depends on**: Step 2 (uses `Msg` and `MsgFlags`).

### Step 4 — `core/src/sessionx/protocol_handler/heartbeat.rs`
- Define `DataCommandResult` enum.
- Change `process_heartbeat_command_impl` return type from
  `Result<Option<Msg>, ZmqError>` to `Result<DataCommandResult, ZmqError>`.
- Update match arms accordingly.
- **Depends on**: Step 3 (`ZmtpCommand::Join`, `ZmtpCommand::Leave`).

### Step 5 — `core/src/sessionx/actor.rs`
- Update the `handle_incoming_from_network` method to match on
  `DataCommandResult` instead of `Option<Msg>`.
- Add `ForwardToSocket` arm that calls `socket_logic.handle_pipe_event`.
- **Depends on**: Step 4 (`DataCommandResult`).

### Step 6 — `core/src/socket/types.rs`
- Add `Radio` and `Dish` variants to `SocketType`.
- **No dependencies** (pure enum addition).

### Step 7 — `core/src/socket/options.rs`
- Add `JOIN: i32 = 74` and `LEAVE: i32 = 75` constants.
- **No dependencies.**

### Step 8 — `core/src/socket/radio_socket.rs` (new file)
- Implement `RadioSocket` struct and `ISocket` trait.
- **Depends on**: Steps 2, 3, 6, 7.

### Step 9 — `core/src/socket/dish_socket.rs` (new file)
- Implement `DishSocket` struct and `ISocket` trait.
- **Depends on**: Steps 2, 3, 6, 7.

### Step 10 — `core/src/socket/mod.rs`
- Add `pub mod radio_socket;` and `pub mod dish_socket;`.
- **Depends on**: Steps 8, 9.

### Step 11 — `core/src/socket/core/mod.rs`
- Add `Radio` and `Dish` arms to the `create_and_spawn` factory match.
- **Depends on**: Steps 6, 8, 9, 10.

### Step 12 — Compile check
```
cargo build -p rzmq
```
Fix any compilation errors before writing tests.

### Step 13 — `core/tests/radio_dish.rs` (new file)
- Write all 14 integration tests.
- **Depends on**: All previous steps compiling successfully.

### Step 14 — Run tests
```
cargo test -p rzmq radio_dish
```
Fix any test failures.

### Step 15 — Full test suite
```
cargo test -p rzmq
```
Ensure no regressions in existing tests.

---

## Risk Areas

### R1 — `DataCommandResult` return type change in `heartbeat.rs`

The function `process_heartbeat_command_impl` is called in exactly **one place**
(`actor.rs:handle_incoming_from_network`). The change is self-contained. The
compiler will flag the call site if the return type doesn't match.

### R2 — `Msg` struct layout change

Adding `group: Option<Bytes>` changes the struct size but not any public
invariants. All existing code that creates `Msg` via `from_vec`, `from_bytes`,
`from_static`, or `Msg::new()` continues to work because they use
`..Default::default()`. The `Default` derive produces `group: None`.

If `Msg` is ever serialised/deserialised directly (e.g. in benchmarks or
cross-process scenarios), the additional field could cause issues — but in
this codebase `Msg` is not serialised; it only crosses the ZMTP wire as
encoded bytes.

### R3 — `HashSet<Bytes>` lookup by `&[u8]`

`Bytes` implements `Borrow<[u8]>`, `Hash` (same as `[u8]`), and `PartialEq<[u8]>`.
Therefore `HashSet::<Bytes>::contains::<[u8]>` works correctly. Verify this
compiles — if not, a `HashMap<Vec<u8>, ()>` is a safe fallback.

### R4 — DISH `handle_pipe_event` and the `parking_lot` lock

The `joined_groups.read()` lock in `handle_pipe_event` is held for the duration
of `contains()` only — a fast O(1) hash lookup. No `await` points occur while
the lock is held. This satisfies `parking_lot`'s non-async requirements.

### R5 — Broadcast-all strategy and HWM

`Distributor::send_to_all` already handles `ResourceLimitReached` by silently
dropping the message for that peer, which is correct per RFC §RADIO ("SHALL
silently drop the message if the queue for a dish is full"). No extra handling
needed.

### R6 — TCP port conflicts in tests

Tests use ports 5800–5806. Check `core/tests/` for any existing test using
these ports. Current known port ranges:
- pub_sub.rs: 5562–5580
- req_rep.rs: 5555–5561
- push_pull.rs: 5590–5600
- dealer_*.rs: 5620–5660
Ports 5800+ appear unused. Use `serial` attribute on TCP tests to prevent
parallel conflicts.

### R7 — `set_group` on decoded message in DishSocket

When DISH reconstructs the decoded message, it calls `msg.set_group(group_bytes)`
where `group_bytes` was decoded from a RADIO wire frame. The group was already
validated by RADIO's `send()` before encoding, so `set_group` should not fail.
Use `.expect("RADIO sent valid group")` or handle the error defensively with
a log+drop.

---

## Checklist

- [x] Step 1: ZmqError::InvalidArgument added
- [ ] Step 2: Msg::group / set_group / clear_group
- [ ] Step 3: ZmtpCommand::Join / Leave / create_join / create_leave
- [ ] Step 4: DataCommandResult enum + heartbeat.rs updated
- [ ] Step 5: actor.rs ForwardToSocket arm
- [ ] Step 6: SocketType::Radio + SocketType::Dish
- [ ] Step 7: options::JOIN + options::LEAVE
- [ ] Step 8: radio_socket.rs
- [ ] Step 9: dish_socket.rs
- [ ] Step 10: socket/mod.rs module declarations
- [ ] Step 11: core/mod.rs factory arms
- [ ] Step 12: cargo build passes
- [ ] Step 13: radio_dish.rs tests
- [ ] Step 14: cargo test radio_dish passes
- [ ] Step 15: full cargo test passes (no regressions)
