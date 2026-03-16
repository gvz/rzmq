# Plan 5: Global State Re-initialization Support

## Problem

The io-uring backend uses `OnceCell` for all global state
(`core/src/uring/mod.rs:58`, `core/src/uring/global_state.rs:25-46`).
`OnceCell::get_or_try_init()` runs the initialization closure exactly once per
process. After `shutdown_uring_backend()` clears the inner `Option` values,
the `OnceCell` containers themselves remain "initialized" -- subsequent calls
to `get_or_try_init()` return the stale (empty) result without re-running the
initialization closure.

This means that once the io-uring backend is shut down (via Plan 2), creating
a new `Context` will call `ensure_global_uring_systems_started()`, which calls
`initialize_uring_backend()`, which calls `URING_INIT_RESULT.get_or_try_init()`,
which returns the stale `Ok(())` from the first initialization. The new context
thinks the backend is running, but it's actually shut down.

### Where This Matters

1. **Benchmarks**: Criterion creates fresh `Context` objects for each sample.
   With Plans 2+4, the last context of one sample triggers shutdown, but the
   first context of the next sample needs to re-initialize.
2. **Tests**: Multiple `#[tokio::test]` functions in the same test binary share
   a process. If one test creates and terminates a context, subsequent tests
   need re-initialization.
3. **Long-running applications**: Less common, but an application that creates
   a context, terminates it, and later creates another context needs this.

### Current Statics Using `OnceCell`

```
URING_INIT_RESULT: OnceCell<Result<(), ZmqError>>              (mod.rs:58)
URING_WORKER_OP_TX: OnceCell<Mutex<Option<SignalingOpSender>>>  (global_state.rs:25)
URING_WORKER_JOIN_HANDLE: OnceCell<Mutex<Option<...>>>          (global_state.rs:27)
PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR: OnceCell<Mutex<...>>     (global_state.rs:30)
PARSED_MSG_RX_FOR_PROCESSOR: OnceCell<Mutex<...>>               (global_state.rs:33)
URING_UPSTREAM_PROCESSOR_JOIN_HANDLE: OnceCell<Mutex<...>>      (global_state.rs:36)
URING_FD_TO_SOCKET_CORE_MAILBOX_MAP: OnceCell<Arc<...>>         (global_state.rs:39)
```

## Solution Options

### Option A: Replace `OnceCell` with `Mutex<Option<T>>` (Recommended)

Replace the `OnceCell` pattern with plain `Mutex<Option<T>>` for all mutable
global state. Keep `OnceCell` only for truly one-time initialization (like the
FD-to-mailbox map `Arc`).

**Pros:**
- Clean re-initialization semantics.
- No sentinel values or special "is it really initialized" checks.
- The `Mutex<Option<T>>` pattern is already used inside the `OnceCell`s.

**Cons:**
- Slightly more code to manage initialization checks.
- Locking overhead on every access (but these are infrequent control-plane ops).

### Option B: Use `OnceLock` + Reset Flag

Keep `OnceCell` but add a mechanism to "reset" it. Standard `OnceLock` doesn't
support this, but we can use an `AtomicBool` guard:

```rust
static URING_NEEDS_REINIT: AtomicBool = AtomicBool::new(false);
```

When `shutdown_uring_backend()` runs, set this flag. When
`ensure_global_uring_systems_started()` runs, check the flag and bypass the
stale `OnceCell` result.

**Pros:** Minimal changes to existing structure.
**Cons:** Hacky. The `OnceCell` still holds stale data.

### Option C: Recreate state in `Mutex<Option<T>>`, use `OnceCell` only for the Mutex

This is effectively what's already happening. The `OnceCell` holds a
`Mutex<Option<T>>`. The `Option` is `Some` when initialized and `None` after
shutdown. The `OnceCell` just ensures the `Mutex` is created.

The bug is in `URING_INIT_RESULT`, which holds `OnceCell<Result<(), ZmqError>>`
(no inner `Mutex<Option<...>>`). This is the only one that prevents
re-initialization.

## Files to Modify

- `core/src/uring/mod.rs` -- Fix `URING_INIT_RESULT` and `initialize_uring_backend()`
- `core/src/uring/global_state.rs` -- Fix `ensure_global_uring_systems_started()`

## Implementation (Option A, focused on the actual bug)

### Step 1: Replace `URING_INIT_RESULT` with a resettable guard

**File:** `core/src/uring/mod.rs`

The actual bug is that `URING_INIT_RESULT: OnceCell<Result<(), ZmqError>>` at
line 58 prevents `get_or_try_init` from re-running.

Change from:
```rust
static URING_INIT_RESULT: OnceCell<Result<(), ZmqError>> = OnceCell::new();
pub static URING_BACKEND_INITIALIZED: AtomicBool = AtomicBool::new(false);
```

To:
```rust
/// Guards concurrent initialization attempts.
static URING_INIT_LOCK: Mutex<()> = Mutex::new(());
pub static URING_BACKEND_INITIALIZED: AtomicBool = AtomicBool::new(false);
```

### Step 2: Rewrite `initialize_uring_backend()` with the new guard

**File:** `core/src/uring/mod.rs`

Change from:
```rust
pub fn initialize_uring_backend(config: UringConfig) -> Result<(), ZmqError> {
    URING_INIT_RESULT
        .get_or_try_init(|| {
            // ... initialization logic ...
        })
        .and_then(|r| r.clone())
}
```

To:
```rust
pub fn initialize_uring_backend(config: UringConfig) -> Result<(), ZmqError> {
    // Fast path: already initialized
    if URING_BACKEND_INITIALIZED.load(Ordering::SeqCst) {
        return Ok(());
    }

    // Slow path: acquire lock and double-check
    let _guard = URING_INIT_LOCK.lock();
    if URING_BACKEND_INITIALIZED.load(Ordering::SeqCst) {
        return Ok(());
    }

    // --- Perform initialization ---

    // 1. Create upstream event channel
    let (upstream_tx, upstream_rx) = fibre::mpmc::unbounded();
    {
        let mut guard = global_state::get_global_parsed_msg_tx_mutex().lock();
        *guard = Some(upstream_tx.clone());
    }
    {
        let mut guard = global_state::get_global_parsed_msg_rx_mutex().lock();
        *guard = Some(upstream_rx);
    }

    // 2. Spawn worker
    let zmtp_factory = Arc::new(ZmtpHandlerFactory::new(upstream_tx));
    let (signaling_op_sender, worker_join_handle) =
        UringWorker::spawn_with_config(config, vec![zmtp_factory], /* ... */)?;

    {
        let mut guard = global_state::get_uring_worker_op_tx_mutex().lock();
        *guard = Some(signaling_op_sender);
    }
    {
        let mut guard = global_state::get_uring_worker_join_handle_mutex().lock();
        *guard = Some(worker_join_handle);
    }

    // 3. Init FD-to-mailbox map
    // ... (use get_or_init for the OnceCell since the map Arc persists)

    // 4. Spawn upstream processor
    let processor_rx = {
        let mut guard = global_state::get_global_parsed_msg_rx_mutex().lock();
        guard.take().expect("RX must be present")
    };
    let shutdown_rx = /* ... from Plan 3 ... */;
    let upstream_handle = tokio::spawn(async move {
        run_global_uring_upstream_processor(processor_rx, fd_map, shutdown_rx).await;
    });
    {
        let mut guard = global_state::get_uring_upstream_processor_join_handle_mutex().lock();
        *guard = Some(upstream_handle);
    }

    // 5. Mark as initialized
    URING_BACKEND_INITIALIZED.store(true, Ordering::SeqCst);

    Ok(())
}
```

This is a double-checked locking pattern. The `Mutex` serializes concurrent
initialization attempts, and `URING_BACKEND_INITIALIZED` provides the fast path.

### Step 3: Reset `URING_BACKEND_INITIALIZED` in `shutdown_uring_backend()`

This is already done at line 119-128:

```rust
if !URING_BACKEND_INITIALIZED.swap(false, Ordering::SeqCst) {
    return Ok(()); // Already shut down or not initialized
}
```

After `swap(false, ...)`, the next `initialize_uring_backend()` call will
see `false` on the fast path and proceed to re-initialize. The `Mutex` guard
prevents races between shutdown and initialization.

### Step 4: Handle the FD-to-mailbox map `OnceCell`

The `URING_FD_TO_SOCKET_CORE_MAILBOX_MAP` uses `OnceCell<Arc<RwLock<HashMap<...>>>>`.
This one is fine as a `OnceCell` because:

- The `Arc<RwLock<HashMap<...>>>` is created once and persists.
- `shutdown_uring_backend()` clears the inner HashMap (line 175-181) but
  doesn't drop the `Arc`.
- Re-initialization reuses the same `Arc`.

Verify that `shutdown_uring_backend()` clears the map:
```rust
if let Some(map) = global_state::get_uring_fd_to_socket_core_mailbox_map_oncecell().get() {
    let mut guard = map.write().await;
    guard.clear();
}
```

If it doesn't, add it.

### Step 5: Handle `ensure_global_uring_systems_started()`

**File:** `core/src/uring/global_state.rs`

This function (lines 88-98) checks `URING_BACKEND_INITIALIZED`:

```rust
pub(crate) fn ensure_global_uring_systems_started() -> Result<(), ZmqError> {
    if !URING_BACKEND_INITIALIZED.load(Ordering::SeqCst) {
        crate::uring::initialize_uring_backend(UringConfig::default())?;
    }
    Ok(())
}
```

This already works correctly with the new `initialize_uring_backend()`:
- If `URING_BACKEND_INITIALIZED` is `false` (either never initialized or
  after shutdown), it calls `initialize_uring_backend()`.
- `initialize_uring_backend()` re-initializes everything.

No change needed here.

## Thread Safety Analysis

The critical section is the transition between `shutdown_uring_backend()` and
`initialize_uring_backend()`:

```
Thread A (shutdown):        Thread B (new Context):
  swap(false)               load() -> false
  lock(INIT_LOCK)           lock(INIT_LOCK) -- BLOCKS
  ... cleanup ...
  unlock(INIT_LOCK)
                            lock(INIT_LOCK) -- ACQUIRED
                            load() -> false
                            ... initialize ...
                            store(true)
                            unlock(INIT_LOCK)
```

This is safe. Thread B blocks until Thread A finishes cleanup, then
Thread B re-initializes.

If Thread B arrives after Thread A has finished:
```
Thread A (shutdown):        Thread B (new Context):
  swap(false)
  lock(INIT_LOCK)
  ... cleanup ...
  unlock(INIT_LOCK)
                            load() -> false
                            lock(INIT_LOCK) -- ACQUIRED immediately
                            load() -> false
                            ... initialize ...
                            store(true)
                            unlock(INIT_LOCK)
```

Also safe.

## Testing

1. `cargo test --features="io-uring"` -- existing tests pass.
2. Write a specific re-initialization test:
   ```rust
   #[tokio::test]
   async fn test_reinitialize_uring_backend() {
       // First context
       let ctx1 = Context::new().unwrap();
       ctx1.term().await.unwrap();
       // Backend should be shut down (last context dropped)

       // Second context -- should re-initialize
       let ctx2 = Context::new().unwrap();
       // Create a socket, verify it works
       let sock = ctx2.socket(SocketType::Push).unwrap();
       sock.close().await.unwrap();
       ctx2.term().await.unwrap();
   }
   ```
3. `cargo bench --features="io-uring"` -- multiple benchmark iterations
   create/destroy contexts; verify no "already initialized" errors.

## Risk Assessment

**Medium risk.** Replacing `OnceCell` with double-checked locking changes the
initialization semantics. The critical invariant is that all global state
(`Mutex<Option<T>>` values) are properly cleared during shutdown and properly
set during initialization. Missing any field will cause a null/empty access.

The FD-to-mailbox map is the trickiest: it's shared via `Arc` and accessed
concurrently. Clearing it during shutdown while a socket might still be
accessing it could cause issues. But with Plans 2+4 (context Drop + benchmark
teardown), all sockets should be closed before shutdown.

## Dependencies

- Requires Plan 2 (context lifecycle) -- without it, `shutdown_uring_backend()`
  is never called, so re-initialization is never needed.
- Requires Plan 3 (stoppable processor) -- the processor must actually stop
  during shutdown for clean re-initialization.
- Should be implemented last, after Plans 1-4 are verified.

## Implementation Order

This plan should be implemented in this order:
1. Replace `URING_INIT_RESULT` with `URING_INIT_LOCK` (Step 1-2)
2. Verify shutdown clears all state (Step 3-4)
3. Test re-initialization (Step 5)
