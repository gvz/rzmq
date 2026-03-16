# Plan 3: Make Upstream Processor Stoppable

## Problem

The upstream processor tokio task
(`core/src/uring/global_state.rs:149-196`) runs an infinite `loop` that only
exits when `msg_rx.recv().await` returns `Err(RecvError::Disconnected)`.
The sender (`PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR`) is held in a global
`OnceCell<Mutex<Option<SyncSender<...>>>>` and is only dropped during
`shutdown_uring_backend()`, which is never called (addressed in Plan 2).

Even with Plan 2 implemented, the current shutdown sequence has a timing issue:
`shutdown_uring_backend()` drops the `SignalingOpSender` first (closing the op
channel), which triggers the worker to drain and eventually drop its end of the
upstream event channel. But between the worker starting to drain and actually
exiting, there's a window where the upstream processor is stuck in
`recv().await` waiting on a channel that still has a live sender (the global TX
hasn't been dropped yet at that point in the shutdown sequence).

### Current Shutdown Sequence in `shutdown_uring_backend()`

```
1. Drop SignalingOpSender  (closes op channel -> worker starts draining)
2. Join worker thread       (blocks until worker exits)
3. Await upstream processor (blocks until processor task completes)
4. Clear remaining global state
```

The upstream processor TX (`PARSED_MSG_TX_FOR_WORKER_TO_PROCESSOR`) is cleared
in step 4 (line 175-181), **after** we try to await the processor in step 3.
But the processor only exits when the TX is dropped. This creates a deadlock:

- Step 3 waits for the processor to exit.
- The processor waits for the TX to be dropped.
- The TX is dropped in step 4, which never runs because step 3 is blocked.

The actual code at `mod.rs:159-172` takes the TX and drops it before awaiting,
but only the global TX copy. The worker thread also holds a cloned `SyncSender`
(passed to it as `upstream_event_tx` in `spawn_with_config`). When the worker
drains and exits, it drops this sender. If the worker exits before step 3,
the processor sees disconnection and exits. But the timing is fragile.

## Solution: Add a `CancellationToken`

Use `tokio_util::sync::CancellationToken` (or a simpler `tokio::sync::Notify` /
manual `AtomicBool` + channel close) to give the upstream processor an explicit
shutdown signal, independent of channel closure.

## Files to Modify

- `core/src/uring/global_state.rs` -- Add cancellation support to processor
- `core/src/uring/mod.rs` -- Store and trigger the cancellation token
- `core/Cargo.toml` -- Add `tokio_util` dependency if not already present
  (check first; it may already be pulled in transitively)

## Implementation

### Step 1: Check for existing `tokio_util` dependency

```bash
grep -r "tokio.util" core/Cargo.toml
```

If not present, add:
```toml
[dependencies]
tokio-util = { version = "0.7", features = ["sync"] }
```

Alternatively, avoid the extra dependency by using a simple
`tokio::sync::watch` channel or `tokio::sync::Notify`.

### Step 2: Add a shutdown signal to global state

**File:** `core/src/uring/global_state.rs`

Using `tokio::sync::watch` (already available via tokio):

```rust
use tokio::sync::watch;

static UPSTREAM_PROCESSOR_SHUTDOWN_TX: OnceCell<Mutex<Option<watch::Sender<bool>>>>
    = OnceCell::new();
```

Add getter:
```rust
pub(crate) fn get_upstream_processor_shutdown_tx_mutex()
    -> &'static Mutex<Option<watch::Sender<bool>>>
{
    UPSTREAM_PROCESSOR_SHUTDOWN_TX
        .get_or_init(|| Mutex::new(None))
}
```

### Step 3: Modify `run_global_uring_upstream_processor` to respect cancellation

**File:** `core/src/uring/global_state.rs`, `run_global_uring_upstream_processor`
(lines 149-196).

Change from:

```rust
pub(crate) async fn run_global_uring_upstream_processor(
    msg_rx: AsyncReceiver<(RawFd, HandlerUpstreamEvent)>,
    fd_to_mailbox_map: Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>,
) {
    loop {
        match msg_rx.recv().await {
            Ok((fd, event)) => { /* process */ }
            Err(fibre::mpmc::RecvError::Disconnected) => { break; }
        }
    }
}
```

To:

```rust
pub(crate) async fn run_global_uring_upstream_processor(
    msg_rx: AsyncReceiver<(RawFd, HandlerUpstreamEvent)>,
    fd_to_mailbox_map: Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                // Shutdown signal received. Drain any remaining messages
                // non-blockingly before exiting.
                while let Ok((fd, event)) = msg_rx.try_recv() {
                    process_upstream_event(fd, event, &fd_to_mailbox_map).await;
                }
                tracing::info!("[UringUpstreamProcessor] Shutdown signal received, exiting.");
                break;
            }

            result = msg_rx.recv() => {
                match result {
                    Ok((fd, event)) => {
                        process_upstream_event(fd, event, &fd_to_mailbox_map).await;
                    }
                    Err(fibre::mpmc::RecvError::Disconnected) => {
                        tracing::info!(
                            "[UringUpstreamProcessor] Channel disconnected, exiting."
                        );
                        break;
                    }
                }
            }
        }
    }
}
```

Extract the event processing logic into a helper to avoid duplication:

```rust
async fn process_upstream_event(
    fd: RawFd,
    event: HandlerUpstreamEvent,
    fd_to_mailbox_map: &Arc<RwLock<HashMap<RawFd, SocketCoreMailboxSender>>>,
) {
    // ... existing match on event, lookup mailbox, send command ...
    // (move the body of the current Ok arm here)
}
```

### Step 4: Create the watch channel during initialization

**File:** `core/src/uring/mod.rs`, in `initialize_uring_backend()` (lines 63-115).

After line 92 (FD-to-mailbox map init), before line 95 (spawning the processor):

```rust
// Create shutdown signal for upstream processor
let (shutdown_tx, shutdown_rx) = watch::channel(false);
{
    let mut guard = global_state::get_upstream_processor_shutdown_tx_mutex().lock();
    *guard = Some(shutdown_tx);
}
```

Modify the `tokio::spawn` call (lines 95-102) to pass `shutdown_rx`:

```rust
let upstream_handle = tokio::spawn(async move {
    run_global_uring_upstream_processor(
        processor_rx,
        fd_map_for_processor,
        shutdown_rx,  // <-- ADD THIS
    )
    .await;
});
```

### Step 5: Trigger the shutdown signal in `shutdown_uring_backend()`

**File:** `core/src/uring/mod.rs`, in `shutdown_uring_backend()` (lines 117-185).

Add **before** the worker thread join (before line 133):

```rust
// Signal the upstream processor to stop FIRST, before shutting down the worker.
// This ensures the processor exits promptly rather than waiting for channel closure.
{
    let mut guard = global_state::get_upstream_processor_shutdown_tx_mutex().lock();
    if let Some(tx) = guard.take() {
        let _ = tx.send(true);
        // tx is dropped here, which also signals the receiver
    }
}
```

The existing code at lines 133-137 (dropping `SignalingOpSender`) and
lines 140-155 (joining worker thread) remain unchanged.

The await of the upstream processor task (lines 159-172) should now complete
promptly because the shutdown signal was sent before the worker even started
draining.

### Step 6: Clear the shutdown TX in the cleanup phase

**File:** `core/src/uring/mod.rs`, in `shutdown_uring_backend()`, in the final
cleanup section (lines 175-181).

Add:
```rust
{
    let mut guard = global_state::get_upstream_processor_shutdown_tx_mutex().lock();
    *guard = None; // Already taken in step 5, but be defensive
}
```

## Revised Shutdown Sequence

```
1. Send shutdown signal to upstream processor  (NEW)
2. Drop SignalingOpSender                       (closes op channel)
3. Join worker thread                           (worker drains + exits)
4. Await upstream processor task                (already exited from step 1)
5. Clear remaining global state
```

This eliminates the timing dependency between worker exit and processor exit.

## Alternative: Use `tokio::sync::Notify`

If adding `watch` feels heavyweight, a `Notify` works too:

```rust
static UPSTREAM_PROCESSOR_SHUTDOWN: OnceCell<Arc<Notify>> = OnceCell::new();

// In processor:
tokio::select! {
    _ = shutdown_notify.notified() => { break; }
    result = msg_rx.recv() => { ... }
}

// In shutdown:
if let Some(notify) = UPSTREAM_PROCESSOR_SHUTDOWN.get() {
    notify.notify_one();
}
```

`Notify` is simpler but doesn't carry a value. Since we only need a signal
(not a value), `Notify` is sufficient. Choose based on project conventions.

## Testing

1. `cargo test --features="io-uring"` -- verify no regressions.
2. Write a test that:
   - Initializes the io-uring backend.
   - Sends some upstream events.
   - Calls `shutdown_uring_backend()`.
   - Verifies it completes within 1 second (no hang).
3. `cargo bench --features="io-uring"` -- verify clean exit.

## Risk Assessment

**Low risk.** The `tokio::select!` pattern is standard. The `biased` keyword
ensures the shutdown branch is checked first, preventing starvation. The drain
loop after shutdown ensures no messages are lost. The existing channel-based
exit path is preserved as a fallback.

## Dependencies

- Should be implemented alongside Plan 2 (which calls `shutdown_uring_backend()`).
- Independent of Plan 1 (eventfd fix).
