# Plan 2: Context Drop and io-uring Lifecycle Management

## Problem

There is no `Drop` implementation for `Context` or `ContextInner`
(`core/src/context.rs`). When a `Context` is dropped without calling `term()`,
all internal actors, the io-uring worker thread, and the upstream processor
tokio task continue running indefinitely. This is the **primary cause** of
`cargo bench --features="io-uring"` hanging -- the tokio runtime cannot shut
down because the upstream processor task is still alive.

### Current State

- `Context::term()` (`context.rs:285-290`) calls `shutdown()` then
  `wait_for_termination()`, but **nobody calls `term()` in benchmarks** (except
  `generic_client_benchmark.rs`).
- `Context::shutdown()` (`context.rs:278-281`) publishes
  `SystemEvent::ContextTerminating` via the event bus but does **not** touch the
  global io-uring backend.
- `shutdown_uring_backend()` (`core/src/uring/mod.rs:117-185`) exists and
  correctly tears down the worker thread + upstream processor, but is **never
  called** from any `Context` lifecycle method.
- The io-uring backend is a process-level singleton via `OnceCell` -- it's
  initialized once on the first `Context::new()` and never cleaned up.

### Consequence

1. Dropping a `Context` without calling `term()` leaks all internal resources.
2. Even calling `term()` does not shut down the io-uring backend.
3. The io-uring worker OS thread runs forever.
4. The upstream processor tokio task runs forever, preventing the tokio runtime
   from shutting down.
5. Benchmark processes hang on exit.

## Files to Modify

- `core/src/context.rs` -- Add `Drop` impl, integrate io-uring shutdown
- `core/src/uring/mod.rs` -- Add context reference counting, conditional shutdown
- `core/src/uring/global_state.rs` -- Add context counter

## Implementation

### Step 1: Add a global active-context counter

**File:** `core/src/uring/global_state.rs`

Add an `AtomicUsize` counter that tracks how many `Context` instances are alive
with io-uring enabled.

```rust
use std::sync::atomic::AtomicUsize;

/// Number of active Context instances using the io-uring backend.
static ACTIVE_URING_CONTEXT_COUNT: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn increment_uring_context_count() {
    ACTIVE_URING_CONTEXT_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
}

pub(crate) fn decrement_uring_context_count() -> usize {
    ACTIVE_URING_CONTEXT_COUNT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
}
```

### Step 2: Increment the counter in `ContextInner::new()`

**File:** `core/src/context.rs`, inside the `#[cfg(feature = "io-uring")]` block
(lines 63-72).

After the successful `ensure_global_uring_systems_started()` call:

```rust
#[cfg(feature = "io-uring")]
{
    match global_state::ensure_global_uring_systems_started() {
        Ok(_) | Err(ZmqError::InvalidState(_)) => {}
        Err(err) => { return Err(err); }
    }
    global_state::get_global_uring_worker_op_tx()?;
    global_state::increment_uring_context_count();  // <-- ADD THIS
}
```

### Step 3: Add `Drop` for `ContextInner`

**File:** `core/src/context.rs`

```rust
impl Drop for ContextInner {
    fn drop(&mut self) {
        // Signal shutdown to all actors via event bus
        // This is a synchronous drop, so we can't await.
        // Publish the terminating event if not already done.
        if let Some(event_bus) = self.event_bus.as_ref() {
            let _ = event_bus.publish(SystemEvent::ContextTerminating);
        }

        #[cfg(feature = "io-uring")]
        {
            // Decrement the active context count.
            // If this was the last context, trigger io-uring backend shutdown.
            let prev_count = global_state::decrement_uring_context_count();
            if prev_count == 1 {
                // prev_count == 1 means we just decremented from 1 to 0.
                // Spawn a blocking task to shut down the io-uring backend.
                // We can't await here, so we spawn it detached.
                // The shutdown function itself is safe to call from a
                // non-async context via spawn_blocking internally.
                std::thread::spawn(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build();
                    if let Ok(rt) = rt {
                        rt.block_on(async {
                            if let Err(e) = crate::uring::shutdown_uring_backend().await {
                                tracing::warn!(
                                    "Failed to shutdown io-uring backend on last context drop: {}",
                                    e
                                );
                            }
                        });
                    }
                });
            }
        }
    }
}
```

**Design considerations:**

- `Drop` cannot be async, so we need to handle the io-uring shutdown carefully.
- Spawning a new thread with a mini runtime is heavy but only happens once
  (when the last context drops). This is the cleanup path, not the hot path.
- Alternative: Use `tokio::task::spawn` if we can guarantee we're inside a
  tokio runtime (which we almost always are). But `Drop` can be called from
  any thread, so the safe option is to spawn a dedicated thread.

### Step 4: Also call `shutdown_uring_backend()` in `Context::term()`

**File:** `core/src/context.rs`, in `Context::term()` (lines 285-290).

```rust
pub async fn term(&self) -> Result<(), ZmqError> {
    self.inner.shutdown().await;
    self.inner.wait_for_termination().await;

    #[cfg(feature = "io-uring")]
    {
        let prev_count = global_state::decrement_uring_context_count();
        if prev_count == 1 {
            crate::uring::shutdown_uring_backend().await?;
        }
    }

    Ok(())
}
```

Wait -- this would double-decrement if `term()` is called and then `Drop` also
fires. We need to guard against that.

### Step 5: Add a `terminated` flag to prevent double-decrement

**File:** `core/src/context.rs`, in `ContextInner`:

```rust
pub(crate) struct ContextInner {
    // ... existing fields ...
    #[cfg(feature = "io-uring")]
    uring_context_decremented: std::sync::atomic::AtomicBool,
}
```

Initialize it to `false` in `ContextInner::new()`.

Then in `ContextInner`, add a helper:

```rust
#[cfg(feature = "io-uring")]
fn try_decrement_uring_context(&self) -> Option<usize> {
    use std::sync::atomic::Ordering;
    if self.uring_context_decremented
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        Some(global_state::decrement_uring_context_count())
    } else {
        None // Already decremented (term() was called before drop)
    }
}
```

Use this helper in both `term()` and `Drop`:

**In `Context::term()`:**
```rust
#[cfg(feature = "io-uring")]
{
    if let Some(prev_count) = self.inner.try_decrement_uring_context() {
        if prev_count == 1 {
            crate::uring::shutdown_uring_backend().await?;
        }
    }
}
```

**In `Drop for ContextInner`:**
```rust
#[cfg(feature = "io-uring")]
{
    if let Some(prev_count) = self.try_decrement_uring_context() {
        if prev_count == 1 {
            std::thread::spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build();
                if let Ok(rt) = rt {
                    rt.block_on(async {
                        let _ = crate::uring::shutdown_uring_backend().await;
                    });
                }
            });
        }
    }
}
```

### Step 6: Make `shutdown_uring_backend()` re-entrant safe

**File:** `core/src/uring/mod.rs`

`shutdown_uring_backend()` already guards with
`URING_BACKEND_INITIALIZED.swap(false, ...)` (line 119-128). Verify this is
sufficient for concurrent calls. The `swap` is atomic, so only one caller will
see `true` and proceed. This is safe.

However, after shutdown, the `OnceCell` statics still hold their initialized
values (the `Option`s inside are `None` after shutdown, but the `OnceCell` itself
is still "initialized"). This means `URING_INIT_RESULT.get_or_try_init(...)` in
`initialize_uring_backend()` will return the stale `Ok(())` without
re-initializing. This is addressed in Plan 5.

## Edge Cases

1. **Multiple contexts created/destroyed rapidly:** The `AtomicUsize` counter
   handles concurrent increment/decrement correctly.

2. **`term()` called on one context while another is still alive:** The counter
   won't reach 0, so the backend stays alive. Correct behavior.

3. **Context dropped from a non-tokio thread:** The `Drop` impl spawns its own
   thread with a mini runtime. Safe.

4. **Context dropped during process exit:** The spawned shutdown thread may not
   complete before `main()` returns. This is acceptable -- the OS will clean up
   the io-uring ring and file descriptors. The important case is within
   benchmark harnesses where the process continues.

5. **`Arc<Context>` with multiple owners:** `Drop` for `ContextInner` only fires
   when the last `Arc` is dropped. The counter tracks `ContextInner` instances,
   not `Arc` clones. Correct.

## Testing

1. `cargo test --features="io-uring"` -- verify no regressions.
2. Write a test that creates and drops multiple `Context` instances, verifying
   the io-uring backend is shut down only when the last one drops.
3. `cargo bench --features="io-uring"` -- verify benchmarks complete without
   hanging.

## Risk Assessment

**Medium risk.** The `Drop` impl introduces a new thread spawn on the final
context drop, which adds complexity. The double-decrement guard via `AtomicBool`
is straightforward but must be correct. The interaction between `term()` and
`Drop` needs careful testing. The `Drop` for `ContextInner` also publishes
`ContextTerminating` which may duplicate what `shutdown()` already did -- this
is idempotent in the event bus, so it's safe.

## Dependencies

- Should be implemented after Plan 1 (eventfd fix) to avoid masking issues.
- Plan 5 (re-initialization) is needed if benchmarks want to create new contexts
  after the backend has been shut down.
