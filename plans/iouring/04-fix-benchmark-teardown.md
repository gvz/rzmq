# Plan 4: Fix Benchmark Teardown

## Problem

All matrix-based benchmarks (`pull_throughput`, `req_rep_throughput`,
`pub_sub_throughput`, `dealer_router_throughput`, `udp_throughput`) fail to
properly tear down the `Context` during benchmark teardown. They only close
individual sockets and then let the `BenchState` (which holds
`ctx_arc: Arc<Context>`) drop silently.

### Current Teardown Pattern (all benchmarks)

```rust
fn teardown_xxx_bench(
    _ctx: BenchContext,
    state: BenchState,
    _runtime: &Runtime,
    _cfg: &ConfigXxx,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        // Close individual sockets
        state.some_socket.close().await ...;
        for sock in state.sockets { sock.close().await ...; }
        // Sleep to allow cleanup
        sleep(Duration::from_millis(100)).await;
        // BenchState is dropped here -- ctx_arc drops silently
    })
}
```

### Consequence

1. Socket actors spawned by the context may continue running as orphaned tokio
   tasks across benchmark iterations.
2. With `--features="io-uring"`, the global io-uring backend accumulates stale
   FD registrations in `URING_FD_TO_SOCKET_CORE_MAILBOX_MAP`.
3. On later iterations, creating new sockets may hit conflicts or race
   conditions with lingering actors from previous iterations.
4. The "Reply channel error" panic seen in benchmark results
   (`dealer_router_throughput` with 16 dealers) is caused by a context being
   dropped while io-uring operations are still in-flight.

### Only Exception

`generic_client_benchmark.rs` (lines 219-225) correctly calls
`client_ctx.term().await.expect(...)`.

## Files to Modify

- `core/benches/pull_throughput.rs` (teardown at ~line 255)
- `core/benches/req_rep_throughput.rs` (teardown at ~line 239)
- `core/benches/pub_sub_throughput.rs` (teardown at ~line 242)
- `core/benches/dealer_router_throughput.rs` (teardown at ~line 278)
- `core/benches/udp_throughput.rs` (teardown at ~line 150)

## Implementation

### Step 1: Add `ctx.term()` call to each teardown function

The fix is the same pattern for all 5 benchmarks. After closing all sockets,
call `ctx_arc.term().await` before the state is dropped.

**Example for `pull_throughput.rs`:**

Change from:
```rust
fn teardown_push_pull_bench(
    _ctx: BenchContext,
    state: BenchState,
    _runtime: &Runtime,
    _cfg: &ConfigPushPull,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        for push_socket in state.push_sockets {
            push_socket.close().await.unwrap_or_else(|e| { ... });
        }
        state.pull_socket.close().await.unwrap_or_else(|e| { ... });
        sleep(Duration::from_millis(100)).await;
    })
}
```

To:
```rust
fn teardown_push_pull_bench(
    _ctx: BenchContext,
    state: BenchState,
    _runtime: &Runtime,
    _cfg: &ConfigPushPull,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        for push_socket in state.push_sockets {
            push_socket.close().await.unwrap_or_else(|e| { ... });
        }
        state.pull_socket.close().await.unwrap_or_else(|e| { ... });

        // Properly terminate the context to ensure all actors are stopped
        // and resources are cleaned up before the next iteration.
        if let Err(e) = state.ctx_arc.term().await {
            eprintln!("[Teardown] Context termination failed: {}", e);
        }
    })
}
```

**Remove the `sleep(Duration::from_millis(100)).await`** -- `ctx.term()` already
waits for all actors to stop (with a 10-second timeout in
`wait_for_termination()`), making the fixed sleep unnecessary.

### Step 2: Apply the same pattern to all 5 benchmarks

**`req_rep_throughput.rs` teardown:**
```rust
// Close rep socket
state.rep_socket.close().await.unwrap_or_else(|e| { ... });
// Close all req sockets
for req_socket in state.req_sockets {
    req_socket.close().await.unwrap_or_else(|e| { ... });
}
// Terminate context
if let Err(e) = state.ctx_arc.term().await {
    eprintln!("[Teardown] Context termination failed: {}", e);
}
```

**`pub_sub_throughput.rs` teardown:**
```rust
state.pub_socket.close().await.unwrap_or_else(|e| { ... });
for sub_socket in state.sub_sockets {
    sub_socket.close().await.unwrap_or_else(|e| { ... });
}
if let Err(e) = state.ctx_arc.term().await {
    eprintln!("[Teardown] Context termination failed: {}", e);
}
```

**`dealer_router_throughput.rs` teardown:**
```rust
for dealer_socket in state.dealer_sockets {
    dealer_socket.close().await.unwrap_or_else(|e| { ... });
}
state.router_socket.close().await.unwrap_or_else(|e| { ... });
if let Err(e) = state.ctx_arc.term().await {
    eprintln!("[Teardown] Context termination failed: {}", e);
}
```

**`udp_throughput.rs` teardown:**
```rust
let _ = state.dish_socket.close().await;
let _ = state.radio_socket.close().await;
if let Err(e) = state.ctx_arc.term().await {
    eprintln!("[Teardown] Context termination failed: {}", e);
}
```

### Step 3: Consider creating a shared teardown helper

Since all benchmarks follow the same pattern, consider a helper macro or
function (optional, for code deduplication):

```rust
/// Terminate the context after closing all sockets.
/// Call this at the end of every benchmark teardown.
async fn terminate_context(ctx: &Arc<Context>) {
    if let Err(e) = ctx.term().await {
        eprintln!("[BenchTeardown] Context termination failed: {}", e);
    }
}
```

This could live in a shared `bench_utils.rs` file or inline in each benchmark.
Given these are benchmarks (not production code), inline is fine.

## Interaction with Plan 2

Plan 2 adds a `Drop` impl for `ContextInner` that also triggers shutdown. With
this plan, teardown explicitly calls `term()`, which:

1. Calls `shutdown()` -- signals all actors to stop.
2. Calls `wait_for_termination()` -- waits for actors to exit.
3. (With Plan 2) Decrements the context counter and potentially shuts down
   the io-uring backend.

The `Drop` impl then fires but finds nothing to do (counter already
decremented, event already published).

Without Plan 2, this plan alone is still valuable: `term()` ensures actors
are stopped between iterations, preventing resource leaks. But the io-uring
backend will still run until process exit (since `term()` doesn't call
`shutdown_uring_backend()` today).

## Impact on Benchmark Timing

- `ctx.term()` adds ~10-50ms per iteration (waiting for actors to stop).
- This replaces the existing `sleep(100ms)`, so net impact is **negative**
  (faster teardown, with deterministic cleanup instead of a fixed sleep).
- The teardown time is NOT included in criterion's measurement (it runs outside
  the measured iteration).

## Testing

1. `cargo bench -- --test` (dry run) -- verify all benchmarks compile and
   teardown runs without errors.
2. `cargo bench --features="io-uring" -- --test` -- verify io-uring benchmarks
   also complete cleanly.
3. Full benchmark run: `cargo bench` and `cargo bench --features="io-uring"`.

## Risk Assessment

**Very low risk.** `Context::term()` is already implemented and used by
`generic_client_benchmark.rs`. This is purely adding a missing cleanup call
that should have been there from the start. The only risk is if `term()`
itself hangs due to actor bugs, but that would indicate a pre-existing bug
that needs separate fixing.

## Dependencies

- Independent of Plans 1-3, but benefits from Plan 2 (which adds io-uring
  shutdown in `term()`).
- Can be implemented first as a quick fix to reduce the hang window.
