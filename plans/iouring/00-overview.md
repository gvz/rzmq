# io-uring Benchmark Hang: Fix Plans Overview

## Root Cause Summary

`cargo bench --features="io-uring"` hangs because the io-uring worker OS thread
and upstream processor tokio task run forever. They are initialized as
process-level singletons on the first `Context::new()` and are never shut down
because:

1. Benchmarks don't call `ctx.term()` during teardown.
2. `Context` has no `Drop` impl to trigger cleanup.
3. `Context::term()` doesn't call `shutdown_uring_backend()`.
4. The upstream processor tokio task blocks the runtime from exiting.

Additionally, the eventfd-based wakeup mechanism is broken (poll SQE never
submitted), causing the worker to busy-loop with exponential backoff instead
of sleeping efficiently.

## Plans

| # | File | Priority | Risk | Description |
|---|------|----------|------|-------------|
| 1 | [01-fix-eventfd-poll-sqe-submission.md](01-fix-eventfd-poll-sqe-submission.md) | High | Low | Call `try_submit_initial_poll_sqe()` in worker main loop |
| 2 | [02-context-drop-and-iouring-lifecycle.md](02-context-drop-and-iouring-lifecycle.md) | High | Medium | Add `Drop` for `ContextInner`, trigger io-uring shutdown |
| 3 | [03-stoppable-upstream-processor.md](03-stoppable-upstream-processor.md) | High | Low | Add cancellation signal to upstream processor task |
| 4 | [04-fix-benchmark-teardown.md](04-fix-benchmark-teardown.md) | High | Very Low | Add `ctx.term().await` to all benchmark teardown functions |
| 5 | [05-global-state-reinitialization.md](05-global-state-reinitialization.md) | Medium | Medium | Replace `OnceCell` with resettable init for backend reuse |

## Recommended Implementation Order

```
Plan 1 (eventfd fix)           -- independent, fixes wakeup bug
    |
Plan 4 (benchmark teardown)    -- independent, quick fix
    |
Plan 3 (stoppable processor)   -- needed by Plan 2
    |
Plan 2 (context lifecycle)     -- needs Plan 3
    |
Plan 5 (re-initialization)     -- needs Plans 2+3
```

**Minimum fix to unblock benchmarks:** Plans 3 + 4 together. Adding
`ctx.term()` to teardown (Plan 4) combined with a stoppable processor (Plan 3)
ensures the tokio runtime can shut down between samples.

**Complete fix:** All 5 plans. This gives robust lifecycle management that works
for benchmarks, tests, and production use.

## Key Files

| File | Role |
|------|------|
| `core/src/io_uring_backend/worker/main_loop.rs` | Worker main loop (Plan 1) |
| `core/src/io_uring_backend/worker/eventfd_poller.rs` | EventFd poll SQE (Plan 1) |
| `core/src/io_uring_backend/worker/mod.rs` | Worker struct + spawn (Plans 1,2) |
| `core/src/io_uring_backend/worker/cqe_processor.rs` | CQE handling (reference) |
| `core/src/uring/mod.rs` | Backend init/shutdown (Plans 2,3,5) |
| `core/src/uring/global_state.rs` | Global statics + processor (Plans 2,3,5) |
| `core/src/context.rs` | Context lifecycle (Plans 2,4) |
| `core/benches/*.rs` | Benchmark teardown (Plan 4) |
