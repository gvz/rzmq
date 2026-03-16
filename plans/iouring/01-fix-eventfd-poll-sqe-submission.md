# Plan 1: Fix EventFd Poll SQE Submission

## Problem

The `EventFdPoller::try_submit_initial_poll_sqe()` method is fully implemented
(`core/src/io_uring_backend/worker/eventfd_poller.rs:73-115`) but is **never
called** anywhere in the codebase. This breaks the eventfd-based wakeup
mechanism that is supposed to allow the `SignalingOpSender` to wake the io-uring
worker thread from idle sleep.

### Current State

- **Writer side (works):** `SignalingOpSender::send()` and `try_send()`
  (`core/src/io_uring_backend/signaling_op_sender.rs:19,49`) write `1u64` to
  the eventfd after every op send.
- **CQE consumer side (works):** `cqe_processor::process_all_cqes()`
  (`core/src/io_uring_backend/worker/cqe_processor.rs:328-345`) checks every
  CQE against `worker.event_fd_poller.handle_cqe_if_matches(...)` as its
  **first** check, correctly consuming eventfd poll completions.
- **SQE submission side (BROKEN):** No code ever calls
  `try_submit_initial_poll_sqe()` to submit the `PollAdd` SQE to the ring. The
  SQE is never submitted, so the CQE never fires, so the worker never wakes via
  eventfd.

### Consequence

Without the eventfd poll SQE, the worker relies entirely on the polling timeout
backoff strategy (`main_loop.rs:609-612`) with exponential backoff from 1ms to
128ms. This means:

1. New operations are not processed until the next timeout expires (up to 128ms
   latency).
2. The worker busy-loops during idle periods instead of efficiently sleeping on
   eventfd.
3. When the worker should be idle/blocked in `submit_with_args`, it can't be
   woken immediately by new work arriving.

## Files to Modify

- `core/src/io_uring_backend/worker/main_loop.rs`

## Implementation

### Step 1: Submit initial eventfd poll SQE before the main loop

Insert a call to `try_submit_initial_poll_sqe` at the start of
`run_worker_loop`, before entering the `while worker.state != WorkerState::Stopped`
loop. This ensures the eventfd is being polled from the very beginning.

**Location:** `main_loop.rs`, after line 396 (before line 398 `while` loop).

```rust
// Submit initial eventfd poll SQE before entering main loop
{
    let mut sq = unsafe { worker.ring.submission_shared() };
    if !worker.event_fd_poller.try_submit_initial_poll_sqe(&mut sq) {
        warn!("[UringWorker] Failed to submit initial eventfd poll SQE");
    }
    drop(sq);
    // Flush the initial poll SQE to the kernel
    let _ = worker.ring.submitter().submit();
}
```

### Step 2: Re-submit eventfd poll SQE in the Running state main loop

After `handle_cqe_if_matches` fires and sets `is_poll_submitted = false` (and
generates a new `current_poll_user_data`), we need to re-submit the poll SQE in
the next iteration. The best place is between **Phase 3 (Ensure Reads)** and
**Phase 4 (Submit and Idle)**.

**Location:** `main_loop.rs`, after line 598 (`drop(sq)` ending Phase 3),
before line 600 (Phase 4).

```rust
// PHASE 3.5: RE-SUBMIT EVENTFD POLL IF NEEDED
if !worker.event_fd_poller.is_poll_submitted {
    let mut sq = unsafe { worker.ring.submission_shared() };
    worker.event_fd_poller.try_submit_initial_poll_sqe(&mut sq);
    drop(sq);
}
```

**Why this location:**
- It's after all work-generating phases (1-3), so the SQ may have space.
- It's before the submit/idle phase (4), so the poll SQE gets submitted
  together with any other pending SQEs in a single `submit()` call.
- The `try_submit_initial_poll_sqe` method is idempotent -- if `is_poll_submitted`
  is already `true`, it returns `true` immediately. But we add the outer check
  anyway to avoid acquiring the SQ unnecessarily.

### Step 3: Also re-submit in the Draining state

During `WorkerState::Draining`, the eventfd poller is NOT re-submitted because
`handle_cqe_if_matches` skips generating a new user_data when
`is_shutting_down == true` (eventfd_poller.rs:200-219). No change needed here.

## Interaction with Backoff

Currently the backoff logic (`main_loop.rs:659-667`) increases `kernel_poll_timeout`
when there's no work. With the eventfd poll SQE active:

- `submit_with_args(1, &submit_args)` at line 609 will now return immediately
  when the eventfd fires (because the PollAdd CQE completes), rather than
  waiting for the full timeout.
- The `needs_wait` condition (line 604-606) should still work correctly: when
  there's no work and no pending SQEs, the worker sleeps in
  `submit_with_args` but can be woken by the eventfd.
- The backoff timeout becomes a maximum sleep duration rather than the primary
  wakeup mechanism.

## Testing

1. `cargo test --features="io-uring"` -- all existing tests pass.
2. `cargo bench --features="io-uring"` -- benchmarks no longer exhibit added
   latency from missed wakeups.
3. Add a unit test that:
   - Spawns the io-uring worker.
   - Sends an op via `SignalingOpSender`.
   - Verifies the worker processes it within <5ms (vs. up to 128ms before).

## Risk Assessment

**Low risk.** The `try_submit_initial_poll_sqe` method is already fully
implemented and tested in isolation. The CQE handling path is already wired up.
This change only connects the missing SQE submission side. The method is
idempotent, so double-calls are safe. Failure to submit (SQ full) is non-fatal;
the worker falls back to the timeout-based polling.
