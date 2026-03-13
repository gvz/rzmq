# UDP io_uring Backend — Test Plan

## Test File: `core/tests/udp_iouring_radio_dish.rs`

**Feature gate:** `#[cfg(all(feature = "udp", feature = "io-uring"))]`

All tests require a Linux kernel 6.0+ with io_uring support.

## Test Infrastructure

### Helper Function

```rust
/// Creates a Radio socket with io_uring UDP enabled.
async fn create_uring_radio(ctx: &Context) -> Socket {
    let radio = ctx.socket(SocketType::Radio).await.unwrap();
    radio.set_option(IO_URING_UDP_ENABLED, 1).await.unwrap();
    radio
}

/// Creates a Dish socket with io_uring UDP enabled.
async fn create_uring_dish(ctx: &Context) -> Socket {
    let dish = ctx.socket(SocketType::Dish).await.unwrap();
    dish.set_option(IO_URING_UDP_ENABLED, 1).await.unwrap();
    dish
}
```

## Test Cases

### 1. Basic Unicast (Radio bind, Dish connect)

```rust
#[tokio::test]
async fn test_uring_udp_unicast_radio_bind_dish_connect() {
    // Radio binds to 127.0.0.1:0 (ephemeral)
    // Dish connects to Radio's bound address
    // Radio sends message with group "test"
    // Dish joins "test", receives message
    // Verify payload matches
}
```

### 2. Basic Unicast (Radio connect, Dish bind)

```rust
#[tokio::test]
async fn test_uring_udp_unicast_radio_connect_dish_bind() {
    // Dish binds to 127.0.0.1:0
    // Radio connects to Dish's bound address
    // Radio sends, Dish receives
}
```

### 3. Group Filtering

```rust
#[tokio::test]
async fn test_uring_udp_group_filtering() {
    // Dish joins "alpha" but not "beta"
    // Radio sends to both groups
    // Verify Dish only receives "alpha" messages
}
```

### 4. Multiple Datagrams

```rust
#[tokio::test]
async fn test_uring_udp_multiple_datagrams() {
    // Send 100 datagrams
    // Verify all received (order may vary for UDP, but loopback is reliable)
}
```

### 5. Large Datagram

```rust
#[tokio::test]
async fn test_uring_udp_large_datagram() {
    // Send a datagram near the 65507-byte limit
    // Verify received correctly
}
```

### 6. Empty Payload

```rust
#[tokio::test]
async fn test_uring_udp_empty_payload() {
    // Send a datagram with empty payload (group only)
    // Verify received
}
```

### 7. Message Too Large

```rust
#[tokio::test]
async fn test_uring_udp_message_too_large() {
    // Attempt to send >65507 bytes
    // Verify ZmqError::MessageTooLarge returned
}
```

### 8. Multiple Dishes

```rust
#[tokio::test]
async fn test_uring_udp_multiple_dishes() {
    // One Radio, two Dishes both bound/connected
    // Radio sends, both Dishes receive
    // (Requires broadcast or multicast, or two separate connections)
}
```

### 9. Graceful Shutdown

```rust
#[tokio::test]
async fn test_uring_udp_graceful_shutdown() {
    // Create Radio + Dish with io_uring
    // Exchange a message
    // Drop Radio socket
    // Verify Dish can still be dropped cleanly
    // Verify no thread leaks
}
```

### 10. Context Shutdown with Active io_uring UDP

```rust
#[tokio::test]
async fn test_uring_udp_context_shutdown() {
    // Create context, Radio, Dish with io_uring
    // Exchange messages
    // Shutdown context
    // Verify all threads and actors cleaned up
}
```

### 11. Socket Option Validation

```rust
#[tokio::test]
async fn test_uring_udp_option_defaults() {
    // Create socket
    // Verify IO_URING_UDP_ENABLED defaults to false
    // Verify IO_URING_UDP_SNDZEROCOPY defaults to false
    // Set IO_URING_UDP_ENABLED = true, verify get returns true
}
```

### 12. Fallback When io_uring Disabled

```rust
#[tokio::test]
async fn test_udp_works_without_uring_option() {
    // Create Radio + Dish WITHOUT setting IO_URING_UDP_ENABLED
    // Verify they use the Tokio path (works normally)
    // This test validates backward compatibility
}
```

### 13. Zero-Copy Send

```rust
#[tokio::test]
async fn test_uring_udp_zerocopy_send() {
    // Create Radio with IO_URING_UDP_SNDZEROCOPY = true
    // Send messages
    // Verify all received correctly
    // (Verifies the two-CQE ZC flow works)
}
```

### 14. High Throughput Burst

```rust
#[tokio::test]
async fn test_uring_udp_burst_throughput() {
    // Send 10000 datagrams rapidly
    // Verify >= 95% received (UDP can drop under extreme load)
    // Verify no panics or hangs
}
```

### 15. IPv6 Unicast

```rust
#[tokio::test]
async fn test_uring_udp_ipv6_unicast() {
    // Radio binds to [::1]:0
    // Dish connects to [::1]:port
    // Exchange message over IPv6 loopback
}
```

## Benchmark File: `core/benches/udp_throughput.rs`

### Benchmark Groups

```rust
fn bench_udp_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("udp_throughput");

    // Small messages (64 bytes)
    group.bench_function("tokio_64b", |b| { ... });
    group.bench_function("uring_64b", |b| { ... });

    // Medium messages (1KB)
    group.bench_function("tokio_1kb", |b| { ... });
    group.bench_function("uring_1kb", |b| { ... });

    // Large messages (8KB)
    group.bench_function("tokio_8kb", |b| { ... });
    group.bench_function("uring_8kb", |b| { ... });

    // Large messages with ZC
    group.bench_function("uring_8kb_zc", |b| { ... });

    group.finish();
}

fn bench_udp_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("udp_latency");

    group.bench_function("tokio_roundtrip", |b| { ... });
    group.bench_function("uring_roundtrip", |b| { ... });

    group.finish();
}
```

### Benchmark Setup Pattern

```rust
// Each benchmark:
// 1. Creates a Tokio runtime
// 2. Creates Context
// 3. Creates Radio + Dish (with appropriate options)
// 4. Warms up with a few messages
// 5. Runs the benchmark iteration (send N messages, recv N)
// 6. Cleans up
```

## Running Tests

```bash
# Run all UDP io_uring tests
cargo test --features "udp,io-uring" --test udp_iouring_radio_dish

# Run with tracing for debugging
RUST_LOG=debug cargo test --features "udp,io-uring" --test udp_iouring_radio_dish -- --nocapture

# Run benchmarks
cargo bench --features "udp,io-uring" --bench udp_throughput

# Full regression (all features)
cargo test --features "full-linux"
```

## Test Dependencies

The tests use the same test infrastructure as existing tests:
- `tokio::test` runtime
- `serial_test` for port-sharing safety (if needed)
- No external processes or network access (all loopback)
- Kernel 6.0+ required (tests should be skipped gracefully on older kernels)

### Kernel Version Check

```rust
fn kernel_supports_multishot_recvmsg() -> bool {
    // Check kernel version >= 6.0 or probe io_uring features
    // If not supported, skip the test with a message
    let uname = nix::sys::utsname::uname().unwrap();
    let release = uname.release().to_str().unwrap_or("0.0");
    let parts: Vec<u32> = release.split('.').take(2)
        .filter_map(|s| s.parse().ok()).collect();
    parts.len() >= 2 && (parts[0] > 6 || (parts[0] == 6 && parts[1] >= 0))
}
```

Tests that require multishot should call this check and skip with a message
if unsupported.
