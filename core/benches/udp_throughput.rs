#![cfg(all(feature = "udp", feature = "io-uring"))]

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rzmq::{
    Context, Msg, SocketType,
    socket::options::{JOIN, RCVHWM, SNDHWM},
};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio::time::sleep;

const UDP_BASE_PORT: u16 = 7900;
static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

fn next_port() -> u16 {
    let port = UDP_BASE_PORT + NEXT_PORT.fetch_add(1, Ordering::Relaxed) % 1000;
    port
}

fn get_port_offset() -> u16 {
    NEXT_PORT.fetch_add(1, Ordering::Relaxed) % 1000
}

fn bench_udp_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut group = c.benchmark_group("udp_throughput");

        let test_cases = vec![
            ("small_64B", 64, 10000),
            ("medium_1KB", 1024, 5000),
            ("large_8KB", 8192, 1000),
        ];

        for (name, msg_size, num_msgs) in test_cases {
            group.throughput(Throughput::Bytes(msg_size as u64 * num_msgs as u64));

            let port = next_port();
            let dish_addr = format!("udp://127.0.0.1:{}", port);
            let radio_addr = format!("udp://127.0.0.1:{}", port);

            let bench_id = format!("{}_{}", name, get_port_offset());

            group.bench_function(&bench_id, |b| {
                b.to_async(&rt).iter(|| async {
                    let ctx = Context::new().unwrap();
                    let dish = ctx.socket(SocketType::Dish).unwrap();
                    dish.set_option_raw(JOIN, b"bench").await.unwrap();
                    dish.set_option(RCVHWM, 100000).await.unwrap();
                    dish.bind(&dish_addr).await.unwrap();

                    let radio = ctx.socket(SocketType::Radio).unwrap();
                    radio.set_option(SNDHWM, 100000).await.unwrap();
                    radio.connect(&radio_addr).await.unwrap();

                    sleep(Duration::from_millis(50)).await;

                    let payload = vec![0u8; msg_size];

                    let start = Instant::now();
                    for _ in 0..num_msgs {
                        let mut msg = Msg::from_vec(payload.clone());
                        msg.set_group("bench").unwrap();
                        radio.send(msg).await.unwrap();
                    }
                    let elapsed = start.elapsed();

                    let _ = ctx.term().await;

                    (num_msgs, elapsed)
                });
            });
        }

        group.finish();
    });
}

fn bench_udp_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut group = c.benchmark_group("udp_latency");
        group.measurement_time(Duration::from_secs(5));

        let msg_sizes = vec![64, 256, 1024];

        for msg_size in msg_sizes {
            group.throughput(Throughput::Bytes(msg_size as u64));

            let port = next_port();
            let dish_addr = format!("udp://127.0.0.1:{}", port);
            let radio_addr = format!("udp://127.0.0.1:{}", port);

            let bench_id = format!("{}_{}B", "roundtrip", msg_size);

            group.bench_function(&bench_id, |b| {
                b.to_async(&rt).iter(|| async {
                    let ctx = Context::new().unwrap();
                    
                    let dish = ctx.socket(SocketType::Dish).unwrap();
                    dish.set_option_raw(JOIN, b"latency").await.unwrap();
                    dish.bind(&dish_addr).await.unwrap();

                    let radio = ctx.socket(SocketType::Radio).unwrap();
                    radio.connect(&radio_addr).await.unwrap();

                    sleep(Duration::from_millis(50)).await;

                    let payload = vec![0u8; msg_size];

                    let start = Instant::now();
                    let mut msg = Msg::from_vec(payload);
                    msg.set_group("latency").unwrap();
                    radio.send(msg).await.unwrap();

                    let rx_start = Instant::now();
                    let _ = dish.recv().await;
                    let end = rx_start.elapsed();

                    let total = start.elapsed();

                    ctx.term().await.unwrap();

                    (end, total)
                });
            });
        }

        group.finish();
    });
}

fn bench_udp_burst(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let mut group = c.benchmark_group("udp_burst");
        
        let burst_sizes = vec![10, 50, 100];
        let msg_size = 64;

        for burst_size in burst_sizes {
            group.throughput(Throughput::Elements(burst_size as u64));

            let port = next_port();
            let dish_addr = format!("udp://127.0.0.1:{}", port);
            let radio_addr = format!("udp://127.0.0.1:{}", port);

            let bench_id = format!("burst_{}", burst_size);

            group.bench_function(&bench_id, |b| {
                b.to_async(&rt).iter(|| async {
                    let ctx = Context::new().unwrap();
                    
                    let dish = ctx.socket(SocketType::Dish).unwrap();
                    dish.set_option_raw(JOIN, b"burst").await.unwrap();
                    dish.set_option(RCVHWM, 100000).await.unwrap();
                    dish.bind(&dish_addr).await.unwrap();

                    let radio = ctx.socket(SocketType::Radio).unwrap();
                    radio.set_option(SNDHWM, 100000).await.unwrap();
                    radio.connect(&radio_addr).await.unwrap();

                    sleep(Duration::from_millis(50)).await;

                    let payload = vec![0u8; msg_size];
                    let mut msgs = Vec::with_capacity(burst_size);
                    for _ in 0..burst_size {
                        let mut msg = Msg::from_vec(payload.clone());
                        msg.set_group("burst").unwrap();
                        msgs.push(msg);
                    }

                    let start = Instant::now();
                    for msg in msgs {
                        radio.send(msg).await.unwrap();
                    }
                    let send_elapsed = start.elapsed();

                    let mut received = 0;
                    let timeout_at = Instant::now() + Duration::from_secs(2);
                    while received < burst_size && Instant::now() < timeout_at {
                        if dish.recv().await.is_ok() {
                            received += 1;
                        }
                    }

                    let total_elapsed = start.elapsed();

                    ctx.term().await.unwrap();

                    (send_elapsed, total_elapsed, received)
                });
            });
        }

        group.finish();
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(10));
    targets = bench_udp_throughput, bench_udp_latency, bench_udp_burst
);
criterion_main!(benches);
