use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rzmq::socket::options::JOIN;
use rzmq::{Context, Msg, SocketType};
use std::hint::black_box;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const BIND_ADDR_BASE: &str = "udp://127.0.0.1";
static NEXT_BENCH_PORT: AtomicU16 = AtomicU16::new(5900);

const MESSAGES_PER_SAMPLE: usize = 10000;
const WARMUP_MESSAGES: usize = 1000;
const STATS_SAMPLES: usize = 1000;

fn create_setup(rt: &Runtime, msg_size: usize) -> (Context, rzmq::Socket, rzmq::Socket) {
  rt.block_on(async {
    let ctx = Context::new().expect("Failed to create context");

    let port = NEXT_BENCH_PORT.fetch_add(1, Ordering::Relaxed);
    let bind_addr = format!("{}:{}", BIND_ADDR_BASE, port);

    let dish = ctx
      .socket(SocketType::Dish)
      .expect("Failed to create Dish socket");
    dish.bind(&bind_addr).await.expect("Failed to bind Dish");
    dish
      .set_option_raw(JOIN, b"latency")
      .await
      .expect("Failed to join group");

    let radio = ctx
      .socket(SocketType::Radio)
      .expect("Failed to create Radio socket");
    radio
      .connect(&bind_addr)
      .await
      .expect("Failed to connect Radio");

    tokio::time::sleep(Duration::from_millis(50)).await;

    for _ in 0..WARMUP_MESSAGES {
      let mut msg = Msg::from_vec(vec![0x42u8; msg_size]);
      msg.set_group("latency".to_string()).unwrap();
      let _ = radio.send(msg).await;
      let _ = dish.recv().await;
    }

    (ctx, radio, dish)
  })
}

fn measure_latency(
  rt: &Runtime,
  radio: &rzmq::Socket,
  dish: &rzmq::Socket,
  msg_size: usize,
) -> Duration {
  rt.block_on(async {
    let start = Instant::now();

    for i in 0..MESSAGES_PER_SAMPLE {
      let mut msg = Msg::from_vec(vec![0x42u8; msg_size]);
      msg.set_group("latency".to_string()).unwrap();
      radio.send(msg).await.expect("Send failed");
      let reply = dish.recv().await.expect("Recv failed");
      black_box(reply);
      black_box(i);
    }

    start.elapsed() / MESSAGES_PER_SAMPLE as u32
  })
}

fn collect_latencies(
  rt: &Runtime,
  radio: &rzmq::Socket,
  dish: &rzmq::Socket,
  msg_size: usize,
  samples: usize,
) -> Vec<Duration> {
  rt.block_on(async {
    let mut latencies = Vec::with_capacity(samples);

    for _ in 0..samples {
      let start = Instant::now();
      let mut msg = Msg::from_vec(vec![0x42u8; msg_size]);
      msg.set_group("latency".to_string()).unwrap();
      radio.send(msg).await.expect("Send failed");
      let reply = dish.recv().await.expect("Recv failed");
      latencies.push(start.elapsed());
      black_box(reply);
    }

    latencies
  })
}

fn calculate_stats(latencies: &[Duration]) {
  let mut sorted: Vec<Duration> = latencies.to_vec();
  sorted.sort();

  let n = sorted.len();
  let min = sorted[0];
  let max = sorted[n - 1];
  let mean = sorted.iter().sum::<Duration>() / n as u32;
  let p50 = sorted[n / 2];
  let p99 = sorted[(n as f64 * 0.99) as usize];
  let p999 = sorted[(n as f64 * 0.999) as usize];

  println!("\n=== UDP RADIO-DISH One-Way Latency ({:?}) ===", mean);
  println!("samples: {}", n);
  println!("min:     {:?}", min);
  println!("max:     {:?}", max);
  println!("mean:    {:?}", mean);
  println!("p50:     {:?}", p50);
  println!("p99:     {:?}", p99);
  println!("p999:    {:?}", p999);
}

fn latency_benchmark(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create runtime");

  let msg_sizes = [64, 256, 512, 1024, 1500];

  let mut group = c.benchmark_group("udp_radio_dish_latency");
  group.warm_up_time(Duration::from_secs(1));
  group.measurement_time(Duration::from_secs(5));
  group.sample_size(50);

  for size in msg_sizes {
    let (_ctx, radio, dish) = create_setup(&rt, size);

    group.bench_with_input(BenchmarkId::new("one_way", size), &size, |b, &size| {
      b.iter(|| measure_latency(&rt, &radio, &dish, size));
    });
  }

  let (_ctx, radio, dish) = create_setup(&rt, 256);
  let latencies = collect_latencies(&rt, &radio, &dish, 256, STATS_SAMPLES);
  calculate_stats(&latencies);

  group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = latency_benchmark
}
criterion_main!(benches);
