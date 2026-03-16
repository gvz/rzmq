#![allow(unused)]

use bench_matrix::{
  AbstractCombination, MatrixCellValue,
  criterion_runner::{
    ExtractorFn, GlobalSetupFn, GlobalTeardownFn,
    async_suite::{AsyncBenchmarkLogicFn, AsyncBenchmarkSuite, AsyncSetupFn, AsyncTeardownFn},
  },
};
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use rzmq::{Context, Msg, SocketType, socket::options::{JOIN, RCVHWM, SNDHWM}};
use std::{
  future::Future,
  pin::Pin,
  sync::Arc,
  sync::atomic::{AtomicU16, Ordering as AtomicOrdering},
  time::{Duration, Instant},
};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

const UDP_BASE_PORT: u16 = 7900;
const BENCH_HWM: i32 = 100_000;

static NEXT_BENCH_PORT: AtomicU16 = AtomicU16::new(0);

#[derive(Debug, Clone)]
pub struct ConfigUdp {
  pub msg_size: usize,
  pub num_messages: usize,
}

#[derive(Default, Debug)]
struct BenchContext {}

struct BenchState {
  ctx_arc: Arc<Context>,
  radio_socket: rzmq::Socket,
  dish_socket: rzmq::Socket,
  message_payload: Vec<u8>,
}

fn extract_config(combo: &AbstractCombination) -> Result<ConfigUdp, String> {
  let msg_size = combo.get_u64(0)? as usize;
  let num_messages = combo.get_u64(1)? as usize;
  Ok(ConfigUdp { msg_size, num_messages })
}

fn setup_udp_bench(
  _runtime: &Runtime,
  cfg: &ConfigUdp,
) -> Pin<Box<dyn Future<Output = Result<(BenchContext, BenchState), String>> + Send>> {
  let cfg_clone = cfg.clone();
  Box::pin(async move {
    let ctx = Context::new().map_err(|e| format!("Zmq Context creation failed: {}", e))?;
    let ctx_arc = Arc::new(ctx);

    let port = UDP_BASE_PORT + NEXT_BENCH_PORT.fetch_add(1, AtomicOrdering::Relaxed) % 1000;
    let dish_addr = format!("udp://127.0.0.1:{}", port);
    let radio_addr = format!("udp://127.0.0.1:{}", port);

    let dish_socket = ctx_arc
      .socket(SocketType::Dish)
      .map_err(|e| e.to_string())?;
    dish_socket
      .set_option_raw(JOIN, b"bench")
      .await
      .map_err(|e| e.to_string())?;
    dish_socket
      .set_option(RCVHWM, BENCH_HWM)
      .await
      .map_err(|e| e.to_string())?;
    dish_socket
      .bind(&dish_addr)
      .await
      .map_err(|e| e.to_string())?;

    let radio_socket = ctx_arc
      .socket(SocketType::Radio)
      .map_err(|e| e.to_string())?;
    radio_socket
      .set_option(SNDHWM, BENCH_HWM)
      .await
      .map_err(|e| e.to_string())?;
    radio_socket
      .connect(&radio_addr)
      .await
      .map_err(|e| e.to_string())?;

    sleep(Duration::from_millis(50)).await;

    let message_payload = vec![0u8; cfg_clone.msg_size];

    Ok((
      BenchContext {},
      BenchState {
        ctx_arc,
        radio_socket,
        dish_socket,
        message_payload,
      },
    ))
  })
}

fn benchmark_logic(
  ctx: BenchContext,
  state: BenchState,
  cfg: &ConfigUdp,
) -> Pin<Box<dyn Future<Output = (BenchContext, BenchState, Duration)> + Send>> {
  let num_messages = cfg.num_messages;

  Box::pin(async move {
    let start_time = Instant::now();

    let radio = state.radio_socket.clone();
    let dish = state.dish_socket.clone();
    let payload = state.message_payload.clone();

    let send_task: JoinHandle<Result<(), rzmq::ZmqError>> = tokio::spawn(async move {
      for _ in 0..num_messages {
        let mut msg = Msg::from_vec(black_box(payload.clone()));
        msg.set_group("bench".to_string()).expect("group is valid");
        radio.send(msg).await?;
      }
      Ok(())
    });

    let recv_task: JoinHandle<usize> = tokio::spawn(async move {
      let mut received = 0;
      let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
      while received < num_messages {
        match timeout(deadline - tokio::time::Instant::now(), dish.recv()).await {
          Ok(Ok(_)) => received += 1,
          _ => break,
        }
      }
      received
    });

    send_task.await.expect("Send task panicked").expect("Send task returned error");
    recv_task.await.expect("Recv task panicked");
    let elapsed = start_time.elapsed();

    (ctx, state, elapsed)
  })
}

fn teardown_udp_bench(
  _ctx: BenchContext,
  state: BenchState,
  _runtime: &Runtime,
  _cfg: &ConfigUdp,
) -> Pin<Box<dyn Future<Output = ()> + Send>> {
  Box::pin(async move {
    let _ = state.dish_socket.close().await;
    let _ = state.radio_socket.close().await;

    // Properly terminate the context to ensure all actors are stopped
    // and resources are cleaned up before the next iteration.
    #[cfg(not(feature = "io-uring"))]
    {
      if let Err(e) = state.ctx_arc.term().await {
        eprintln!("[Teardown] Context termination failed: {}", e);
      }
    }
  })
}

fn udp_matrix_benchmark(c: &mut Criterion) {
  let rt = Runtime::new().expect("Failed to create Tokio runtime for benchmarks");

  let parameter_axes = vec![
    vec![
      MatrixCellValue::Unsigned(64),
      MatrixCellValue::Unsigned(1024),
      MatrixCellValue::Unsigned(8192),
    ],
    vec![
      MatrixCellValue::Unsigned(5000),
      MatrixCellValue::Unsigned(10000),
    ],
  ];
  let parameter_names = vec!["MsgSize".to_string(), "NumMessages".to_string()];

  let suite = AsyncBenchmarkSuite::new(
    c,
    &rt,
    "UDP_Throughput".to_string(),
    Some(parameter_names),
    parameter_axes,
    Box::new(extract_config),
    setup_udp_bench,
    benchmark_logic,
    teardown_udp_bench,
  )
  .configure_criterion_group(|group| {
    group
      .warm_up_time(Duration::from_secs(2))
      .measurement_time(Duration::from_secs(10))
      .sample_size(10);
  })
  .throughput(|cfg: &ConfigUdp| {
    Throughput::Bytes((cfg.msg_size * cfg.num_messages) as u64)
  });

  suite.run();
}

criterion_group!(benches, udp_matrix_benchmark);
criterion_main!(benches);