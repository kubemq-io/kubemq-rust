//! Benchmarks for Queues messaging pattern.
use criterion::{criterion_group, criterion_main, Criterion};
use kubemq::{KubemqClient, PollRequest, QueueMessageBuilder};
use std::time::Duration;

async fn create_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await
        .expect("Failed to connect to KubeMQ broker")
}

fn queue_send_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(create_client());
    let channel = "bench.queues.send";

    c.bench_function("queue_send_throughput", |b| {
        b.to_async(&rt).iter(|| {
            let msg = QueueMessageBuilder::new()
                .channel(channel)
                .body(b"benchmark-queue-payload".to_vec())
                .build();
            let c = client.clone();
            async move {
                c.send_queue_message(msg).await.unwrap();
            }
        });
    });

    rt.block_on(client.close()).unwrap();
}

fn queue_poll_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(create_client());
    let channel = "bench.queues.poll";

    // Pre-populate queue
    rt.block_on(async {
        for _ in 0..1000 {
            let msg = QueueMessageBuilder::new()
                .channel(channel)
                .body(b"benchmark-queue-poll".to_vec())
                .build();
            client.send_queue_message(msg).await.unwrap();
        }
    });

    c.bench_function("queue_poll_throughput", |b| {
        b.to_async(&rt).iter(|| {
            let c = client.clone();
            async move {
                let poll = PollRequest {
                    channel: channel.to_string(),
                    max_items: 1,
                    wait_timeout_seconds: 1,
                    auto_ack: true,
                };
                // Use poll_queue (one-shot) for benchmark simplicity with &mut self poll.
                let _ = c.poll_queue(poll).await;
            }
        });
    });

    rt.block_on(client.close()).unwrap();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(5))
        .sample_size(100);
    targets = queue_send_throughput, queue_poll_throughput
}
criterion_main!(benches);
