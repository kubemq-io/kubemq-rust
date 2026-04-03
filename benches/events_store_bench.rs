//! Benchmarks for Events Store messaging pattern.
use criterion::{criterion_group, criterion_main, Criterion};
use kubemq::{EventStoreBuilder, KubemqClient};
use std::time::Duration;

async fn create_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await
        .expect("Failed to connect to KubeMQ broker")
}

fn events_store_send_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(create_client());
    let channel = "bench.events_store.send";

    c.bench_function("events_store_send_throughput", |b| {
        b.to_async(&rt).iter(|| {
            let event = EventStoreBuilder::new()
                .channel(channel)
                .body(b"benchmark-store-payload".to_vec())
                .build();
            let c = client.clone();
            async move {
                c.send_event_store(event).await.unwrap();
            }
        });
    });

    rt.block_on(client.close()).unwrap();
}

fn events_store_stream_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(create_client());
    let channel = "bench.events_store.stream";

    let stream = rt.block_on(client.send_event_store_stream()).unwrap();

    c.bench_function("events_store_stream_throughput", |b| {
        b.to_async(&rt).iter(|| {
            let event = EventStoreBuilder::new()
                .channel(channel)
                .body(b"benchmark-store-stream".to_vec())
                .build();
            let s = &stream;
            async move {
                s.send(event).await.unwrap();
            }
        });
    });

    stream.close();
    rt.block_on(client.close()).unwrap();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(3))
        .measurement_time(Duration::from_secs(5))
        .sample_size(100);
    targets = events_store_send_throughput, events_store_stream_throughput
}
criterion_main!(benches);
