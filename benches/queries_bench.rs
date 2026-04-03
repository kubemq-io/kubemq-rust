//! Benchmarks for Queries (RPC with Cache) messaging pattern.
use criterion::{criterion_group, criterion_main, Criterion};
use kubemq::{KubemqClient, QueryBuilder, QueryReplyBuilder};
use std::time::Duration;

async fn create_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await
        .expect("Failed to connect to KubeMQ broker")
}

fn query_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(create_client());
    let channel = "bench.queries.roundtrip";

    // Set up responder
    let responder_client = client.clone();
    let _sub = rt.block_on(async {
        client
            .subscribe_to_queries(
                channel,
                "",
                move |query| {
                    let rc = responder_client.clone();
                    Box::pin(async move {
                        let reply = QueryReplyBuilder::new()
                            .request_id(&query.id)
                            .response_to(&query.response_to)
                            .body(b"bench-response".to_vec())
                            .build();
                        tokio::spawn(async move {
                            let _ = rc.send_query_response(reply).await;
                        });
                    })
                },
                None,
            )
            .await
            .unwrap()
    });

    // Wait for subscription
    rt.block_on(tokio::time::sleep(Duration::from_millis(500)));

    c.bench_function("query_roundtrip", |b| {
        b.to_async(&rt).iter(|| {
            let query = QueryBuilder::new()
                .channel(channel)
                .body(b"bench-query".to_vec())
                .timeout(Duration::from_secs(5))
                .build();
            let c = client.clone();
            async move {
                c.send_query(query).await.unwrap();
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
    targets = query_roundtrip
}
criterion_main!(benches);
