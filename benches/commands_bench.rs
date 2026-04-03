//! Benchmarks for Commands (RPC) messaging pattern.
use criterion::{criterion_group, criterion_main, Criterion};
use kubemq::{CommandBuilder, CommandReplyBuilder, KubemqClient};
use std::time::Duration;

async fn create_client() -> KubemqClient {
    KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await
        .expect("Failed to connect to KubeMQ broker")
}

fn command_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let client = rt.block_on(create_client());
    let channel = "bench.commands.roundtrip";

    // Set up responder
    let responder_client = client.clone();
    let _sub = rt.block_on(async {
        client
            .subscribe_to_commands(
                channel,
                "",
                move |cmd| {
                    let rc = responder_client.clone();
                    Box::pin(async move {
                        let reply = CommandReplyBuilder::new()
                            .request_id(&cmd.id)
                            .response_to(&cmd.response_to)
                            .build();
                        tokio::spawn(async move {
                            let _ = rc.send_command_response(reply).await;
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

    c.bench_function("command_roundtrip", |b| {
        b.to_async(&rt).iter(|| {
            let cmd = CommandBuilder::new()
                .channel(channel)
                .body(b"bench-command".to_vec())
                .timeout(Duration::from_secs(5))
                .build();
            let c = client.clone();
            async move {
                c.send_command(cmd).await.unwrap();
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
    targets = command_roundtrip
}
criterion_main!(benches);
