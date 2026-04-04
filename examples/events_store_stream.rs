//! # Events Store Stream
//!
//! Demonstrates high-throughput persistent event publishing using a streaming
//! channel. Events are pushed through a gRPC stream for lower latency and
//! each is persisted to the store. Results are checked for send failures.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 100 events via store stream to channel: events_store.stream.example
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_stream
//! ```
use kubemq::prelude::*;
use kubemq::EventStoreBuilder;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let mut stream = client.send_event_store_stream().await?;
    let channel = "events_store.stream.example";

    // Stream 100 persistent events
    for i in 0..100 {
        let event = EventStoreBuilder::new()
            .channel(channel)
            .metadata(format!("store-stream-{}", i))
            .body(format!("persistent-payload-{}", i).into_bytes())
            .build();

        stream.send(event).await?;
    }

    println!("Sent 100 events via store stream to channel: {}", channel);

    // Check for results
    while let Ok(result) = stream.results().try_recv() {
        if !result.sent {
            eprintln!(
                "Send failed: id={}, error={}",
                result.event_id, result.error
            );
        }
    }

    stream.close();
    client.close().await?;
    Ok(())
}
