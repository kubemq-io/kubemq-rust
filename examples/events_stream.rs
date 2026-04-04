//! # Events Stream
//!
//! Demonstrates high-throughput event publishing using a streaming channel.
//! Instead of individual send calls, events are pushed through a persistent
//! gRPC stream for lower latency. Sends 100 events and checks for stream errors.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 100 events via stream to channel: events.stream.example
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_stream
//! ```
use kubemq::prelude::*;
use kubemq::EventBuilder;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let mut stream = client.send_event_stream().await?;
    let channel = "events.stream.example";

    // Send 100 events via the stream
    for i in 0..100 {
        let event = EventBuilder::new()
            .channel(channel)
            .metadata(format!("stream-event-{}", i))
            .body(format!("payload-{}", i).into_bytes())
            .build();

        stream.send(event).await?;
    }

    println!("Sent 100 events via stream to channel: {}", channel);

    // Check for any stream errors
    while let Ok(err) = stream.errors().try_recv() {
        eprintln!("Stream error: {}", err);
    }

    stream.close();
    client.close().await?;
    Ok(())
}
