//! # Queue Stream Upstream (Batch Send)
//!
//! Demonstrates the queue upstream stream for high-throughput batch sending.
//! A batch of 10 messages is sent through the upstream gRPC stream in a single
//! operation, and the batch result is checked for errors.
//!
//! ## Expected Output
//!
//! ```text
//! Sent batch via upstream stream
//! Batch result: ref_id=batch-001, is_error=false, items=10
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_upstream
//! ```
use kubemq::prelude::*;
use kubemq::QueueMessageBuilder;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.upstream.example";

    let mut upstream = client.queue_upstream().await?;

    // Send a batch of messages
    let messages: Vec<QueueMessage> = (0..10)
        .map(|i| {
            QueueMessageBuilder::new()
                .channel(channel)
                .body(format!("upstream-msg-{}", i).into_bytes())
                .build()
        })
        .collect();

    upstream.send("batch-001", messages).await?;
    println!("Sent batch via upstream stream");

    // Check results
    if let Some(result) = upstream.results().recv().await {
        println!(
            "Batch result: ref_id={}, is_error={}, items={}",
            result.ref_request_id,
            result.is_error,
            result.results.len()
        );
    }

    upstream.close();
    client.close().await?;
    Ok(())
}
