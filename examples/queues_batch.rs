//! # Queues Batch Send and Receive
//!
//! Demonstrates sending and receiving queue messages in batches. Ten messages
//! are sent in a single batch call, then all are received together. Batch
//! operations reduce round-trips compared to individual sends.
//!
//! ## Expected Output
//!
//! ```text
//! Batch sent: id=<uuid>, is_error=false
//! Batch sent: id=<uuid>, is_error=false
//! ...
//! Received 10 messages from batch
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_batch
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

    let channel = "queues.batch.example";

    // Build a batch of messages
    let messages: Vec<QueueMessage> = (0..10)
        .map(|i| {
            QueueMessageBuilder::new()
                .channel(channel)
                .body(format!("batch-message-{}", i).into_bytes())
                .build()
        })
        .collect();

    // Send batch
    let results = client.send_queue_messages(messages).await?;
    for r in &results {
        println!("Batch sent: id={}, is_error={}", r.message_id, r.is_error);
    }

    // Receive all
    let received = client.receive_queue_messages(channel, 10, 5, false).await?;
    println!("Received {} messages from batch", received.len());

    client.close().await?;
    Ok(())
}
