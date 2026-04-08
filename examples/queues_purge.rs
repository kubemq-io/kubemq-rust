//! # Queues — Purge Queue
//!
//! Demonstrates purging all messages from a queue channel using
//! `ack_all_queue_messages`. After sending several messages, the queue is
//! purged and a subsequent receive confirms it is empty.
//!
//! ## Expected Output
//!
//! ```text
//! Sent 5 messages to queue
//! Purged queue: queues.purge.example
//! After purge: 0 messages
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_purge
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

    let channel = "queues.purge.example";

    // Send messages
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("purge-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }
    println!("Sent 5 messages to queue");

    // Purge the queue
    client.ack_all_queue_messages(channel).await?;
    println!("Purged queue: {}", channel);

    // Verify empty
    let remaining = client.receive_queue_messages(channel, 10, 2, false).await?;
    println!("After purge: {} messages", remaining.len());

    client.close().await?;
    Ok(())
}
