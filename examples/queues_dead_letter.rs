//! # Queues Dead-Letter Queue (DLQ)
//!
//! Demonstrates dead-letter queue routing. A message is sent with
//! `max_receive_count=3` and a designated dead-letter channel. After three
//! peek attempts (simulating failed processing), the message is automatically
//! moved to the dead-letter queue.
//!
//! ## Expected Output
//!
//! ```text
//! Sent message with DLQ policy: id=<uuid>
//! Peek attempt 1: 1 messages
//! Peek attempt 2: 1 messages
//! Peek attempt 3: 1 messages
//! Dead-letter queue messages: 1
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_dead_letter
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

    let channel = "queues.dlq.example";
    let dlq_channel = "queues.dlq.dead_letters";

    // Send a message with max_receive_count=3 and dead-letter queue
    let msg = QueueMessageBuilder::new()
        .channel(channel)
        .body(b"may-fail-processing".to_vec())
        .max_receive_count(3)
        .max_receive_queue(dlq_channel)
        .build();

    let result = client.send_queue_message(msg).await?;
    println!("Sent message with DLQ policy: id={}", result.message_id);

    // Simulate receiving without ack (peek) 3 times to trigger DLQ
    for i in 0..3 {
        let msgs = client.receive_queue_messages(channel, 1, 2, true).await?;
        println!("Peek attempt {}: {} messages", i + 1, msgs.len());
    }

    // Check dead-letter queue
    let dlq_msgs = client
        .receive_queue_messages(dlq_channel, 10, 2, false)
        .await?;
    println!("Dead-letter queue messages: {}", dlq_msgs.len());

    client.close().await?;
    Ok(())
}
