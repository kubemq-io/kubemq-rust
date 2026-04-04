//! # Queues Peek
//!
//! Demonstrates peeking at queue messages without consuming them. Peeked
//! messages remain in the queue and are still available for a subsequent
//! receive call. Useful for inspecting queue contents without side effects.
//!
//! ## Expected Output
//!
//! ```text
//! Peeked: id=<uuid>, body=peek-me
//! Peeked 1 messages (still in queue)
//! Received 1 messages after peek
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_peek
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

    let channel = "queues.peek.example";

    // Send a message first
    let msg = QueueMessageBuilder::new()
        .channel(channel)
        .body(b"peek-me".to_vec())
        .build();
    client.send_queue_message(msg).await?;

    // Peek -- messages remain in the queue
    let peeked = client.receive_queue_messages(channel, 10, 5, true).await?;

    for m in &peeked {
        println!(
            "Peeked: id={}, body={}",
            m.id,
            String::from_utf8_lossy(&m.body)
        );
    }
    println!("Peeked {} messages (still in queue)", peeked.len());

    // Messages are still available for receive
    let received = client.receive_queue_messages(channel, 10, 5, false).await?;
    println!("Received {} messages after peek", received.len());

    client.close().await?;
    Ok(())
}
