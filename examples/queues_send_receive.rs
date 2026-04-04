//! # Queues Send and Receive
//!
//! Demonstrates the basic queue message pattern: send a single message to a
//! queue channel, then receive and consume it. Messages are pulled (not pushed)
//! and automatically acknowledged on receive.
//!
//! ## Expected Output
//!
//! ```text
//! Sent: message_id=<uuid>, sent_at=<timestamp>
//! Received: id=<uuid>, body=Hello Queue!
//! Total received: 1
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_send_receive
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

    let channel = "queues.simple.example";

    // Send a message
    let msg = QueueMessageBuilder::new()
        .channel(channel)
        .body(b"Hello Queue!".to_vec())
        .metadata("queue-metadata")
        .build();

    let result = client.send_queue_message(msg).await?;
    println!(
        "Sent: message_id={}, sent_at={}",
        result.message_id, result.sent_at
    );

    // Receive messages
    let messages = client.receive_queue_messages(channel, 10, 5, false).await?;

    for m in &messages {
        println!(
            "Received: id={}, body={}",
            m.id,
            String::from_utf8_lossy(&m.body)
        );
    }

    println!("Total received: {}", messages.len());

    client.close().await?;
    Ok(())
}
