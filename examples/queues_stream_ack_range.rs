//! # Queue Stream Selective Ack (By Range)
//!
//! Demonstrates selectively acknowledging individual messages from a poll batch.
//! Five messages are polled; only the first three are acknowledged while the
//! remaining two are left unacknowledged (and will be redelivered).
//!
//! ## Expected Output
//!
//! ```text
//! Received 5 messages
//! Acked message at index 0
//! Acked message at index 1
//! Acked message at index 2
//! Skipping ack for message at index 3
//! Skipping ack for message at index 4
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_stream_ack_range
//! ```
use kubemq::prelude::*;
use kubemq::{PollRequest, QueueMessageBuilder};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.ackrange.example";

    // Send messages
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("range-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    let mut receiver = client.new_queue_downstream_receiver().await?;

    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 5,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;
    println!("Received {} messages", response.messages.len());

    // Ack only the first 3 messages individually
    for (i, msg) in response.messages.iter().enumerate() {
        if i < 3 {
            msg.ack().await?;
            println!("Acked message at index {}", i);
        } else {
            println!("Skipping ack for message at index {}", i);
        }
    }

    receiver.close().await?;
    client.close().await?;
    Ok(())
}
