//! # Queues Acknowledge All
//!
//! Demonstrates bulk-acknowledging all messages in a queue channel. Five
//! messages are sent, then `ack_all_queue_messages` removes them all in a
//! single operation. Useful for clearing a queue or resetting after errors.
//!
//! ## Expected Output
//!
//! ```text
//! Ack all: affected=5, is_error=false
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example queues_ack_all
//! ```
use kubemq::prelude::*;
use kubemq::{AckAllQueueMessagesRequest, QueueMessageBuilder};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.ackall.example";

    // Send some messages
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("ack-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    // Acknowledge all messages
    let req = AckAllQueueMessagesRequest {
        request_id: String::new(),
        client_id: String::new(),
        channel: channel.to_string(),
        wait_time_seconds: 5,
    };

    let resp = client.ack_all_queue_messages(&req).await?;
    println!(
        "Ack all: affected={}, is_error={}",
        resp.affected_messages, resp.is_error
    );

    client.close().await?;
    Ok(())
}
