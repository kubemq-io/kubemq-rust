//! Example: Acknowledge all messages in a queue.
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
