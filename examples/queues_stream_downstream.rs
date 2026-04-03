//! Example: Queue stream downstream receive with manual ack.
use kubemq::prelude::*;
use kubemq::{PollRequest, QueueMessageBuilder};
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.downstream.example";

    // Send some messages first
    for i in 0..5 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("downstream-msg-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }

    // Create a downstream receiver
    let mut receiver = client.new_queue_downstream_receiver().await?;

    // Poll for messages
    let poll = PollRequest {
        channel: channel.to_string(),
        max_items: 5,
        wait_timeout_seconds: 5,
        auto_ack: false,
    };

    let response = receiver.poll(poll).await?;
    println!(
        "Polled: {} messages, transaction={}",
        response.messages.len(),
        response.transaction_id
    );

    // Acknowledge all received messages
    response.ack_all().await?;
    println!("All messages acknowledged");

    receiver.close().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    client.close().await?;
    Ok(())
}
