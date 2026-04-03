//! Example: Batch send and receive queue messages.
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
