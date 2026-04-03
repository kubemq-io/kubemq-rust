//! Example: Send and receive queue messages (simple API).
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
