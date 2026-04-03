//! Example: Send a queue message with a delay.
use kubemq::prelude::*;
use kubemq::QueueMessageBuilder;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.delayed.example";

    // Send a message delayed by 5 seconds
    let msg = QueueMessageBuilder::new()
        .channel(channel)
        .body(b"delayed-message".to_vec())
        .delay_seconds(5)
        .build();

    let result = client.send_queue_message(msg).await?;
    println!(
        "Sent delayed message: id={}, delayed_to={}",
        result.message_id, result.delayed_to
    );

    // Try to receive immediately -- should be empty
    let immediate = client.receive_queue_messages(channel, 10, 1, false).await?;
    println!(
        "Immediate receive (before delay): {} messages",
        immediate.len()
    );

    // Wait for the delay
    println!("Waiting 6 seconds for delayed message...");
    tokio::time::sleep(std::time::Duration::from_secs(6)).await;

    let delayed = client.receive_queue_messages(channel, 10, 5, false).await?;
    println!("After delay: {} messages received", delayed.len());

    client.close().await?;
    Ok(())
}
