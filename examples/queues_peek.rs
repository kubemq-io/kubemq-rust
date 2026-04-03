//! Example: Peek at queue messages without consuming them.
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
