//! Example: Queue stream upstream batch send.
use kubemq::prelude::*;
use kubemq::QueueMessageBuilder;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "queues.stream.upstream.example";

    let mut upstream = client.queue_upstream().await?;

    // Send a batch of messages
    let messages: Vec<QueueMessage> = (0..10)
        .map(|i| {
            QueueMessageBuilder::new()
                .channel(channel)
                .body(format!("upstream-msg-{}", i).into_bytes())
                .build()
        })
        .collect();

    upstream.send("batch-001", messages).await?;
    println!("Sent batch via upstream stream");

    // Check results
    if let Some(result) = upstream.results().recv().await {
        println!(
            "Batch result: ref_id={}, is_error={}, items={}",
            result.ref_request_id,
            result.is_error,
            result.results.len()
        );
    }

    upstream.close();
    client.close().await?;
    Ok(())
}
