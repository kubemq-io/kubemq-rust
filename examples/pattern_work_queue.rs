//! # Pattern — Work Queue
//!
//! Demonstrates the competing consumers (work queue) pattern. Tasks are
//! enqueued, and two workers pull and process them independently. Each task
//! is consumed by exactly one worker.
//!
//! ## Expected Output
//!
//! ```text
//! Enqueued 6 tasks
//! Worker-1 processed: task-0
//! Worker-2 processed: task-1
//! Worker-1 processed: task-2
//! ...
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example pattern_work_queue
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

    let channel = "patterns.work_queue.example";

    // Enqueue tasks
    for i in 0..6 {
        let msg = QueueMessageBuilder::new()
            .channel(channel)
            .body(format!("task-{}", i).into_bytes())
            .build();
        client.send_queue_message(msg).await?;
    }
    println!("Enqueued 6 tasks");

    // Worker 1 pulls a batch
    let worker1_msgs = client.receive_queue_messages(channel, 3, 5, false).await?;
    for m in &worker1_msgs {
        println!(
            "Worker-1 processed: {}",
            String::from_utf8_lossy(&m.body)
        );
    }

    // Worker 2 pulls remaining
    let worker2_msgs = client.receive_queue_messages(channel, 3, 5, false).await?;
    for m in &worker2_msgs {
        println!(
            "Worker-2 processed: {}",
            String::from_utf8_lossy(&m.body)
        );
    }

    client.close().await?;
    Ok(())
}
