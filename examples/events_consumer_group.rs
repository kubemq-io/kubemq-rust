//! # Events Consumer Group
//!
//! Demonstrates load-balanced event consumption using a consumer group. Two
//! subscribers join the same group on the same channel. Each event is delivered
//! to only one consumer, distributing the workload across the group.
//!
//! ## Expected Output
//!
//! ```text
//! Consumer-1 received: id=<uuid>, body=message-0
//! Consumer-2 received: id=<uuid>, body=message-1
//! Consumer-1 received: id=<uuid>, body=message-2
//! ...
//! Sent 10 events to group 'my-consumer-group'
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_consumer_group
//! ```
use kubemq::prelude::*;
use kubemq::EventBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events.group.example";
    let group = "my-consumer-group";

    // Subscribe two consumers in the same group -- each event goes to only one
    let sub1 = client
        .subscribe_to_events(
            channel,
            group,
            |event| {
                Box::pin(async move {
                    println!(
                        "Consumer-1 received: id={}, body={}",
                        event.id,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    let sub2 = client
        .subscribe_to_events(
            channel,
            group,
            |event| {
                Box::pin(async move {
                    println!(
                        "Consumer-2 received: id={}, body={}",
                        event.id,
                        String::from_utf8_lossy(&event.body)
                    );
                })
            },
            None,
        )
        .await?;

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Send several events -- they will be distributed across consumers
    for i in 0..10 {
        let event = EventBuilder::new()
            .channel(channel)
            .body(format!("message-{}", i).into_bytes())
            .build();
        client.send_event(event).await?;
    }

    println!("Sent 10 events to group '{}'", group);
    tokio::time::sleep(Duration::from_secs(2)).await;

    sub1.unsubscribe().await;
    sub2.unsubscribe().await;
    client.close().await?;
    Ok(())
}
