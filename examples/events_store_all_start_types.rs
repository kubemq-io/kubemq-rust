//! # Events Store — All Subscription Start Types
//!
//! Demonstrates all six Events Store subscription start position types:
//! `StartNewOnly`, `StartFromFirst`, `StartFromLast`, `StartAtSequence`,
//! `StartAtTime`, and `StartAtTimeDelta`. Each type controls where in the
//! stored event stream the subscriber begins receiving messages.
//!
//! ## Expected Output
//!
//! ```text
//! --- StartNewOnly ---
//! --- StartFromFirst ---
//!   seq=1, id=<uuid>, body=...
//!   seq=2, id=<uuid>, body=...
//! --- StartFromLast ---
//!   seq=<last>, id=<uuid>, body=...
//! --- StartAtSequence(5) ---
//!   seq=5, id=<uuid>, body=...
//!   seq=6, id=<uuid>, body=...
//! --- StartAtTime ---
//!   seq=<n>, id=<uuid>, body=...
//! --- StartAtTimeDelta(30s) ---
//!   seq=<n>, id=<uuid>, body=...
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker with pre-existing stored events on the
//! `events_store.start_types` channel. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example events_store_all_start_types
//! ```
use kubemq::prelude::*;
use kubemq::EventsStoreSubscription;
use std::time::{Duration, SystemTime};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "events_store.start_types";
    let handler = |event: EventStoreReceive| -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move {
            println!(
                "  seq={}, id={}, body={}",
                event.sequence,
                event.id,
                String::from_utf8_lossy(&event.body)
            );
        })
    };

    // 1. StartNewOnly -- only new messages after subscribing
    println!("--- StartNewOnly ---");
    let sub1 = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartNewOnly,
            handler,
            None,
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sub1.unsubscribe().await;

    // 2. StartFromFirst -- replay from the very first message
    println!("--- StartFromFirst ---");
    let sub2 = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartFromFirst,
            handler,
            None,
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sub2.unsubscribe().await;

    // 3. StartFromLast -- only the last stored message
    println!("--- StartFromLast ---");
    let sub3 = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartFromLast,
            handler,
            None,
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sub3.unsubscribe().await;

    // 4. StartAtSequence -- from a specific sequence number
    println!("--- StartAtSequence(5) ---");
    let sub4 = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartAtSequence(5),
            handler,
            None,
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sub4.unsubscribe().await;

    // 5. StartAtTime -- from a specific point in time
    println!("--- StartAtTime ---");
    let start_time = SystemTime::now() - Duration::from_secs(3600); // 1 hour ago
    let sub5 = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartAtTime(start_time),
            handler,
            None,
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sub5.unsubscribe().await;

    // 6. StartAtTimeDelta -- relative time offset
    println!("--- StartAtTimeDelta(30s) ---");
    let sub6 = client
        .subscribe_to_events_store(
            channel,
            "",
            EventsStoreSubscription::StartAtTimeDelta(Duration::from_secs(30)),
            handler,
            None,
        )
        .await?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    sub6.unsubscribe().await;

    client.close().await?;
    Ok(())
}
