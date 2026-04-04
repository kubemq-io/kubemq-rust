//! # Channel Management
//!
//! Demonstrates creating, listing, and deleting channels across all five
//! messaging pattern types: Events, Events Store, Commands, Queries, and
//! Queues. Also shows filtered channel listing by search term.
//!
//! ## Expected Output
//!
//! ```text
//! Created events channel
//! Created events store channel
//! Created commands channel
//! Created queries channel
//! Created queues channel
//! Events channels: <count>
//!   name=example-events-ch, active=<bool>, last_activity=<timestamp>
//!   ...
//! Filtered queues channels: <count>
//! All example channels deleted
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example channel_management
//! ```
use kubemq::channel_type;
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    // Create channels of each type
    client.create_events_channel("example-events-ch").await?;
    println!("Created events channel");

    client
        .create_events_store_channel("example-store-ch")
        .await?;
    println!("Created events store channel");

    client
        .create_commands_channel("example-commands-ch")
        .await?;
    println!("Created commands channel");

    client.create_queries_channel("example-queries-ch").await?;
    println!("Created queries channel");

    client.create_queues_channel("example-queues-ch").await?;
    println!("Created queues channel");

    // List channels by type
    let events_channels = client.list_events_channels("").await?;
    println!("Events channels: {}", events_channels.len());
    for ch in &events_channels {
        println!(
            "  name={}, active={}, last_activity={}",
            ch.name, ch.is_active, ch.last_activity
        );
    }

    // List with search filter
    let filtered = client
        .list_channels(channel_type::QUEUES, "example")
        .await?;
    println!("Filtered queues channels: {}", filtered.len());

    // Delete channels
    client.delete_events_channel("example-events-ch").await?;
    client
        .delete_events_store_channel("example-store-ch")
        .await?;
    client
        .delete_commands_channel("example-commands-ch")
        .await?;
    client.delete_queries_channel("example-queries-ch").await?;
    client.delete_queues_channel("example-queues-ch").await?;
    println!("All example channels deleted");

    client.close().await?;
    Ok(())
}
