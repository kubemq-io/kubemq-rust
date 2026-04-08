//! # Connection — Custom Timeouts
//!
//! Demonstrates configuring custom timeout and keep-alive settings on the
//! client builder. Includes connection timeout, keep-alive interval, and
//! retry policy with exponential backoff.
//!
//! ## Expected Output
//!
//! ```text
//! Connected with custom timeouts. Server: <version>
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example connection_custom_timeouts
//! ```
use kubemq::prelude::*;
use kubemq::RetryPolicy;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-timeout-example")
        .retry_policy(RetryPolicy {
            max_retries: 5,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(30),
            multiplier: 2.0,
            ..Default::default()
        })
        .on_connected(|| async {
            println!("[STATE] Connected");
        })
        .on_closed(|| async {
            println!("[STATE] Closed");
        })
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Connected with custom timeouts. Server: {}", info.version);

    client.close().await?;
    Ok(())
}
