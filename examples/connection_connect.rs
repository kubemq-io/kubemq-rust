//! # Connection — Connect
//!
//! Demonstrates creating a KubeMQ client with various builder options and
//! verifying the connection with a ping. Shows minimal, keep-alive, and
//! production-style configurations.
//!
//! ## Expected Output
//!
//! ```text
//! Minimal client connected. Server: <version>
//! Configured client connected. Server: <version>
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example connection_connect
//! ```
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    // Minimal connection
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Minimal client connected. Server: {}", info.version);
    client.close().await?;

    // Connection with client ID and check_connection
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .client_id("rust-example-client")
        .check_connection(true)
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Configured client connected. Server: {}", info.version);
    client.close().await?;

    Ok(())
}
