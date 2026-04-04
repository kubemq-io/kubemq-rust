//! # Connection Ping
//!
//! Demonstrates connecting to a KubeMQ broker, sending a ping request to
//! verify connectivity, and printing server information (host, version,
//! start time, and uptime).
//!
//! ## Expected Output
//!
//! ```text
//! Server host: localhost
//! Server version: <version>
//! Server start time: <timestamp>
//! Server uptime (seconds): <seconds>
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example connection_ping
//! ```
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Server host: {}", info.host);
    println!("Server version: {}", info.version);
    println!("Server start time: {}", info.server_start_time);
    println!("Server uptime (seconds): {}", info.server_up_time_seconds);

    client.close().await?;
    Ok(())
}
