//! # Connection — Close
//!
//! Demonstrates graceful client closure. After connecting and verifying with
//! ping, the client is explicitly closed. Subsequent operations on a closed
//! client return errors.
//!
//! ## Expected Output
//!
//! ```text
//! Connected. Server: <version>
//! Client closed
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example connection_close
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
    println!("Connected. Server: {}", info.version);

    client.close().await?;
    println!("Client closed");

    Ok(())
}
