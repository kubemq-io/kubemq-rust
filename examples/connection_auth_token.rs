//! # Connection with Auth Token
//!
//! Demonstrates connecting to a KubeMQ broker using an authentication token.
//! The token is passed via the builder and included in every gRPC request.
//!
//! ## Expected Output
//!
//! ```text
//! Connected with auth token. Server version: <version>
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker with authentication enabled.
//! By default connects to `localhost:50000`. Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example connection_auth_token
//! ```
use kubemq::prelude::*;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .auth_token("your-auth-token-here")
        .build()
        .await?;

    let info = client.ping().await?;
    println!(
        "Connected with auth token. Server version: {}",
        info.version
    );

    client.close().await?;
    Ok(())
}
