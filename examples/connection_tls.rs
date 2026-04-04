//! # Connection with Server-Side TLS
//!
//! Demonstrates connecting to a KubeMQ broker over a TLS-encrypted channel.
//! Only a CA certificate is required (no client certificate), verifying the
//! server's identity without mutual authentication.
//!
//! ## Expected Output
//!
//! ```text
//! Connected with TLS. Server version: <version>
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker with TLS enabled and a valid CA certificate.
//! Update the file path in the example to point to your CA certificate.
//! By default connects to `localhost:50000`. Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example connection_tls
//! ```
use kubemq::prelude::*;
use kubemq::TlsConfig;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let tls = TlsConfig {
        ca_cert_file: Some("/path/to/ca.pem".to_string()),
        ..Default::default()
    };

    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .tls_config(tls)
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Connected with TLS. Server version: {}", info.version);

    client.close().await?;
    Ok(())
}
