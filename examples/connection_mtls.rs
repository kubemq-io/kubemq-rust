//! # Connection with Mutual TLS (mTLS)
//!
//! Demonstrates connecting to a KubeMQ broker using mutual TLS authentication.
//! Both server and client certificates are configured via [`TlsConfig`], requiring
//! a CA certificate, client certificate, and client private key.
//!
//! ## Expected Output
//!
//! ```text
//! Connected with mTLS. Server version: <version>
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker with mTLS enabled and valid certificate files.
//! Update the file paths in the example to point to your certificates.
//! By default connects to `localhost:50000`. Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example connection_mtls
//! ```
use kubemq::prelude::*;
use kubemq::TlsConfig;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let tls = TlsConfig {
        ca_cert_file: Some("/path/to/ca.pem".to_string()),
        cert_file: Some("/path/to/client.pem".to_string()),
        key_file: Some("/path/to/client-key.pem".to_string()),
        ..Default::default()
    };

    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .tls_config(tls)
        .build()
        .await?;

    let info = client.ping().await?;
    println!("Connected with mTLS. Server version: {}", info.version);

    client.close().await?;
    Ok(())
}
