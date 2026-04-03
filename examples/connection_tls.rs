//! Example: Connect to KubeMQ with server-side TLS.
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
