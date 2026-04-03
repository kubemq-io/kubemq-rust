//! Example: Connect to KubeMQ with an authentication token.
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
