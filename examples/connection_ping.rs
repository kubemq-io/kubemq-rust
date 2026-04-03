//! Example: Connect to KubeMQ, ping the server, print server info, and close.
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
