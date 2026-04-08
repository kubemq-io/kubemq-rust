//! # Commands — Timeout
//!
//! Demonstrates command timeout behavior. A command is sent to a channel with
//! no handler, using a short timeout. The broker returns a timeout error since
//! no responder processes the command.
//!
//! ## Expected Output
//!
//! ```text
//! Sending command with 2s timeout and no handler...
//! Command timed out (expected): <error message>
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example commands_timeout
//! ```
use kubemq::prelude::*;
use kubemq::CommandBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let channel = "commands.timeout.example";

    println!("Sending command with 2s timeout and no handler...");

    let command = CommandBuilder::new()
        .channel(channel)
        .body(b"will-timeout".to_vec())
        .timeout(Duration::from_secs(2))
        .build();

    match client.send_command(command).await {
        Ok(resp) => println!(
            "Response (unexpected): executed={}, error='{}'",
            resp.executed, resp.error
        ),
        Err(e) => println!("Command timed out (expected): {}", e),
    }

    client.close().await?;
    Ok(())
}
