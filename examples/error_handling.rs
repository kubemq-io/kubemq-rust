//! # Error Handling
//!
//! Demonstrates the SDK's error types and how to match on them. Shows
//! connection errors (connecting to a non-existent port), validation errors
//! (sending to an empty channel), and error code matching with [`ErrorCode`]
//! variants (`Validation`, `Transient`, `Timeout`, `Authentication`).
//!
//! ## Expected Output
//!
//! ```text
//! Connection failed (expected): <error message>
//!   Error code: Transient
//!   Is retryable: true
//!   Suggestion: <suggestion text>
//! Validation error (expected):
//!   Code: Validation
//!   Message: <validation message>
//!   Suggestion: <suggestion text>
//! Got validation error
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker on port 50000 for validation error demos.
//! The connection error demo intentionally uses a wrong port. Override with
//! `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example error_handling
//! ```
use kubemq::prelude::*;
use kubemq::{ErrorCode, EventBuilder, KubemqError};

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    // Attempt to connect to a non-existent server to demonstrate errors
    let result = KubemqClient::builder()
        .host("localhost")
        .port(59999) // unlikely port
        .check_connection(true)
        .build()
        .await;

    match result {
        Ok(client) => {
            println!("Connected (unexpected)");
            client.close().await?;
        }
        Err(ref e) => {
            println!("Connection failed (expected): {}", e);
            println!("  Error code: {:?}", e.code());
            println!("  Is retryable: {}", e.is_retryable());
            println!("  Suggestion: {}", e.suggestion());
        }
    }

    // Demonstrate validation errors
    let client_result = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await;

    if let Ok(client) = client_result {
        // Send to empty channel -- validation error
        let event = EventBuilder::new()
            .channel("") // invalid
            .body(b"test".to_vec())
            .build();

        match client.send_event(event).await {
            Ok(()) => println!("Sent (unexpected for empty channel)"),
            Err(KubemqError::Validation {
                code,
                message,
                suggestion,
                ..
            }) => {
                println!("Validation error (expected):");
                println!("  Code: {:?}", code);
                println!("  Message: {}", message);
                println!("  Suggestion: {}", suggestion);
            }
            Err(e) => println!("Other error: {}", e),
        }

        // Demonstrate error matching by code
        let event = EventBuilder::new()
            .channel("")
            .body(b"test".to_vec())
            .build();

        if let Err(e) = client.send_event(event).await {
            match e.code() {
                ErrorCode::Validation => println!("Got validation error"),
                ErrorCode::Transient => println!("Got transient error"),
                ErrorCode::Timeout => println!("Got timeout error"),
                ErrorCode::Authentication => println!("Got auth error"),
                _ => println!("Got other error: {:?}", e.code()),
            }
        }

        client.close().await?;
    }

    Ok(())
}
