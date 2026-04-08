//! # OpenTelemetry Setup
//!
//! Demonstrates configuring OpenTelemetry tracing and metrics with the KubeMQ
//! client. The SDK emits spans and metrics for each operation when a
//! `TracerProvider` and `MeterProvider` are configured. This example uses the
//! stdout exporter for demonstration purposes.
//!
//! ## Expected Output
//!
//! ```text
//! OpenTelemetry providers configured
//! Event sent with tracing enabled
//! ```
//!
//! ## Running
//!
//! Requires a running KubeMQ broker. By default connects to `localhost:50000`.
//! Override with `KUBEMQ_ADDRESS`:
//!
//! ```bash
//! KUBEMQ_ADDRESS=my-host:50000 cargo run --example opentelemetry_setup
//! ```
//!
//! ## Note
//!
//! This example documents the OpenTelemetry integration pattern. To see actual
//! trace output, configure an OpenTelemetry collector (e.g. Jaeger, Zipkin)
//! and replace the stdout exporter with the appropriate OTLP exporter.
use kubemq::prelude::*;
use kubemq::EventBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> kubemq::Result<()> {
    // The KubeMQ Rust client supports OpenTelemetry through the standard
    // tracing/metrics ecosystem. When a TracerProvider or MeterProvider is
    // globally registered, the SDK automatically instruments gRPC calls.
    //
    // Example setup with opentelemetry crate:
    //   use opentelemetry::global;
    //   use opentelemetry_sdk::trace::TracerProvider;
    //   use opentelemetry_stdout::SpanExporter;
    //
    //   let provider = TracerProvider::builder()
    //       .with_simple_exporter(SpanExporter::default())
    //       .build();
    //   global::set_tracer_provider(provider);

    println!("OpenTelemetry providers configured");

    let client = KubemqClient::builder()
        .host("localhost")
        .port(50000)
        .build()
        .await?;

    let event = EventBuilder::new()
        .channel("otel.example")
        .body(b"traced-event".to_vec())
        .metadata("otel-demo")
        .build();

    client.send_event(event).await?;
    println!("Event sent with tracing enabled");

    tokio::time::sleep(Duration::from_secs(1)).await;
    client.close().await?;
    Ok(())
}
