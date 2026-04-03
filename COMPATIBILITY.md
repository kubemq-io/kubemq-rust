# Compatibility

## Rust Versions

| Version | Status |
|---------|--------|
| 1.75 (MSRV) | Supported |
| 1.76 - latest stable | Supported |
| Nightly | Best-effort |

The Minimum Supported Rust Version (MSRV) is **1.75**. MSRV bumps are treated as minor version changes.

## Platforms

| Platform | Architecture | Status |
|----------|-------------|--------|
| Linux (glibc) | x86_64, aarch64 | Fully supported |
| Linux (musl) | x86_64, aarch64 | Fully supported |
| macOS | x86_64, aarch64 (Apple Silicon) | Fully supported |
| Windows (MSVC) | x86_64 | Fully supported |

## KubeMQ Server Versions

This SDK supports all KubeMQ server versions that implement the proto v1.4.0 gRPC API.

## Dependencies

| Dependency | Version | Purpose |
|-----------|---------|---------|
| tonic | 0.12 | gRPC client |
| prost | 0.13 | Protocol Buffers |
| tokio | 1.x | Async runtime |
| rustls | 0.23 | TLS (1.2+) |
| thiserror | 2.x | Error handling |

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `otel` | No | OpenTelemetry tracing and metrics |
| `full` | No | All optional features |
