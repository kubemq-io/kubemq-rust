# Contributing to KubeMQ Rust SDK

Thank you for your interest in contributing to the KubeMQ Rust SDK.

## Development Setup

### Prerequisites

- Rust 1.75+ (install via [rustup](https://rustup.rs/))
- A running KubeMQ broker (for integration tests and examples)
- Protocol Buffers compiler (`protoc`) -- only needed for proto regeneration

### Clone and Build

```bash
git clone https://github.com/kubemq-io/kubemq-rust.git
cd kubemq-rust
cargo build
```

### Running Tests

```bash
# Unit tests (no broker needed)
cargo test

# All tests including integration (requires broker on localhost:50000)
cargo test --all-features

# Doc tests
cargo test --doc
```

### Formatting and Linting

```bash
cargo fmt --check
cargo clippy --all-features --all-targets -- -D warnings
```

### Running Benchmarks

Benchmarks require a live KubeMQ broker:

```bash
cargo bench
```

### Running Examples

```bash
cargo run --example connection_ping
cargo run --example events_pubsub
```

### Running the Burn-in App

```bash
cd burnin
cargo run -- --config burnin-config.yaml
```

## Code Style

- Follow standard Rust conventions and `rustfmt` formatting
- All public types and methods must have `///` doc comments
- Use `thiserror` for error types
- Use builders for complex struct construction
- All async operations return `kubemq::Result<T>`

## Pull Request Process

1. Fork the repository and create a feature branch
2. Ensure all tests pass: `cargo test --all-features`
3. Ensure code is formatted: `cargo fmt`
4. Ensure no clippy warnings: `cargo clippy --all-features -- -D warnings`
5. Update documentation if adding new public API
6. Submit a pull request with a clear description of changes

## Reporting Issues

Please use GitHub Issues to report bugs or request features. Include:

- Rust version (`rustc --version`)
- KubeMQ server version
- Steps to reproduce
- Expected vs actual behavior
