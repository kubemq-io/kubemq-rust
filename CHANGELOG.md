# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-04-02

### Added

- Initial release of the KubeMQ Rust SDK
- Full 1:1 feature parity with Go v2 and C++ SDKs (176/176 compliance items)
- Events (Pub/Sub): send, stream send, subscribe with wildcards and consumer groups
- Events Store (Persistent Pub/Sub): send, stream send, subscribe with 6 start positions
- Commands (RPC): send, subscribe, respond with timeout
- Queries (RPC with Cache): send, subscribe, respond with cache TTL
- Queues Stream API: upstream batch send, downstream poll with ack/nack/requeue
- Queues Simple API: send, batch send, receive, peek, ack-all
- Channel Management: create, delete, list for all 5 channel types
- Auto-reconnection with configurable exponential backoff
- TLS and mTLS support via rustls
- Authentication via auth token and CredentialProvider trait
- OpenTelemetry instrumentation (feature-gated: `otel`)
- Environment variable configuration (KUBEMQ_* variables)
- 34 standalone examples
- 5 Criterion benchmarks
- Burn-in soak test application with axum REST API
- CI pipeline for Linux, macOS, Windows with fmt, clippy, test, coverage, audit
