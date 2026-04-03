# Benchmarks

## Methodology

Benchmarks use [Criterion.rs](https://bheisler.github.io/criterion.rs/) with the following configuration:

- **Warm-up:** 3 seconds
- **Measurement:** 5 seconds
- **Sample size:** 100 iterations minimum
- **Broker:** Live KubeMQ broker on localhost:50000

All benchmarks connect to a real broker and measure end-to-end throughput including gRPC overhead.

## Benchmark Suite

| Benchmark | File | Description |
|-----------|------|-------------|
| `events_send_throughput` | `benches/events_bench.rs` | Unary event sends (ops/sec) |
| `events_stream_throughput` | `benches/events_bench.rs` | Streaming event sends (ops/sec) |
| `events_store_send_throughput` | `benches/events_store_bench.rs` | Event store unary sends (ops/sec) |
| `events_store_stream_throughput` | `benches/events_store_bench.rs` | Event store stream sends (ops/sec) |
| `queue_send_throughput` | `benches/queues_bench.rs` | Queue message sends (ops/sec) |
| `queue_poll_throughput` | `benches/queues_bench.rs` | Queue poll operations (ops/sec) |
| `command_roundtrip` | `benches/commands_bench.rs` | Command send + response latency |
| `query_roundtrip` | `benches/queries_bench.rs` | Query send + response latency |

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark
cargo bench -- events_send_throughput

# Generate HTML report
cargo bench -- --output-format=bencher
```

Results are saved in `target/criterion/` with HTML reports.

## Performance Targets

| Metric | Target |
|--------|--------|
| Events throughput | >= Go SDK msgs/sec |
| Queue throughput | >= Go SDK msgs/sec |
| Command latency (p99) | <= Go SDK p99 |
| Query latency (p99) | <= Go SDK p99 |
| Memory overhead per connection | < 10 MB |
| Binary size (release, no LTO) | < 20 MB |

## Notes

- Benchmarks require a running KubeMQ broker and are not run in CI
- Results vary based on hardware, network, and broker configuration
- Use the burn-in application for sustained load testing
