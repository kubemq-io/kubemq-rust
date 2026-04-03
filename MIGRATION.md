# Migration Guide

## From 0.x to 1.0

This is the initial release of the KubeMQ Rust SDK. No migration is needed.

## Migrating from Other SDKs

### From Go SDK

| Go SDK | Rust SDK |
|--------|----------|
| `kubemq.NewClient(ctx, opts...)` | `KubemqClient::builder().host("...").port(...).build().await?` |
| `client.SendEvent(ctx, event)` | `client.send_event(&event).await?` |
| `client.SubscribeToEvents(ctx, ch, grp, errCh)` | `client.subscribe_to_events(ch, grp, callback, on_error).await?` |
| `client.Close()` | `client.close().await?` |
| Context-based cancellation | `Subscription::unsubscribe().await` or drop |
| `KubeMQError` struct | `KubemqError` enum with `thiserror` |
| Functional options | Builder pattern |

### From C++ SDK

| C++ SDK | Rust SDK |
|---------|----------|
| `Client::Create(config)` | `KubemqClient::builder()...build().await?` |
| Smart pointer ownership | `Arc<Inner>` via `Clone` |
| RAII cleanup | `Drop` trait + `close().await` |
| Header/source separation | Module system |

## Future Versions

This section will be updated with migration guides for future major versions.
