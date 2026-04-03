# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x | Yes |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **Do not** open a public GitHub issue
2. Email security@kubemq.io with:
   - Description of the vulnerability
   - Steps to reproduce
   - Impact assessment
   - Any suggested fixes

We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Security Features

- **TLS/mTLS:** All connections can be secured with TLS 1.2+ via rustls
- **Authentication:** Auth token support via gRPC metadata
- **Credential Provider:** Pluggable token refresh with expiration handling
- **No unsafe code:** The SDK does not use `unsafe` blocks
- **Dependency auditing:** `cargo audit` runs in CI via rustsec/audit-check

## Dependencies

We monitor dependencies for known vulnerabilities using:

- `cargo audit` in CI
- Dependabot alerts on GitHub
- Minimal dependency footprint

## Best Practices

- Always use TLS in production
- Rotate auth tokens regularly
- Use the `CredentialProvider` trait for dynamic token management
- Keep the SDK updated to the latest version
