# Changelog

All notable changes to `arbitro-go` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/). Module versions are published
as git tags (`vX.Y.Z`); this file is the source of truth for what each tag
contains.

## [0.6.2] - 2026-07-18

Reliability and parity release. Brings the Go client's ack-reliability, cron,
heartbeat/reconnect, and request/reply behaviour to parity with the Rust
reference client. Pairs with `arbitro-server >= 0.6.2` (uses the `AckState`
frames `0x0A01`–`0x0A04`).

### Added
- **Ack reliability hot tier** — pending state, wire frames `0x0A0x`, and
  `seen` dedup.
- **`Client.Request`** — correlated request/reply (Wave4b fix) plus
  `publish_with_headers`.
- **Heartbeat + reconnect supervisor** — dead-connection detection, supervisor
  loop, resubscribe, and cron replay on reconnect.
- **Cron end-to-end** — wire bodies, dispatch, and timeout enforcement.
- **`ConsumerExists` / `UpsertConsumer`** and a **`PublishWait`** name-alias for
  the Rust `publish_wait` rename.
- **Stats + metrics, TLS, consumer validation.**
- `List*` implementations, `DeleteConsumer` body, `msgId` propagation, batch
  chunking, and `Hello` padding.

### Fixed
- **`G05`** — flipped `AckPolicy` / `DeliverPolicy` constants so the wire values
  now match `arbitro-proto` in the broker.

### Docs
- Documented the ack-reliability cold-tier out-of-scope boundary.

[0.6.2]: https://github.com/arbitro-io/arbitro-go/compare/v0.6.1...v0.6.2
