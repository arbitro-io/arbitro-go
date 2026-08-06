# Changelog

All notable changes to `arbitro-go` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/). Module versions are published
as git tags (`vX.Y.Z`); this file is the source of truth for what each tag
contains.

## [Unreleased]

### Added
- **`WithAckStoreDir(dir)`** — the ack-store WAL's storage location is now part
  of the normal option surface. `""` selects the platform default:
  `$ARBITRO_ACKSTORE_DIR`, else `$XDG_STATE_HOME/arbitro/ackstore` (Linux/BSD),
  `~/Library/Application Support/arbitro/ackstore` (macOS), or
  `%LOCALAPPDATA%\arbitro\ackstore` (Windows). Never the cwd, never a temp dir
  — both silently defeat restart survival, so an unresolvable default is a hard
  error instead.
- **`DefaultAckStoreDir()`** — report the resolved path without opening
  anything; log it at startup.
- **Single-writer directory lock** — `OpenWAL` takes an OS advisory lock
  (`flock` on unix, exclusive share-mode open on Windows) on
  `<dir>/ackstore.lock`. A second client on the same directory now fails with
  `ErrAckStoreLocked` instead of interleaving frames into one log, which after
  a restart misattributed records between slots and could skip real work. The
  kernel releases the lock on process exit, so a crash never wedges the store.
- **`WAL.Dir()`** — the directory the store actually resolved to.
- **`ErrAckStoreLocked` / `ErrNoDefaultAckStoreDir`** — matchable with
  `errors.Is` on the error returned by `Connect`.

### Changed
- `ackstore.Config.Dir` may now be empty (resolves the default) instead of
  erroring with "Config.Dir required".
- An unusable store directory (a regular file, a path under a file, no write
  permission) now reports the path and the specific problem instead of an
  opaque `*PathError`.
- `Connect` validates the ack-store configuration **before** dialling, and
  closes an already-opened store when the dial fails — previously a transient
  network error leaked the WAL's file handle (and now its directory lock) for
  the life of the process, so a retry loop would have hit `ErrAckStoreLocked`.

### Unchanged
- On-disk WAL format and store semantics. This is configuration only.
- Default dedup is still the in-memory store; no files appear unless a durable
  store is explicitly requested.

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
