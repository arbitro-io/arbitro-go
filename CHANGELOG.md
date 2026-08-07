# Changelog

All notable changes to `arbitro-go` are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/). Module versions are published
as git tags (`vX.Y.Z`); this file is the source of truth for what each tag
contains.

## [Unreleased]

### Breaking
- **Wire error codes corrected.** The `ErrCode*` constants were a generation
  behind the protocol — `ErrCodeStreamNotFound` was `0x0010` where the broker
  sends `0x0201`, `ErrCodeIdempotencyDuplicate` was `0x0015` against `0x0206`,
  and so on across the table. Nothing crashed: `IsNotFound`, `IsAlreadyExists`
  and `IsDuplicate` compared against numbers the broker stopped emitting and
  answered `false` forever, so a missing stream read as an unrecognised failure
  and a duplicate publish was never detected as one. All values now match
  `arbitro-proto`'s `ErrorCode`, and a test pins the numbers so future drift
  fails loudly instead of silently.

  If you compare `ErrCode*` constants, no change is needed — they are correct
  now. If you hardcoded the numeric values, they must be updated.
- **Six error constants removed**: `ErrCodeStreamFilterOverlap`,
  `ErrCodeSubjectNotFound`, `ErrCodeConsumerFilterOverlap`,
  `ErrCodeInvalidSequence`, `ErrCodeMaxInflightReached`, `ErrCodeAckTimeout`.
  The broker deleted these codes and never sends them; keeping them would leave
  comparisons that can only ever be false.
- **Client-side codes moved out of the wire range.** `ErrCodeTimeout` and
  `ErrCodeInvalidConfig` are now `0xFF01`/`0xFF02` (were `0x00FF`/`0x00FE`) so
  they cannot collide with a future broker code.

### Fixed
- **`PauseConsumer` and `ResumeConsumer` never worked.** Both sent
  `{stream_id, name}`; the wire declares `{consumer_id}`. The broker answers a
  body it cannot deserialize with `InternalError`, so every pause and resume
  failed. They now resolve the consumer ID first.
- **Stopping a cron never worked.** `DeleteCron` sent a JSON body where the
  wire wants the raw name bytes, so the broker looked up the JSON verbatim as a
  cron name, missed, and returned `InternalError`.
- **`StreamExists` returned an error for a stream that does not exist**
  instead of `false`. "No such stream" is the answer to the question, not a
  failure to answer it — matching `stream_exists` in the Rust client.
- **`DeleteMessage` swallowed broker errors** into `(false, nil)`. "Nothing to
  delete" and "the delete never ran" are different answers, and reporting the
  second as the first leaves the message deliverable while the caller believes
  it is gone. Only `StreamNotFound` maps to `false` now.
- **Workflow: a step whose retry could not be queued was acked anyway.**
  `handleStepError` re-published the task with `attempt+1` and then discarded
  the publish error. The republish *is* the retry, so a failed publish silently
  dropped the step — the workflow stopped at a failed step and reported
  nothing. A failed republish now nacks, falling back to redelivery.
- The error predicates now use `errors.As`, so a `%w`-wrapped broker error is
  still classified correctly.

### Added
- **JSON helpers on `StepContext`** — `JSON`, `JSONOrDefault`, `JSONMerge`,
  `JSONReplace`. The workflow context is opaque bytes, so every step carrying
  JSON was Unmarshal/Marshal boilerplate around one line of work. `JSONMerge`
  and `JSONReplace` return `([]byte, error)` to hand straight back from a step:
  `return step.JSONMerge(...)`. Merge is shallow by design — a deep merge has
  to guess whether arrays replace or concatenate, and guessing wrong is silent.
  Uses `encoding/json`; no new dependency. At parity with the Rust client's
  `StepContext` helpers.
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
- **`ListConsumers` documents that it returns IDs only.** The reply is a fixed
  13-byte binary entry (`consumer_id`, `stream_id`, `queue_id`, `paused`) with
  no name field, so `ConsumerInfo.Name` and `.Filter` come back empty. Match on
  `ConsumerID`, or use `ConsumerInfo(stream, name)` when the name matters.
  Behaviour is unchanged — this was previously undocumented, and a name match
  here silently never succeeded.
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
