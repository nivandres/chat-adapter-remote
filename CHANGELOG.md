# Changelog

## 0.2.0

Correctness release following a review against real adapter objects. Anyone on 0.1.0 should upgrade; the bugs below cause silent message loss, corrupted data, and host crashes.

### Fixed

- Messages carrying `replyTo` were dropped. The inbound schema was a strict mirror of `SerializedMessage`, so any field it did not list failed validation, and the failure was acknowledged with HTTP 200. Payload schemas are now permissive and a validation failure returns 400.
- `fetchMessages` corrupted every message. Results were passed through the buffer codec instead of the message serializer, and the codec had no `Date` branch, so timestamps became `{}`. Results are now serialized like inbound messages, and the codec handles `Date`.
- `RemoteChat.processMessage` could crash the host. Adapters call it unawaited from their event loops, so a rejection became an unhandled rejection. It now resolves and logs on failure.
- An adapter failing to `initialize()` crashed the host before it served a request. The rejection is now observed at construction; `ready` still surfaces it.
- Unbridged `ChatInstance` members threw into adapter event loops, so a single WhatsApp reaction or poll vote killed the host. They now log and no-op.
- The codec rejected any object referenced twice as a sibling, treating shared references as cycles. Cycle detection now tracks the ancestor path.
- A dispatch returning `undefined` produced a response with no `result` member, which the client then rejected.

### Added

- `AdapterHost.start()` and `AdapterHost.stop()`, with `autoStart` to opt out of initializing during construction. `start()` is idempotent and rejects loudly; `ready` is now shorthand for it. `stop()` disconnects the adapter and refuses further dispatch, so it can be wired to `SIGTERM`.
- `AdapterHost.handlePlatformWebhook()`, so adapters driven by platform webhooks rather than a socket have a host-side route that waits for startup first.
- `onError` and `onReady` callbacks on `serveAdapter`. `onError` reports the `initialize`, `forward`, `dispatch`, and `shutdown` phases.
- `maxConcurrentForwards` (default 8) caps inbound messages in flight to the consumer, so a platform backlog cannot open one request per message.
- `logForwardLevel` on `serveAdapter`, forwarding the log threshold through to the bridged logger.
- `protocolVersion` in the handshake; both sides refuse to initialize on a mismatch.
- `lockScope`, `persistThreadHistory`, and `supportsTurnCancellation` carried over the handshake.
- `isDM` bridged per inbound message, so DM routing reaches `onNewMention` without a literal mention.
- `timestampToleranceMs` and `maxBodyBytes` are configurable on the consumer; they were previously declared but unused.
- A log-forwarding level threshold, defaulting to `info`.

### Changed

- **Breaking:** `@chat-adapter/shared` moved from a dependency to a peer dependency. Two copies break the `instanceof` checks that error reconstruction relies on.
- **Breaking:** `RemoteChat` and `RemoteChatUnsupportedMethodError` are no longer exported; use `createRemoteChat`.
- Inbound requests are acknowledged as soon as the message is accepted, with handlers running under the caller's `waitUntil`, instead of the host blocking on the full handler chain.
- Request bodies are counted while streaming rather than buffered before the size check.
- `notify` no longer awaits a response.
- Response ids are matched against request ids, and the id counter is per client.
- `RpcErrorCode.REPLAY_REJECTED` renamed to `STALE_TIMESTAMP`, matching what the check actually does.
- `engines.node >= 20`, `sideEffects: false`, and a `prepublishOnly` guard added.
- Signing imports `node:crypto` rather than the bare `crypto` specifier, so bundlers stop attempting a browser polyfill.

## 0.1.0

Initial release.

[0.2.0]: https://github.com/nivandres/chat-adapter-remote/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nivandres/chat-adapter-remote/releases/tag/v0.1.0
