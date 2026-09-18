# Changelog

## 0.5.0

Two defects found running 0.4.0 against a real WhatsApp account, both of which ended the host process.

### Fixed

- `chat.getState()` returned `undefined` on the host, because unbridged `ChatInstance` members resolve to a warning no-op. That is fine for the `processX` events, which return nothing, but an adapter persisting its own data does `chat.getState().set(...)` in one expression and got a `TypeError` far from the cause. Baileys keeps each poll's secret there, so polls could not be sent — and an incoming vote later killed the worker outright. `getState()` now answers with a real store.
- A rejection thrown inside the adapter's own event loop ended the process. Those belong to no request, so nothing here could wrap them, and `onError` never saw them. The host now keeps them from ending the process and reports them under a new `adapter` phase. Set `catchUnhandledRejections: false` to opt out.

### Added

- `state` on the host. Without one, `getState()` reaches the consumer's store over the same protocol, so both halves share it. With one, the host uses it directly and no operation crosses the wire; the host connects and disconnects it, since it was handed over. The wire names an operation from the `StateAdapter` surface, it never picks one.

## 0.4.0

### Fixed

- Responses had no size limit: `maxBodyBytes` only covered requests, so a deferred attachment could be pulled into the consumer unbounded — exactly the case deferring exists to avoid. Responses are now read under the same limit.
- The attachment budget was spent per message rather than per body, so `fetchMessages`, `fetchChannelMessages` and `listThreads` could still inline past the limit. One budget now covers a whole response, and an event carrying two messages shares one.
- A deferred attachment could only be read once, because the host dropped it as it was read. It is now kept until it expires and the consumer memoises the fetch, so reading it twice works and a failed read can be retried.
- `customMethods: true` exposed lifecycle methods like `connect` and callback-shaped `onSomething` methods, neither of which can work over RPC. Discovery skips both; an explicit list still wins for anything else.

### Changed

- `maxBodyBytes` is unlimited by default. It guarded the window before a signature can be checked, but both ends of this bridge are yours, and on the consumer the runtime already caps request bodies well below the old 5 MB default. Set it if an endpoint is reachable from somewhere you do not control. `"auto"` attachments keep their own threshold, so deferring still works when no limit is set.

### Added

- `customMethods` on the host, exposing adapter methods outside the `Adapter` interface so platform-specific calls reach the consumer over the same signed protocol. A `string[]` names them; `true` exposes the adapter's own public methods, skipping `constructor`, `_`-prefixed names and the interface itself. Off by default: the wire picks from a list the host decided on, it never selects what to call.
- `createRemoteAdapter<TAdapter>()` takes the original adapter type and derives its thread and raw-message types along with those custom methods, so `createRemoteAdapter<BaileysAdapter>()` types `setPresence` and friends from the source. Crossing the wire makes them all async, so a synchronous method is typed as returning a promise of what it returned.

### Changed

- `createRemoteAdapter` takes one type parameter, the adapter type, instead of `<TThreadId, TRawMessage>`. Calls without explicit type arguments are unaffected.

## 0.3.0

Attachment bytes no longer have to travel inside the message. Both sides must be upgraded together: the protocol version is now 3.

### Added

- `inlineAttachments`, defaulting to `"auto"`. Small media still travels inside the message; once it would not fit the body budget the host keeps the bytes and sends an id, and the consumer's `fetchData()` pulls them when a handler actually reads the attachment. When the platform reported a size, an oversized attachment is never even downloaded on the host. `true` always inlines, `false` never does. When the adapter implements `rehydrateAttachment` and the attachment carries `fetchMetadata`, deferring uses that instead and nothing is held here — it survives a host restart and never expires. Nothing here is adapter-specific — the host holds whatever the adapter already knew how to do, so it works for adapters that implement neither `rehydrateAttachment` nor `fetchMetadata`.
- `attachmentTtlMs`, how long bytes the consumer never fetched are kept.
- `url` and `secret` fall back to `CHAT_ADAPTER_REMOTE_URL`, `CHAT_ADAPTER_REMOTE_CONSUMER_URL` and `CHAT_ADAPTER_REMOTE_SECRET`, and a missing one now throws `ValidationError` from `@chat-adapter/shared` rather than a plain `Error`.

### Changed

- `PROTOCOL_VERSION` is 3. A serialized attachment can now carry a reference instead of its bytes, so an older consumer would silently receive media it cannot read; the handshake refuses the pairing instead.
- The published package no longer ships `CHANGELOG.md`, matching the Chat SDK publishing checklist.

## 0.2.1

Hardening pass driven by running 0.2.0 against a real WhatsApp adapter, plus the rest of the inbound surface.

### Fixed

- Delivered attachments carried `data` but no `fetchData`, which is what the SDK actually reads. `toAiMessages` drops an image that only has `data`, so media forwarded through the bridge disappeared on its way to a model. Both are now present.
- Modal submits and options loads queued behind inbound message forwards, so a backlog could push them past the few seconds the platform allows before the user sees a connection error. They no longer go through the forward limiter: they are paced by a human clicking and cannot flood it.
- The consumer returned the answer to those two without running it through the codec, the one place a value crossed the wire uncoded.
- A stream abandoned mid-flight was only reclaimed by a later stream call, so a consumer that lost its connection could leave the adapter blocked on an iterable that never ended. A sweep now runs while streams are open, unref'd so it never holds the process alive.
- Missing `secret`, `url` or `consumerUrl` crashed on the first request with `The "key" argument must be of type string` or `Failed to parse URL from undefined`, neither of which names the missing option. Both sides now fail at construction saying which one it is.
- Every request rejected before it was parsed — bad signature, oversized body, malformed JSON — reached the caller as `response id did not match request`. Those answer with `id: null` per JSON-RPC, and the client matched the id before reading the error, so the real reason was never visible. The error is now read first.
- The host sanitises adapter errors on the wire but did not log them locally either, so a failing call left no trace on either side. The host now logs the original error before sanitising it.
- A throw inside request verification escaped `handleRequest` and `handleWebhook`, because verification ran outside their try/catch. An unhandled rejection there takes down the process holding the platform connection; both now answer with an error response instead.
- `streamStart` could hang forever on an adapter that neither answered nor started reading, holding the request open with no stream id, so nothing was left for the sweep to reclaim. It now gives up after `streamStartTimeoutMs` (default ten seconds).
- The forward limiter let a new arrival overtake messages already queued, and its queue was unbounded, so a history sync could park an arbitrary number of pending promises. Arrivals now queue behind the backlog, which is capped by `maxQueuedForwards` (default 1000).

### Added

- `onError` on the consumer too, since it is the half that runs where no debugger can be attached. Inbound handler failures reach it instead of only the logger.
- Bridged the remaining inbound events whose payload is plain data: `processModalSubmit`, `processModalClose`, `processOptionsLoad`, `processAppHomeOpened`, `processAppContextChanged`, `processAssistantThreadStarted`, `processAssistantContextChanged`, `processAgentSessionStopped`, `processAgentSessionTitleChanged`, and `processMemberJoinedChannel`. `processModalSubmit` and `processOptionsLoad` are awaited rather than acknowledged, so the consumer's answer reaches the platform. Only `getState`, `history` and `transcripts` are left, and those hand back live objects rather than data.
- `npm run support-map`, which prints what the bridge carries, derived from the protocol so it cannot drift from the code.

### Compatibility

The protocol version is unchanged, so 0.2.0 and 0.2.1 interoperate. A 0.2.1 host sending one of the new events to a 0.2.0 consumer is answered with a validation error and logged, rather than failing the connection.

## 0.2.0

A correctness pass against real adapter objects, plus an expansion of the bridged surface to almost all of the `Adapter` interface. Anyone on 0.1.0 should upgrade; the bugs below cause silent message loss, corrupted data, and host crashes. The handshake now carries a protocol version, so both sides must be upgraded together.

### Fixed

- Messages carrying `replyTo` were dropped. The inbound schema was a strict mirror of `SerializedMessage`, so any field it did not list failed validation, and the failure was acknowledged with HTTP 200. Payload schemas are now permissive and a validation failure returns 400.
- `fetchMessages` corrupted every message. Results were passed through the buffer codec instead of the message serializer, and the codec had no `Date` branch, so timestamps became `{}`. Results are now serialized like inbound messages, and the codec handles `Date`.
- `RemoteChat.processMessage` could crash the host. Adapters call it unawaited from their event loops, so a rejection became an unhandled rejection. It now resolves and logs on failure.
- An adapter failing to `initialize()` crashed the host before it served a request. The rejection is now observed at construction; `ready` still surfaces it.
- Unbridged `ChatInstance` members threw into adapter event loops, so a single WhatsApp reaction or poll vote killed the host. They now log and no-op.
- The codec rejected any object referenced twice as a sibling, treating shared references as cycles. Cycle detection now tracks the ancestor path.
- A dispatch returning `undefined` produced a response with no `result` member, which the client then rejected.
- The consumer's per-thread facts cache grew for the lifetime of the process. It is now bounded by `maxCachedThreads` (default 1000), oldest evicted first.
- `createAdapterHost` could be made to start after all, because `autoStart` from the caller's options overrode it.

### Added

- Capability handshake. The host reports which optional `Adapter` members its adapter actually implements and the consumer removes the rest from itself, so Chat's `adapter.method?.()` fallbacks keep working instead of failing against a method the real adapter never had.
- Bridged `reply`, `endTyping`, `markAsRead`, `listThreads`, `getUser`, `postObject`, `editObject`, `openDM`, `openModal`, `postEphemeral`, `postChannelMessage`, `fetchMessage`, `fetchChannelInfo`, `fetchChannelMessages`, `fetchSubject`, and `onThreadSubscribe`. Every return value carrying a `Message` goes through the message codec, so dates and attachments survive.
- Bridged `stream`, as an open/push/end call sequence that rebuilds the async iterable on the host. One push is in flight at a time, so the first chunk leaves immediately and the rest coalesce behind it. An adapter that declines to stream is reported on the open call, so the consumer returns `null` with the caller's iterable untouched and Chat SDK's post-and-edit fallback can still read it. Abandoned streams expire after `streamTtlMs` (default five minutes).
- Bridged `scheduleMessage` and `rehydrateAttachment`, both of which return live values. Each keeps its object on the host and is reached by id: the returned `cancel()` calls back to the host, and the rebuilt `fetchData()` fetches the bytes through it. Scheduled entries are dropped once their delivery time passes.
- Bridged the inbound events `processReaction`, `processMessageUpdated`, `processMessageDeleted`, `processAction`, `processSlashCommand`, and `abortTurn`. Messages inside an event payload go through the message codec and the reaction emoji is resolved back to the same `EmojiValue` singleton, so `===` comparisons in `onReaction` keep working.
- `ChatInstance.getUserName()` on the host answers with the wrapped adapter's `userName` instead of the no-op that returned nothing.
- `getChannelVisibility`, answered from facts the host attaches to each inbound message.
- `AdapterHost.start()` and `AdapterHost.stop()`, with `autoStart` to opt out of initializing during construction. `start()` is idempotent and rejects loudly; `ready` is now shorthand for it. `stop()` disconnects the adapter and refuses further dispatch, so it can be wired to `SIGTERM`.
- `createAdapterHost`, the same host left stopped. `serveAdapter` still starts during construction.
- `AdapterHost.handleWebhook()`, so adapters driven by platform webhooks rather than a socket have a host-side route that waits for startup first.
- `AdapterHost.fetch`, a bound handler that drops straight into any Fetch-API router.
- Replay protection. Each signature is accepted once inside its freshness window, on both the host and the consumer. The default store is per-process and bounded; `replayGuard.seen()` may be async, so a shared store can back it.
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
- Per-message facts travel as one object rather than positional arguments, so a new fact does not change arity.
- Errors raised by this package itself — an unimplemented method, an expired stream, a schedule that can no longer be cancelled — keep their code and message. Errors from the wrapped adapter still collapse to a generic message.
- An inbound event that arrives before `initialize()` is refused with 503 instead of acknowledged, since a 200 tells the host it was delivered. Inbound handler rejections are caught and logged rather than left unhandled.
- Inbound requests are acknowledged as soon as the message is accepted, with handlers running under the caller's `waitUntil`, instead of the host blocking on the full handler chain.
- Request bodies are counted while streaming rather than buffered before the size check.
- `notify` no longer awaits a response.
- Response ids are matched against request ids, and the id counter is per client.
- `RpcErrorCode.REPLAY_REJECTED` renamed to `STALE_TIMESTAMP`, matching what the check actually does.
- `engines.node >= 20`, `sideEffects: false`, and a `prepublishOnly` guard added.
- Signing imports `node:crypto` rather than the bare `crypto` specifier, so bundlers stop attempting a browser polyfill. The build no longer strips that prefix back out.
- zod widened to `^3.0.0 || ^4.0.0`, matching what `chat` already requires of its consumers. The schemas were pinned to v4-only APIs, so a consumer satisfying `chat` with zod v3 installed a second nested copy of v4; they now use the subset both majors share and are exercised against 3.25 and 4.6.
- The published package no longer ships sourcemaps, and no longer exports `StreamRegistry`, `createRemoteChat`, or the wire schemas. Those are internals, and the package is 22.5 kB from 50.3 kB as a result.

## 0.1.0

Initial release.

[0.5.0]: https://github.com/nivandres/chat-adapter-remote/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/nivandres/chat-adapter-remote/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nivandres/chat-adapter-remote/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/nivandres/chat-adapter-remote/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/nivandres/chat-adapter-remote/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nivandres/chat-adapter-remote/releases/tag/v0.1.0
