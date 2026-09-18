# chat-adapter-remote

Run any [Chat SDK](https://chat-sdk.dev) `Adapter` in a different process from your bot logic, bridged over signed HTTP JSON-RPC.

Some platforms (WhatsApp via Baileys, for example) need a persistent, continuously-running process holding a live connection. This splits that connection from the bot logic: a **host** process holds the real, unmodified adapter; a **consumer** process holds the real `Chat` instance and your handlers.

## Install

```bash
npm i chat-adapter-remote
```

Peer dependencies: `chat@^4.40.0` and `@chat-adapter/shared@^4.40.0`. ESM only.

Both sides require Node >= 20 or Bun. Signing uses `node:crypto` and the codec uses `Buffer`, so the consumer does **not** currently run on edge runtimes (Cloudflare Workers, Vercel Edge) without a Node compatibility layer.

`@chat-adapter/shared` is a peer, not a bundled dependency, on purpose: adapter errors are reconstructed across the boundary with `instanceof`, which only works when both sides resolve the same copy of those error classes.

## Usage

**Host** — wherever the real adapter's connection lives:

```ts
import { serveAdapter } from "chat-adapter-remote/host";

const host = serveAdapter(realAdapter, {
  secret: process.env.CHAT_ADAPTER_REMOTE_SECRET!,
  consumerUrl: "https://your-app.example.com/api/webhooks/remote",
});

// app.post("/rpc", (req) => host.handleRequest(req));
```

**Consumer** — wherever `Chat` and your handlers live:

```ts
import { Chat } from "chat";
import { createRemoteAdapter } from "chat-adapter-remote";

const remote = createRemoteAdapter({
  url: "https://your-worker.example.com/rpc",
  secret: process.env.CHAT_ADAPTER_REMOTE_SECRET!,
  name: "remote",
});

const chat = new Chat({ userName: "mybot", adapters: { remote }, state });

chat.onNewMention(async (thread) => {
  await thread.post("Hello from the other side!");
});

// Mount chat.webhooks.remote as the consumerUrl route. Call
// chat.webhooks.remote(request, options) rather than remote.handleWebhook
// directly — that is what triggers Chat's lazy adapter initialization.
```

Register the adapter under the same key as its `name`, since Chat derives state keys from one and routes webhooks by the other.

## Host lifecycle

`serveAdapter` initializes during construction, so the one-liner above keeps working. For explicit control — and to see a failed connection loudly rather than through an object that silently never works — use `start()` and `stop()`:

```ts
const host = serveAdapter(adapter, {
  secret,
  consumerUrl,
  autoStart: false,
  onReady: () => console.log("connected"),
  onError: (error, { phase, threadId }) => report(error, { phase, threadId }),
  logForwardLevel: "warn",
  maxConcurrentForwards: 16,
});

await host.start(); // rejects if the adapter fails to connect
process.on("SIGTERM", () => host.stop());
```

`start()` is idempotent and returns the same promise however often it is called; `ready` is a shorthand for it. `onError` fires for the `initialize`, `forward`, `dispatch`, and `shutdown` phases. `maxConcurrentForwards` caps how many inbound messages are in flight to the consumer at once, so a backlog of queued platform messages cannot open one request each.

## Webhook-driven adapters

Adapters driven by platform webhooks rather than a socket need a second route on the host. `handlePlatformWebhook` waits for startup first, so an early delivery cannot reach a half-initialized adapter:

```ts
app.post("/rpc", (req) => host.handleRequest(req)); // from the consumer
app.post("/slack", (req) => host.handlePlatformWebhook(req)); // from the platform
```

Inbound events still reach the consumer through the same bridged `processMessage`, so only `processMessage`-driven adapters work end to end — see the matrix below.

## Capability matrix

Outbound (consumer → host, into the real adapter):

| Bridged                                                                                                                                     | Not bridged                                                                                                                                                                                                                                                                                                 |
| ------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `postMessage`, `editMessage`, `deleteMessage`, `addReaction`, `removeReaction`, `fetchMessages`, `fetchThread`, `startTyping`, `disconnect` | `stream`, `reply`, `endTyping`, `markAsRead`, `openDM`, `openModal`, `postEphemeral`, `postChannelMessage`, `postObject`, `editObject`, `scheduleMessage`, `fetchMessage`, `fetchChannelInfo`, `fetchChannelMessages`, `listThreads`, `getUser`, `onThreadSubscribe`, `fetchSubject`, `rehydrateAttachment` |

Unbridged optional methods are simply absent on the consumer adapter, which Chat treats as unsupported — the same as any adapter that omits them. Notably, omitting `stream` means Slack-style native streaming falls back to post-and-edit.

Inbound (host → consumer, into the real `Chat`):

| Bridged                          | Not bridged                                                                                                                                                                                                                            |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `processMessage`, log forwarding | `processReaction`, `processAction`, `processSlashCommand`, `processMessageUpdated`, `processMessageDeleted`, modal submit/close, options load, agent-session and assistant events, `getState`, `getUserName`, `history`, `transcripts` |

Unbridged `ChatInstance` members resolve to a logged no-op rather than throwing. Adapters call them from inside their own event loops, where a throw becomes an unhandled rejection that would kill the host process. The consequence is that the corresponding features silently do nothing — a WhatsApp reaction or poll vote is received by the adapter and then dropped at the bridge.

Capabilities carried over the handshake: `name`, `userName`, `botUserId`, `lockScope`, `persistThreadHistory`, `supportsTurnCancellation`. `isDM` and `channelIdFromThreadId` are answered from facts the host attaches to each inbound message; for a thread the consumer has not yet seen, `isDM` is `false` and `channelIdFromThreadId` falls back to the `{adapter}:{channel}` convention.

`encodeThreadId`, `decodeThreadId`, `renderFormatted`, and `parseMessage` are synchronous and throw if called. Chat SDK core does not call them on an adapter it holds.

## Delivery semantics

At-most-once, with no retry, queue, or deduplication. The host forwards a message, and a failed forward is logged and dropped. The consumer acknowledges as soon as the message is accepted and runs handlers under the caller's `waitUntil`, so an acknowledgement means "received", not "handled".

Attachment bytes are inlined base64, which costs about a third more than the raw size and counts against `maxBodyBytes` (default 5 MB). Larger media needs out-of-band transfer.

## Protocol

Both sides exchange `protocolVersion` during the handshake and refuse to initialize on a mismatch. The wire format is JSON-RPC 2.0 over HTTP POST.

## Security

- Every request in both directions is HMAC-SHA256 signed over the raw body, compared in constant time.
- Signed timestamps outside a tolerance window (default 30s) are rejected. This is timestamp validation, not replay protection: there is no nonce store, so a captured request can be replayed inside the window.
- Host dispatch is a closed Zod allowlist of method literals; the adapter is never indexed with a string from the wire.
- Bodies are counted while streaming and rejected past the limit rather than buffered first.
- Adapter errors map to fixed codes and are reconstructed as their original class on the far side. Unrecognized errors collapse to a generic message; original messages and stack traces do not leave the host.
- One shared secret covers both directions, with no key id or rotation path. A leaked secret grants full send-as-the-bot access.

## Not included

One adapter per host: multiple tenants means one host per tenant, with several named `RemoteAdapter`s on one `Chat`. There is no shared-process registry, no retry or delivery guarantee, no batching of forwarded logs, and no graceful-shutdown wiring.
