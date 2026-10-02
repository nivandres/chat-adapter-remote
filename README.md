# chat-adapter-remote

Run any [Chat SDK](https://chat-sdk.dev) `Adapter` in a different process from your bot logic, bridged over signed HTTP JSON-RPC.

Some platforms — WhatsApp via Baileys, for example — need a process that holds a live connection continuously. This splits that connection from the bot logic: a **host** process holds the real, unmodified adapter; a **consumer** process holds the real `Chat` instance and your handlers. Neither side changes; `Chat` sees an ordinary adapter, and the adapter sees an ordinary `ChatInstance`.

```bash
npm i chat-adapter-remote
```

ESM only, Node >= 20 or Bun. Peers: `chat` and `@chat-adapter/shared`, which must be installed at matching versions.

`url`, `consumerUrl` and `secret` fall back to `CHAT_ADAPTER_REMOTE_URL`, `CHAT_ADAPTER_REMOTE_CONSUMER_URL` and `CHAT_ADAPTER_REMOTE_SECRET`.

## Host

```ts
import { serveAdapter } from "chat-adapter-remote/host";

const host = serveAdapter(realAdapter, {
  secret: process.env.CHAT_ADAPTER_REMOTE_SECRET!,
  consumerUrl: "https://your-app.example.com/api/webhooks/remote",
});

export const POST = host.fetch;
```

`host.fetch` is a bound `(request: Request) => Promise<Response>`, so it goes straight into any Fetch-API router. Frameworks built on `node:http` need their own adapter for that, the same as for any other Fetch handler.

Adapters driven by platform webhooks rather than a socket need a second route. `host.handleWebhook` waits for startup first, so an early delivery cannot reach a half-initialized adapter.

## Consumer

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

export async function POST(request: Request) {
  return chat.webhooks.remote(request, { waitUntil });
}
```

Mount `chat.webhooks.remote` — not `remote.handleWebhook` — as the `consumerUrl` route; that is what triggers Chat's lazy adapter initialization. Register it under the key you pass as `name`; thread ids are translated to it, whatever the host's adapter is called.

Pass `waitUntil` on serverless. Events are acknowledged as soon as they are accepted, so without it the runtime can freeze the handler mid-turn and the reply is never sent.

## Lifecycle

`serveAdapter` connects during construction. `createAdapterHost` is the same host left stopped:

```ts
const host = createAdapterHost(adapter, {
  secret,
  consumerUrl,
  onReady: () => console.log("connected"),
  onError: (error, { phase, threadId }) => report(error, { phase, threadId }),
});

await host.start(); // rejects if the adapter fails to connect
process.on("SIGTERM", () => host.stop());
```

`start()` is idempotent; `ready` is shorthand for it. `stop()` disconnects the adapter, drops open streams, and refuses further dispatch. `onError` covers the `initialize`, `forward`, `dispatch`, `shutdown` and `adapter` phases.

`host.emit({ type: "qr", code })` reaches the consumer's `onEvent`, for showing a pairing QR or connection state in your own UI.

`serveAdapters({ [name]: { adapter, secret, consumerUrl } })` runs several adapters in one process, each reached at `/<path>/<name>` with its own secret; one that fails to connect leaves the others running. `hosts.add(name, entry)` and `hosts.remove(name)` change them at runtime.

For one adapter per tenant record, `serveTenants({ list, load })` serves the ids `list()` returns, each built by `load(id)` from your record — credentials, secret and `consumerUrl` included. Call `tenants.load(id)` when a record changes: it replaces the running adapter, or removes it when `load` returns nothing. A route nobody added answers 404 and never triggers a lookup.

## What crosses the bridge

**Outbound** is the whole `Adapter` interface except four synchronous members — `encodeThreadId`, `decodeThreadId`, `renderFormatted`, `parseMessage` — which cannot be answered over a round trip. They throw if you call them.

**Inbound** is every event whose payload is plain data: messages, reactions, edits, deletes, button clicks, slash commands, modals, options load, agent-session, assistant and app-home events, and turn cancellation. `getState`, `history` and `transcripts` are not bridged and resolve to a logged no-op.

Adapters that persist their own data call `chat.getState()`. That reaches the consumer's store, limited by the consumer's `hostState`: `"scoped"` (default) lends keyed values and lists under a prefix of its own, `"full"` the whole store, `"off"` nothing. Pass `state` to the host to give it a store of its own instead.

The host reports which optional members its adapter actually implements, and the consumer removes the rest, so Chat's own fallbacks still apply to anything the real adapter never had.

`isDM`, `channelIdFromThreadId` and `getChannelVisibility` are answered from facts the host sends with each message, kept in the consumer's store so another instance can load them. A thread no instance has seen falls back to the SDK defaults.

Methods outside the `Adapter` interface are not exposed unless the host says so. List them with `customMethods`, or pass `true` for the adapter's own public methods, and they arrive on the consumer over the same signed protocol:

```ts
serveAdapter(whatsapp, { secret, consumerUrl, customMethods: ["setPresence"] });

const remote = createRemoteAdapter<BaileysAdapter>({ url, secret });
await remote.setPresence(jid, "composing");
```

## Streaming

Streaming behaves as it would in-process: the adapter's own streaming when it has one, otherwise Chat's post-and-edit. A turn that is cut off ends the stream with its `signal` aborted, and a source that fails fails the adapter's read.

The host's `stream.mode` can stream for an adapter that has none. `"buffer"` gathers the reply behind a typing indicator and posts it once; `stream.publishOnAbort` decides what a cut-off reply does (`"partial"` by default, `"discard"`, or `{ minChars }`). `"edit"` posts and then edits, and keeps what it showed if cut off. Neither posts an empty reply: it rejects with `StreamDiscardedError`, so it is not mistaken for a failure.

## Delivery

A forward that never reached the consumer is sent again every minute for 24 hours (`forwardRetry`: `intervalMs`, `backoff`, `maxIntervalMs`, `retentionMs`, `maxAttempts`), and reported to `onDropped` if it never arrives. One that timed out is not, since it may have been handled. The queue lives in memory by default; pass a `forwardQueue` to keep it somewhere shared. An acknowledgement means "received", not "handled". Chat deduplicates a resend for 10 minutes, so one acknowledged late and resent after that can be handled twice.

A consumer that starts while the host is down recovers on its own: each call tries again, rather than failing for good.

Chat serializes work per thread and drops by default, so a burst on a single thread mostly does not reach your handlers — set a `queue` concurrency strategy on the consumer's `Chat` if you need every message. This matters here because a host draining a backlog after a reconnect sends exactly that shape of burst.

Attachments are inlined as base64 while they stay under about 4 MB, or under `maxBodyBytes` when you set one. Past that the host keeps the bytes and the consumer reads them through `fetchData()`, which is what the SDK already calls — set `inlineAttachments` to `true` or `false` to force either. For anything large, give the attachment a `url` instead: the platform fetches it directly and it never crosses the bridge.

## Security

- Every request in both directions is HMAC-SHA256 signed over a timestamp, a nonce and the raw body, and compared in constant time.
- Signatures are single-use inside a tolerance window. The default replay store is per-process; pass a `replayGuard` — `seen()` may be async — to share one across instances.
- `maxBodyBytes` bounds how much is read from a request or a response. It is unlimited by default, since both ends are yours; set it if either endpoint is reachable from somewhere you do not control.
- Adapter errors are rebuilt as their original class on the far side. Unrecognized errors collapse to a generic message, so host internals never leave the host.
- One shared secret covers both directions, with no key id or rotation path. A leaked secret grants full send-as-the-bot access.

## Limits

No batching of forwarded logs. Signing uses `node:crypto` and the codec uses `Buffer`, so neither side runs on edge runtimes without a Node compatibility layer.

## License

MIT
