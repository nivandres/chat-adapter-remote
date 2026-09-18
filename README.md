# chat-adapter-remote

Run any [Chat SDK](https://chat-sdk.dev) `Adapter` in a different process from your bot logic, bridged over signed HTTP JSON-RPC.

Some platforms — WhatsApp via Baileys, for example — need a process that holds a live connection continuously. This splits that connection from the bot logic: a **host** process holds the real, unmodified adapter; a **consumer** process holds the real `Chat` instance and your handlers. Neither side changes; `Chat` sees an ordinary adapter, and the adapter sees an ordinary `ChatInstance`.

```bash
npm i chat-adapter-remote
```

ESM only, Node >= 20 or Bun. Peers: `chat` and `@chat-adapter/shared`. The second is a peer because adapter errors are rebuilt as their original class across the boundary, so `instanceof` in your handlers only works against the same copy you have. Install both at matching versions — `@chat-adapter/shared` pins `chat` exactly, so a version split there quietly gives you two copies of `chat` as well.

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

Mount `chat.webhooks.remote` — not `remote.handleWebhook` — as the `consumerUrl` route; that is what triggers Chat's lazy adapter initialization. Register the adapter under the same key as its `name`.

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

`start()` is idempotent; `ready` is shorthand for it. `stop()` disconnects the adapter, drops open streams, and refuses further dispatch. `onError` covers the `initialize`, `forward`, `dispatch`, and `shutdown` phases.

## What crosses the bridge

**Outbound** (into the real adapter) is the whole `Adapter` interface except its four synchronous members — `encodeThreadId`, `decodeThreadId`, `renderFormatted`, `parseMessage` — which cannot be answered over RPC and which Chat core never calls on an adapter it holds. They throw if you call them yourself.

Two members return live values, so each keeps its object on the host and is reached by id: `scheduleMessage` returns a `cancel()` that calls back, and `rehydrateAttachment` returns a `fetchData()` that fetches the bytes through the host.

**Inbound** (into the real `Chat`) covers messages, reactions, edits, deletes, button clicks, slash commands, turn cancellation, and log forwarding. The rest of `ChatInstance` — modal and options-load events, agent-session and app-home events, direct state and history access — is not bridged and resolves to a logged no-op rather than throwing, because adapters call these from inside their own event loops where a throw would kill the host process. Those features silently do nothing.

The host reports which optional members its adapter actually implements, and the consumer removes the rest from itself. Chat decides what an adapter can do with `adapter.method?.()`, so a method the real adapter never had stays absent and its built-in fallback still applies.

`isDM`, `channelIdFromThreadId`, and `getChannelVisibility` are synchronous, so they are answered from facts the host attaches to each inbound message and caches per thread. A thread the consumer has not seen falls back to the SDK defaults.

## Streaming

`stream` takes an AsyncIterable, which cannot be an RPC argument, so it becomes open/push/end. One push is in flight at a time: the first chunk leaves immediately and whatever arrives while it travels goes out together, so batching follows the round trip and a pause in the producer never holds buffered chunks back.

When the host's adapter declines to stream — returning `null` before reading anything, which is how it hands back to Chat SDK's post-and-edit fallback — the host says so when the stream is opened and the consumer returns without touching the caller's iterable, because that fallback re-reads it.

## Delivery

At-most-once. No retry, queue, or deduplication: a failed forward is logged and dropped. An acknowledgement means "received", not "handled".

Chat core serializes work per thread and drops by default, so a burst on a single thread mostly does not reach your handlers. That is core behavior rather than the bridge — distinct threads all run — but it matters here because a host draining a backlog after a reconnect sends exactly that shape of burst. Set a `queue` concurrency strategy on the consumer's `Chat` if you need every message.

Attachment bytes are inlined as base64 and counted against `maxBodyBytes`. Larger media needs out-of-band transfer.

## Security

- Every request in both directions is HMAC-SHA256 signed over the raw body and compared in constant time.
- Signed timestamps outside a tolerance window are rejected, and each signature is accepted only once inside it. The default replay store is per-process and bounded; pass a `replayGuard` — `seen()` may be async — to share one across instances.
- Bodies are rejected past `maxBodyBytes` while being read, before they are buffered.
- Dispatch is a closed Zod allowlist of method literals. Neither side is ever indexed with a string from the wire.
- Adapter errors map to fixed codes and are rebuilt as their original class on the far side. Unrecognized errors collapse to a generic message, so host internals never leave the host.
- One shared secret covers both directions, with no key id or rotation path. A leaked secret grants full send-as-the-bot access.

## Limits

One adapter per host: multiple tenants means one host per tenant, with several named `RemoteAdapter`s on one `Chat`. No shared-process registry, no delivery guarantee, and no batching of forwarded logs. Signing uses `node:crypto` and the codec uses `Buffer`, so neither side runs on edge runtimes without a Node compatibility layer.
