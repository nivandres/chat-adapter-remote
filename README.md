# chat-adapter-remote

Run any [Chat SDK](https://chat-sdk.dev) `Adapter` in a different process from your bot logic, bridged over signed HTTP JSON-RPC.

Some platforms — WhatsApp via Baileys, for example — need a process that holds a live connection continuously. This splits that connection from the bot logic: a **host** process holds the real, unmodified adapter; a **consumer** process holds the real `Chat` instance and your handlers. Neither side changes; `Chat` sees an ordinary adapter, and the adapter sees an ordinary `ChatInstance`.

```bash
npm i chat-adapter-remote
```

ESM only, Node >= 20 or Bun. Peers: `chat` and `@chat-adapter/shared`, which must be installed at matching versions.

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

**Outbound** is the whole `Adapter` interface except four synchronous members — `encodeThreadId`, `decodeThreadId`, `renderFormatted`, `parseMessage` — which cannot be answered over a round trip. They throw if you call them.

**Inbound** is every event whose payload is plain data: messages, reactions, edits, deletes, button clicks, slash commands, modals, options load, agent-session, assistant and app-home events, and turn cancellation. `getState`, `history` and `transcripts` are not bridged and resolve to a logged no-op.

The host reports which optional members its adapter actually implements, and the consumer removes the rest, so Chat's own fallbacks still apply to anything the real adapter never had.

`isDM`, `channelIdFromThreadId` and `getChannelVisibility` are answered from facts the host sends with each message. A thread the consumer has not seen yet falls back to the SDK defaults.

## Streaming

Adapters with native streaming get it. When the host's adapter declines to stream, Chat's own post-and-edit fallback takes over as usual.

## Delivery

At-most-once: a failed forward is logged and dropped, and an acknowledgement means "received", not "handled".

Chat serializes work per thread and drops by default, so a burst on a single thread mostly does not reach your handlers — set a `queue` concurrency strategy on the consumer's `Chat` if you need every message. This matters here because a host draining a backlog after a reconnect sends exactly that shape of burst.

Attachments are inlined as base64 and bounded by `maxBodyBytes` (5 MB default), which has to be raised on **both** sides. For anything larger, give the attachment a `url` instead: the platform fetches it directly and it never crosses the bridge.

## Security

- Every request in both directions is HMAC-SHA256 signed over the raw body and compared in constant time.
- Signatures are single-use inside a tolerance window. The default replay store is per-process; pass a `replayGuard` — `seen()` may be async — to share one across instances.
- Bodies are rejected past `maxBodyBytes` while being read.
- Adapter errors are rebuilt as their original class on the far side. Unrecognized errors collapse to a generic message, so host internals never leave the host.
- One shared secret covers both directions, with no key id or rotation path. A leaked secret grants full send-as-the-bot access.

## Limits

One adapter per host: multiple tenants means one host per tenant, with several named `RemoteAdapter`s on one `Chat`. No shared-process registry, no delivery guarantee, and no batching of forwarded logs. Signing uses `node:crypto` and the codec uses `Buffer`, so neither side runs on edge runtimes without a Node compatibility layer.
