# chat-adapter-remote

Run any [Chat SDK](https://chat-sdk.dev) `Adapter` in a different process from your bot logic, bridged over signed HTTP JSON-RPC.

Some platforms (WhatsApp via Baileys, for example) require a persistent, continuously-running process holding a live connection. `chat-adapter-remote` splits that connection from the bot logic: a **host** process holds the real, unmodified adapter; a **consumer** process holds the real `Chat` instance and your handlers. Neither side needs to know it's talking to the other over a network.

## Install

```bash
npm i chat-adapter-remote
```

## How it works

- **Consumer process** — your real `Chat` instance and handlers. Register `createRemoteAdapter(...)` as the adapter; it forwards every outbound call (`postMessage`, `editMessage`, ...) to the host.
- **Host process** — the real, unmodified adapter (Chatwoot, Baileys, whatever). Wrap it with `serveAdapter(...)` from the `./host` subpath; it hands the real adapter a fake `ChatInstance` so its own `initialize()`/inbound events forward back to the consumer.

## Usage

**Host** (wherever the real adapter's persistent connection lives):

```ts
import { serveAdapter } from "chat-adapter-remote/host";
import { createChatwootAdapter } from "chat-adapter-chatwoot";

const host = serveAdapter(createChatwootAdapter({/* ... */}), {
  secret: process.env.CHAT_ADAPTER_REMOTE_SECRET!,
  consumerUrl: "https://your-app.example.com/api/webhooks/remote",
});

// Mount host.handleRequest as a route, e.g. in a Hono/Express/Next.js handler:
// app.post("/rpc", (req) => host.handleRequest(req));
```

**Consumer** (wherever `Chat` and your handlers live):

```ts
import { Chat } from "chat";
import { createRemoteAdapter } from "chat-adapter-remote";
import { createRedisState } from "@chat-adapter/state-redis";

const remote = createRemoteAdapter({
  url: "https://your-worker.example.com/rpc",
  secret: process.env.CHAT_ADAPTER_REMOTE_SECRET!,
});

const chat = new Chat({
  userName: "mybot",
  adapters: { remote },
  state: createRedisState(),
});

chat.onNewMention(async (thread) => {
  await thread.post("Hello from the other side!");
});

// Mount chat.webhooks.remote as the consumerUrl route above.
// Call chat.webhooks.remote(request, options), not remote.handleWebhook(request)
// directly — chat.webhooks.<name> triggers Chat's lazy adapter initialization
// (the handshake) before the first inbound event is handled.
```

## What's bridged

**Outbound** (consumer → host, into the real adapter): `postMessage`, `editMessage`, `deleteMessage`, `addReaction`, `removeReaction`, `fetchMessages`, `fetchThread`, `startTyping`, `disconnect`.

**Inbound** (host → consumer, into the real `Chat`): `processMessage`, and log lines (best-effort).

`encodeThreadId`/`decodeThreadId`/`renderFormatted` are declared synchronous on `Adapter` and are not called by Chat SDK core on an adapter it holds, so `RemoteAdapter` does not forward them. `channelIdFromThreadId` is called by core, so it is answered locally: cached from every inbound message's host-computed value, falling back to the `{adapter}:{channel}` convention for any thread not yet seen.

Any `ChatInstance` or `Adapter` member outside this list throws a clear, typed error if reached.

## Security

The host's dispatch is a closed Zod allowlist (never `adapter[method]` with a raw string), HMAC-SHA256 signed with replay protection, and every thrown adapter error is mapped to a fixed code and reconstructed as the exact real error class on the other side — see `src/rpc/errors.ts` and `src/host/adapter-host.ts`.
