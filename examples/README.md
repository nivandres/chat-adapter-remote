# Manual verification: real WhatsApp via chat-adapter-baileys

These two scripts are not part of the automated test suite (`bun test`). They require a real WhatsApp account and a phone to scan a QR code, which cannot be automated in CI.

## Run it

Both scripts need `bun` (they use `Bun.serve`) and a shared secret set in the environment:

```bash
export CHAT_ADAPTER_REMOTE_SECRET=$(openssl rand -hex 32)
```

Use the same value in both terminals. Terminal 1:

```bash
bun run examples/baileys-host.ts
```

Scan the QR code with WhatsApp (Linked devices → Link a device) on the number to use. Once connected, terminal 2:

```bash
bun run examples/baileys-consumer.ts
```

From a different WhatsApp number, message the linked number. The message appears in the consumer terminal, and an `echo: ...` reply arrives back on WhatsApp. This exercises the full round trip: WhatsApp → Baileys socket (host process) → `RemoteChat` → HTTP → `RemoteAdapter` → `Chat.processMessage` (consumer process) → the registered handler → `RemoteAdapter.postMessage` → HTTP → `AdapterHost` → `BaileysAdapter.postMessage` → WhatsApp.

Session credentials are saved to `./examples/.baileys-auth` after the first successful scan; restarting `baileys-host.ts` reconnects without a new QR prompt. That directory holds a live WhatsApp session and is excluded via `.gitignore` — never commit it.

## Known gaps

Plain text messages fit the bridge: `BaileysAdapter.initialize()` only calls `getLogger`, and inbound text routes through `processMessage`, both bridged. Two operations are not bridged. They no longer crash the host — the call is logged and ignored — but the feature silently does nothing:

- Reacting to a message — the adapter calls `chat.processReaction(...)`.
- Voting in a poll — the adapter calls `chat.getState()` for decryption-state persistence.
