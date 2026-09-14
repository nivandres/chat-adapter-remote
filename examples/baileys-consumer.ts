/**
 * Manual verification harness, not part of the automated test suite. See
 * examples/README.md. Run with: bun run examples/baileys-consumer.ts
 * (after examples/baileys-host.ts is running and connected)
 */
import { createMemoryState } from "@chat-adapter/state-memory";
import { Chat } from "chat";

import { createRemoteAdapter } from "../src/adapter";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value)
    throw new Error(
      `${name} is required — set it before running this example.`,
    );
  return value;
}

const SECRET = requireEnv("CHAT_ADAPTER_REMOTE_SECRET");
const HOST_URL = process.env.HOST_URL ?? "http://127.0.0.1:4000/rpc";
const CONSUMER_PORT = Number(process.env.CONSUMER_PORT ?? 4001);

const whatsapp = createRemoteAdapter({ url: HOST_URL, secret: SECRET });

const chat = new Chat({
  userName: "chat-adapter-remote-demo",
  adapters: { whatsapp },
  state: createMemoryState(),
});

chat.onNewMention(async (thread, message) => {
  console.log(`[whatsapp] ${message.author.userName}: ${message.text}`);
  await thread.post(`echo: ${message.text}`);
});

const server = Bun.serve({
  port: CONSUMER_PORT,
  hostname: "127.0.0.1",
  fetch: (request) => {
    const url = new URL(request.url);
    if (url.pathname === "/inbound") return chat.webhooks.whatsapp(request, {});
    return new Response("not found", { status: 404 });
  },
});

console.log(
  `Consumer listening on http://${server.hostname}:${server.port}/inbound — send a WhatsApp message to test.`,
);
