import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import { Chat, Message, parseMarkdown, type ChatInstance } from "chat";

import { createRemoteAdapter } from "../src/adapter";
import { serveAdapter } from "../src/host";
import { encode, decode } from "../src/rpc/codec";
import { OUTBOUND_CALLS } from "../src/rpc/methods";
import { sign, verify } from "../src/rpc/signing";
import { serializeMessage, deserializeMessage } from "../src/rpc/message-wire";
import { startRealServer } from "../src/e2e/http-bridge";

const SECRET = "x".repeat(64);
const THREAD = "mock:general:1";

function message(text: string): Message {
  return new Message({
    id: "m1",
    threadId: THREAD,
    text,
    formatted: parseMarkdown(text),
    raw: { platform: "mock", ts: "1700000000.000" },
    author: {
      userId: "u1",
      userName: "alice",
      fullName: "Alice",
      isBot: false,
      isMe: false,
    },
    metadata: { dateSent: new Date(), edited: false },
    attachments: [],
  });
}

function bench(name: string, iterations: number, fn: () => void) {
  fn();
  const start = process.hrtime.bigint();
  for (let i = 0; i < iterations; i++) fn();
  const ns = Number(process.hrtime.bigint() - start) / iterations;
  console.log(
    `  ${name.padEnd(34)} ${(ns / 1000).toFixed(2).padStart(9)} us/op`,
  );
}

async function benchAsync(
  name: string,
  iterations: number,
  fn: () => Promise<unknown>,
) {
  await fn();
  const start = process.hrtime.bigint();
  for (let i = 0; i < iterations; i++) await fn();
  const ns = Number(process.hrtime.bigint() - start) / iterations;
  console.log(
    `  ${name.padEnd(34)} ${(ns / 1000).toFixed(2).padStart(9)} us/op`,
  );
}

async function main() {
  const wire = await serializeMessage(message("hello world"));
  const body = JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "postMessage",
    params: [THREAD, "hello"],
  });
  const timestamp = String(Date.now());
  const signature = sign(body, timestamp, SECRET);
  const parsed = JSON.parse(body);

  console.log("\nper-stage cost (single-threaded, in-process)");
  bench("sign", 20000, () => sign(body, timestamp, SECRET));
  bench("verify", 20000, () => verify(body, timestamp, signature, SECRET));
  bench("zod allowlist parse", 20000, () =>
    OUTBOUND_CALLS.safeParse({
      method: parsed.method,
      id: parsed.id,
      params: parsed.params,
    }),
  );
  bench("JSON.parse (same body)", 20000, () => JSON.parse(body));
  bench("decode params", 20000, () => decode(parsed.params));
  bench("deserializeMessage", 5000, () => deserializeMessage(wire));
  // Reused, so the number is our serialization and not chat's Message + parseMarkdown.
  const sample = message("hello world");
  await benchAsync("serializeMessage", 5000, () => serializeMessage(sample));
  bench("Message construction (chat, fyi)", 2000, () => message("hello world"));
  await benchAsync("encode message", 5000, () => encode(wire));

  console.log("\nwire size");
  const inbound = JSON.stringify({
    jsonrpc: "2.0",
    id: 1,
    method: "processMessage",
    params: [THREAD, wire, { channelId: "mock:general", isDM: false }],
  });
  console.log(
    `  inbound processMessage body        ${String(Buffer.byteLength(inbound)).padStart(9)} bytes`,
  );
  console.log(
    `  outbound postMessage body          ${String(Buffer.byteLength(body)).padStart(9)} bytes`,
  );

  // Real sockets, both directions.
  let hostChat!: ChatInstance;
  let chat!: Chat;
  const adapter = createMockAdapter("mock", {
    initialize: async (instance: ChatInstance) => void (hostChat = instance),
  });
  const consumerServer = await startRealServer((request) =>
    chat.webhooks.mock(request, {}),
  );
  const host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: `${consumerServer.url}/inbound`,
  });
  const hostServer = await startRealServer(host.fetch);
  const remote = createRemoteAdapter({
    url: `${hostServer.url}/rpc`,
    secret: SECRET,
    name: "mock",
  });
  chat = new Chat({
    userName: "mock-bot",
    adapters: { mock: remote },
    state: createMemoryState(),
  });
  await host.ready;

  console.log("\nend to end over localhost sockets");
  await benchAsync("outbound postMessage round trip", 300, () =>
    remote.postMessage(THREAD, "hi"),
  );

  let delivered = 0;
  chat.onNewMessage(/.*/, async () => void delivered++);
  const N = 300;
  const startedAt = process.hrtime.bigint();
  await Promise.all(
    Array.from({ length: N }, (_, i) =>
      hostChat.processMessage(adapter, `mock:t${i}:1`, message("hello world")),
    ),
  );
  const totalMs = Number(process.hrtime.bigint() - startedAt) / 1e6;
  console.log(
    `  inbound ${N} msgs across threads    ${totalMs.toFixed(0).padStart(9)} ms  (${(N / (totalMs / 1000)).toFixed(0)} msg/s, 8 in flight)`,
  );

  await hostServer.close();
  await consumerServer.close();
  process.exit(0);
}

void main();
