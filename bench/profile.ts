import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import { Chat, Message, parseMarkdown, type ChatInstance } from "chat";

import { createRemoteAdapter } from "../src/adapter";
import { serveAdapter } from "../src/host";
import { encode, decode } from "../src/rpc/codec";
import { seal, unseal } from "../src/rpc/sealing";
import { translateIds, translateParams } from "../src/thread-ids";
import { OUTBOUND_CALLS } from "../src/rpc/methods";
import { sign, verify } from "../src/rpc/signing";
import { serializeMessage, deserializeMessage } from "../src/rpc/message-wire";
import { startRealServer } from "../src/e2e/http-bridge";

const SECRET = "x".repeat(64);
const THREAD = "mock:general:1";

function message(text: string): Message {
  return new Message({
    // Unique, or Chat drops every message after the first as a duplicate.
    id: crypto.randomUUID(),
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

/** Latency distribution, since a mean hides the tail a chat user feels. */
async function percentiles(
  name: string,
  iterations: number,
  fn: () => Promise<unknown>,
) {
  for (let i = 0; i < 20; i++) await fn();
  const samples: number[] = [];
  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint();
    await fn();
    samples.push(Number(process.hrtime.bigint() - start) / 1000);
  }
  samples.sort((a, b) => a - b);
  const at = (q: number) =>
    samples[Math.min(samples.length - 1, Math.floor(q * samples.length))]!;
  const us = (value: number) => `${value.toFixed(0).padStart(6)} us`;
  console.log(
    `  ${name.padEnd(34)} p50 ${us(at(0.5))}  p95 ${us(at(0.95))}  p99 ${us(at(0.99))}`,
  );
}

async function throughput(
  name: string,
  total: number,
  concurrency: number,
  fn: () => Promise<unknown>,
) {
  let next = 0;
  const start = process.hrtime.bigint();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (next++ < total) await fn();
    }),
  );
  const seconds = Number(process.hrtime.bigint() - start) / 1e9;
  console.log(
    `  ${name.padEnd(34)} ${(total / seconds).toFixed(0).padStart(9)} ops/s  (${concurrency} in flight)`,
  );
}

/** A host and a consumer on real localhost sockets, as two processes would talk. */
async function pair(options: { encrypt?: boolean } = {}) {
  let hostChat!: ChatInstance;
  let chat!: Chat;
  const adapter = createMockAdapter("mock", {
    initialize: async (instance: ChatInstance) => void (hostChat = instance),
    stream: async (_threadId: string, chunks: AsyncIterable<unknown>) => {
      for await (const _chunk of chunks) void _chunk;
      return { id: "streamed", threadId: THREAD, raw: {} };
    },
  } as never);
  const consumerServer = await startRealServer((request) =>
    chat.webhooks.mock(request, {}),
  );
  const host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: `${consumerServer.url}/inbound`,
    ...options,
  });
  const hostServer = await startRealServer(host.fetch);
  const remote = createRemoteAdapter({
    url: `${hostServer.url}/rpc`,
    secret: SECRET,
    name: "mock",
    ...options,
  });
  chat = new Chat({
    userName: "mock-bot",
    adapters: { mock: remote },
    state: createMemoryState(),
    concurrency: { strategy: "concurrent" },
  });
  await host.ready;
  await chat.initialize();
  return {
    adapter,
    chat,
    remote,
    hostChat: () => hostChat,
    close: async () => {
      await host.stop();
      await hostServer.close();
      await consumerServer.close();
    },
  };
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
  const signature = sign(body, timestamp, "nonce", SECRET);
  const parsed = JSON.parse(body);

  console.log("\nper-stage cost (single-threaded, in-process)");
  bench("sign", 20000, () => sign(body, timestamp, "nonce", SECRET));
  bench("verify", 20000, () =>
    verify(body, timestamp, "nonce", signature, SECRET),
  );
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
  const inboundBody = JSON.stringify({ params: [THREAD, wire] });
  const sealed = seal(inboundBody, SECRET, "request.n");
  bench("seal (encrypt: true)", 20000, () =>
    seal(inboundBody, SECRET, "request.n"),
  );
  bench("unseal (encrypt: true)", 20000, () =>
    unseal(sealed, SECRET, "request.n"),
  );
  bench("translate thread ids, params", 20000, () =>
    translateParams([THREAD, "hello"], "mock", "remote"),
  );
  bench("translate thread ids, message", 20000, () =>
    translateIds(wire, "mock", "remote"),
  );

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

  console.log("\nbaselines, for scale");
  const echo = await startRealServer(async () => Response.json({ ok: true }));
  await percentiles("bare HTTP echo, localhost", 500, () =>
    fetch(echo.url, { method: "POST", body }),
  );
  await echo.close();
  const direct = createMockAdapter("mock");
  await percentiles("adapter call, in-process", 2000, () =>
    direct.postMessage(THREAD, "hi"),
  );

  for (const encrypt of [false, true]) {
    const b = await pair({ encrypt });
    console.log(
      `\nend to end over localhost sockets${encrypt ? ", encrypt: true" : ""}`,
    );
    await percentiles("outbound postMessage", 500, () =>
      b.remote.postMessage(THREAD, "hi"),
    );
    await throughput("outbound postMessage", 2000, 32, () =>
      b.remote.postMessage(THREAD, "hi"),
    );

    let arrived: () => void = () => {};
    b.chat.onNewMessage(/.*/, async () => arrived());
    await percentiles("inbound message to handler", 300, async () => {
      const handled = new Promise<void>((resolve) => (arrived = resolve));
      await b
        .hostChat()
        .processMessage(
          b.adapter,
          `mock:t${Math.random()}:1`,
          message("hello"),
        );
      await handled;
    });

    const chunks = Array.from({ length: 500 }, (_, i) => `token${i} `);
    await percentiles("stream of 500 chunks", 30, () =>
      b.remote.stream!(
        THREAD,
        (async function* () {
          yield* chunks;
        })(),
      ),
    );

    const megabyte = Buffer.alloc(1024 * 1024, 7);
    await percentiles("inbound with a 1 MB attachment", 30, async () => {
      const handled = new Promise<void>((resolve) => (arrived = resolve));
      await b.hostChat().processMessage(
        b.adapter,
        `mock:a${Math.random()}:1`,
        new Message({
          ...message("photo"),
          attachments: [
            {
              type: "image",
              name: "a.png",
              mimeType: "image/png",
              size: megabyte.length,
              fetchData: async () => megabyte,
            },
          ],
        }),
      );
      await handled;
    });
    await b.close();
  }

  process.exit(0);
}

void main();
