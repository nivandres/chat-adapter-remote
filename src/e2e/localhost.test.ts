import { AdapterRateLimitError } from "@chat-adapter/shared";
import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import {
  Chat,
  Message,
  getEmoji,
  parseMarkdown,
  type Adapter,
  type ChatInstance,
  type StreamChunk,
} from "chat";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createRemoteAdapter, type RemoteAdapter } from "../adapter";
import { serveAdapter, type AdapterHost } from "../host";
import { startRealServer, type RealHttpServer } from "./http-bridge";

const SECRET = "e2e-secret";
const THREAD = "mock:general:1";

const author = {
  userId: "u1",
  userName: "alice",
  fullName: "Alice",
  isBot: false,
  isMe: false,
};

function message(text: string, threadId = THREAD, overrides = {}): Message {
  return new Message({
    id: `m-${Math.random()}`,
    threadId,
    text,
    formatted: parseMarkdown(text),
    raw: {},
    author,
    metadata: { dateSent: new Date("2024-01-01T00:00:00.000Z"), edited: false },
    attachments: [],
    ...overrides,
  });
}

/** Everything the same as in-process, except both directions cross real sockets. */
describe("e2e over real localhost HTTP", () => {
  let hostServer: RealHttpServer;
  let consumerServer: RealHttpServer;
  let adapter: Adapter;
  let hostChat: ChatInstance;
  let host: AdapterHost;
  let remote: RemoteAdapter;
  let chat: Chat;

  async function boot(overrides: Partial<Adapter> = {}) {
    adapter = createMockAdapter("mock", {
      initialize: vi.fn(async (instance: ChatInstance) => {
        hostChat = instance;
      }),
      ...overrides,
    });

    consumerServer = await startRealServer((request) =>
      chat.webhooks.mock!(request, {}),
    );
    host = serveAdapter(adapter, {
      secret: SECRET,
      consumerUrl: `${consumerServer.url}/inbound`,
    });
    hostServer = await startRealServer(host.fetch);

    remote = createRemoteAdapter({
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
  }

  async function reboot(overrides: Partial<Adapter>) {
    await hostServer.close();
    await consumerServer.close();
    await boot(overrides);
  }

  beforeEach(() => boot());

  afterEach(async () => {
    await hostServer.close();
    await consumerServer.close();
  });

  it("carries a mention to a handler and the reply back to the adapter", async () => {
    let replied!: () => void;
    const reply = new Promise<void>((resolve) => (replied = resolve));
    chat.onNewMention(async (thread) => {
      await thread.post("hello over a real socket");
      replied();
    });

    await hostChat.processMessage(adapter, THREAD, message("hey @mock-bot"));

    await reply;
    expect(adapter.postMessage).toHaveBeenCalledWith(THREAD, expect.anything());
  });

  it("carries attachment bytes intact", async () => {
    const bytes = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x00, 0x01, 0xff, 0xfe]);
    let received!: Message;
    let done!: () => void;
    const arrived = new Promise<void>((resolve) => (done = resolve));
    chat.onNewMention(async (_thread, incoming) => {
      received = incoming;
      done();
    });

    await hostChat.processMessage(
      adapter,
      THREAD,
      message("@mock-bot a file", THREAD, {
        attachments: [
          {
            type: "file",
            name: "payload.png",
            mimeType: "image/png",
            fetchData: async () => bytes,
          },
        ],
      }),
    );

    await arrived;
    expect(received.attachments[0]!.name).toBe("payload.png");
    expect(received.attachments[0]!.data).toEqual(bytes);
  });

  it("streams a reply chunk by chunk to the real adapter", async () => {
    const chunks: Array<string | StreamChunk> = [];
    await reboot({
      stream: vi.fn(async (_threadId, textStream) => {
        for await (const chunk of textStream) chunks.push(chunk);
        return { id: "streamed", threadId: THREAD, raw: {} };
      }) as never,
    });

    const result = await remote.stream!(THREAD, {
      async *[Symbol.asyncIterator]() {
        for (const token of ["Hel", "lo ", "world"]) {
          yield token;
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
      },
    });

    expect(chunks.join("")).toBe("Hello world");
    expect(result).toMatchObject({ id: "streamed" });
  });

  it("carries a reaction event with its emoji singleton", async () => {
    let seen!: string;
    let done!: () => void;
    const arrived = new Promise<void>((resolve) => (done = resolve));
    chat.onReaction([getEmoji("thumbs_up")], async (event) => {
      seen = event.emoji.name;
      done();
    });

    hostChat.processReaction({
      added: true,
      emoji: getEmoji("thumbs_up"),
      rawEmoji: "+1",
      messageId: "m1",
      raw: {},
      threadId: THREAD,
      user: author,
    } as never);

    await arrived;
    expect(seen).toBe("thumbs_up");
  });

  it("keeps twenty concurrent threads separate", async () => {
    const seen: string[] = [];
    let done!: () => void;
    const all = new Promise<void>((resolve) => (done = resolve));
    chat.onNewMention(async (thread, incoming) => {
      await thread.post(`ack ${incoming.text}`);
      seen.push(incoming.text);
      if (seen.length === 20) done();
    });

    await Promise.all(
      Array.from({ length: 20 }, (_, index) =>
        hostChat.processMessage(
          adapter,
          `mock:t${index}:1`,
          message(`@mock-bot ${index}`, `mock:t${index}:1`),
        ),
      ),
    );

    await all;
    expect(new Set(seen).size).toBe(20);
    expect(adapter.postMessage).toHaveBeenCalledTimes(20);
  });

  it("rebuilds an adapter error class across the sockets", async () => {
    await reboot({
      postMessage: vi
        .fn()
        .mockRejectedValue(new AdapterRateLimitError("mock", 42)),
    });

    await expect(remote.postMessage(THREAD, "hi")).rejects.toMatchObject({
      name: "AdapterRateLimitError",
      retryAfter: 42,
    });
  });

  it("rejects an unsigned request over the real socket", async () => {
    const response = await fetch(`${hostServer.url}/rpc`, {
      method: "POST",
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "postMessage",
        params: ["t", "hi"],
      }),
      headers: { "content-type": "application/json" },
    });

    expect(response.status).toBe(401);
    expect(adapter.postMessage).not.toHaveBeenCalled();
  });
});
