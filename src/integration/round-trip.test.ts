import { AdapterRateLimitError } from "@chat-adapter/shared";
import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import {
  Chat,
  Message,
  parseMarkdown,
  type Adapter,
  type ChatInstance,
} from "chat";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { createRemoteAdapter, RemoteAdapter } from "../adapter";
import { AdapterHost, serveAdapter } from "../host";

const SECRET = "test-shared-secret";
const HOST_URL = "https://host.test/rpc";
const CONSUMER_URL = "https://consumer.test/inbound";

function makeTestMessage(threadId: string, text: string): Message {
  return new Message({
    id: `m-${threadId}-${text.length}`,
    threadId,
    text,
    formatted: parseMarkdown(text),
    raw: {},
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

describe("round trip: RemoteAdapter <-> AdapterHost over an injectable-fetch loopback", () => {
  let mockAdapter: Adapter;
  let posted: Array<{ threadId: string; message: unknown }>;
  let capturedChat: ChatInstance;
  let host: AdapterHost;
  let remoteAdapter: RemoteAdapter;
  let chat: Chat;

  beforeEach(async () => {
    posted = [];

    mockAdapter = createMockAdapter("mock", {
      initialize: vi.fn(async (c: ChatInstance) => {
        capturedChat = c;
      }),
      postMessage: vi.fn(async (threadId: string, message: unknown) => {
        posted.push({ threadId, message });
        return { id: "sent-1", threadId, raw: {} };
      }),
    });

    // Both sides run real HTTP handling (signing, schema validation, dispatch); only the socket hop is stubbed.
    const stubFetch: typeof fetch = async (input, init) => {
      const request = new Request(input as string, init);
      const url = new URL(request.url);
      if (url.hostname === "host.test") return host.handleRequest(request);
      if (url.hostname === "consumer.test")
        return chat.webhooks.mock(request, {});
      throw new Error(`round-trip test: unexpected fetch to ${url}`);
    };

    host = serveAdapter(mockAdapter, {
      secret: SECRET,
      consumerUrl: CONSUMER_URL,
      fetch: stubFetch,
    });
    remoteAdapter = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      name: "mock",
      userName: "mock-bot",
      fetch: stubFetch,
    });

    chat = new Chat({
      userName: "mock-bot",
      adapters: { mock: remoteAdapter },
      state: createMemoryState(),
    });

    await host.ready;
  });

  it("delivers an inbound message through to a registered handler, and a reply back through to the mock adapter", async () => {
    chat.onNewMention(async (thread) => {
      await thread.post("hello from consumer");
    });

    await capturedChat.processMessage(
      mockAdapter,
      "mock:general:1",
      makeTestMessage("mock:general:1", "hey @mock-bot"),
    );

    expect(posted).toHaveLength(1);
    expect(posted[0]!.threadId).toBe("mock:general:1");
  });

  it("reconstructs the exact error class thrown by the real adapter", async () => {
    mockAdapter.postMessage = vi
      .fn()
      .mockRejectedValue(new AdapterRateLimitError("mock", 42));

    await expect(
      remoteAdapter.postMessage("mock:general:1", "hi"),
    ).rejects.toMatchObject({
      name: "AdapterRateLimitError",
      retryAfter: 42,
    });
  });

  it("caches channelId from inbound traffic so channelIdFromThreadId answers synchronously and correctly", async () => {
    await capturedChat.processMessage(
      mockAdapter,
      "mock:general:99",
      makeTestMessage("mock:general:99", "no mention here"),
    );
    expect(remoteAdapter.channelIdFromThreadId("mock:general:99")).toBe(
      "mock:general",
    );
  });

  it("falls back to the colon-convention for a threadId never seen inbound", () => {
    expect(remoteAdapter.channelIdFromThreadId("mock:other:1")).toBe(
      "mock:other",
    );
  });

  it("round-trips an attachment's binary data intact", async () => {
    const serialized = {
      ...makeTestMessage("mock:general:1", "see attached").toJSON(),
      attachments: [
        { type: "file", name: "a.txt", data: Buffer.from("payload") },
      ],
    };
    mockAdapter.fetchMessages = vi
      .fn()
      .mockResolvedValue({ messages: [serialized], nextCursor: undefined });

    const result = await remoteAdapter.fetchMessages("mock:general:1");
    const attachment = result.messages[0]!.attachments[0]!;
    expect(Buffer.isBuffer(attachment.data)).toBe(true);
    expect((attachment.data as Buffer).toString()).toBe("payload");
  });
});

describe("security negatives (no RemoteAdapter involved — hand-built signed requests)", () => {
  let mockAdapter: Adapter;
  let host: AdapterHost;

  beforeEach(async () => {
    mockAdapter = createMockAdapter("mock", {
      initialize: vi.fn().mockResolvedValue(undefined),
    });
    host = serveAdapter(mockAdapter, {
      secret: SECRET,
      consumerUrl: CONSUMER_URL,
      fetch: vi.fn(),
    });
    await host.ready;
  });

  async function signedRequest(
    body: string,
    secretOverride = SECRET,
    timestampOverride?: string,
  ) {
    const { sign } = await import("../rpc/signing");
    const timestamp = timestampOverride ?? String(Date.now());
    const signature = sign(body, timestamp, secretOverride);
    return new Request(HOST_URL, {
      method: "POST",
      body,
      headers: {
        "x-chat-adapter-remote-signature": signature,
        "x-chat-adapter-remote-timestamp": timestamp,
      },
    });
  }

  it("rejects a tampered signature and never dispatches", async () => {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "postMessage",
      params: ["t", "hi"],
    });
    const request = await signedRequest(body, "wrong-secret");
    const response = await host.handleRequest(request);
    expect(response.status).toBe(401);
    expect(mockAdapter.postMessage).not.toHaveBeenCalled();
  });

  it("rejects a stale timestamp and never dispatches", async () => {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "postMessage",
      params: ["t", "hi"],
    });
    const staleTimestamp = String(Date.now() - 60_000);
    const request = await signedRequest(body, SECRET, staleTimestamp);
    const response = await host.handleRequest(request);
    expect(response.status).toBe(401);
    expect(mockAdapter.postMessage).not.toHaveBeenCalled();
  });

  it("rejects an unknown method and never dispatches", async () => {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "__proto__",
      params: [],
    });
    const request = await signedRequest(body);
    const response = await host.handleRequest(request);
    const json = await response.json();
    expect(json.error.code).toBeDefined();
    expect(mockAdapter.postMessage).not.toHaveBeenCalled();
  });

  it("rejects wrong-arity params and never dispatches", async () => {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "deleteMessage",
      params: ["t"],
    });
    const request = await signedRequest(body);
    const response = await host.handleRequest(request);
    const json = await response.json();
    expect(json.error.code).toBeDefined();
    expect(mockAdapter.deleteMessage).not.toHaveBeenCalled();
  });

  it("rejects an oversized body", async () => {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "postMessage",
      params: ["t", "x".repeat(6_000_000)],
    });
    const request = await signedRequest(body);
    const response = await host.handleRequest(request);
    expect(response.status).toBe(413);
    expect(mockAdapter.postMessage).not.toHaveBeenCalled();
  });
});
