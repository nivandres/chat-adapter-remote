import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import {
  Chat,
  Message,
  parseMarkdown,
  type Adapter,
  type ChatInstance,
} from "chat";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createRemoteAdapter, RemoteAdapter } from "../adapter";
import { AdapterHost, serveAdapter } from "../host";
import { startRealServer, type RealHttpServer } from "./http-bridge";

const SECRET = "e2e-shared-secret";

/** Same round trip as src/integration/round-trip.test.ts, but over two real Node http servers on real localhost ports instead of an in-process stub. */
describe("e2e: RemoteAdapter <-> AdapterHost over real localhost HTTP", () => {
  let hostServer: RealHttpServer;
  let consumerServer: RealHttpServer;
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

    // Consumer's server starts first — RemoteChat needs consumerUrl at construction.
    consumerServer = await startRealServer((request) =>
      chat.webhooks.mock(request, {}),
    );

    host = serveAdapter(mockAdapter, {
      secret: SECRET,
      consumerUrl: `${consumerServer.url}/inbound`,
    });
    hostServer = await startRealServer((request) =>
      host.handleRequest(request),
    );

    remoteAdapter = createRemoteAdapter({
      url: `${hostServer.url}/rpc`,
      secret: SECRET,
      name: "mock",
      userName: "mock-bot",
    });
    chat = new Chat({
      userName: "mock-bot",
      adapters: { mock: remoteAdapter },
      state: createMemoryState(),
    });

    await host.ready;
  });

  afterEach(async () => {
    await hostServer.close();
    await consumerServer.close();
  });

  it("delivers a real inbound HTTP round trip through to a handler, and a reply back over a real HTTP round trip", async () => {
    chat.onNewMention(async (thread) => {
      await thread.post("hello over a real socket");
    });

    const message = new Message({
      id: "m1",
      threadId: "mock:general:1",
      text: "hey @mock-bot",
      formatted: parseMarkdown("hey @mock-bot"),
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

    // Simulates the real adapter receiving a platform event; everything
    // downstream crosses the real sockets started above.
    await capturedChat.processMessage(mockAdapter, "mock:general:1", message);

    expect(posted).toHaveLength(1);
    expect(posted[0]!.threadId).toBe("mock:general:1");
  });

  it("rejects a real HTTP request with a bad signature over the real socket", async () => {
    const response = await fetch(`${hostServer.url}/rpc`, {
      method: "POST",
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "postMessage",
        params: ["t", "hi"],
      }),
      headers: {
        "x-chat-adapter-remote-signature": "sha256=bogus",
        "x-chat-adapter-remote-timestamp": String(Date.now()),
      },
    });
    expect(response.status).toBe(401);
    expect(mockAdapter.postMessage).not.toHaveBeenCalled();
  });
});
