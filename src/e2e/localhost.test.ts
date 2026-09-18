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

import { createRemoteAdapter, type RemoteAdapter } from "../adapter";
import { serveAdapter, type AdapterHost } from "../host";
import { startRealServer, type RealHttpServer } from "./http-bridge";

const SECRET = "e2e-secret";

/** The same round trip as bridge.test.ts, but over two real localhost servers. */
describe("e2e over real localhost HTTP", () => {
  let hostServer: RealHttpServer;
  let consumerServer: RealHttpServer;
  let adapter: Adapter;
  let hostChat: ChatInstance;
  let host: AdapterHost;
  let remote: RemoteAdapter;
  let chat: Chat;

  beforeEach(async () => {
    adapter = createMockAdapter("mock", {
      initialize: vi.fn(async (instance: ChatInstance) => {
        hostChat = instance;
      }),
    });

    consumerServer = await startRealServer((request) =>
      chat.webhooks.mock(request, {}),
    );
    host = serveAdapter(adapter, {
      secret: SECRET,
      consumerUrl: `${consumerServer.url}/inbound`,
    });
    hostServer = await startRealServer((request) =>
      host.handleRequest(request),
    );

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
  });

  afterEach(async () => {
    await hostServer.close();
    await consumerServer.close();
  });

  it("carries a message to a handler and the reply back to the adapter, across real sockets", async () => {
    let replied!: () => void;
    const reply = new Promise<void>((resolve) => (replied = resolve));
    chat.onNewMention(async (thread) => {
      await thread.post("hello over a real socket");
      replied();
    });

    await hostChat.processMessage(
      adapter,
      "mock:general:1",
      new Message({
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
      }),
    );

    await reply;
    expect(adapter.postMessage).toHaveBeenCalledWith(
      "mock:general:1",
      expect.anything(),
    );
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
