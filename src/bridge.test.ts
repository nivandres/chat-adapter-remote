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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createRemoteAdapter, type RemoteAdapter } from "./adapter";
import { AdapterHost, serveAdapter } from "./host";
import { createRemoteChat } from "./host/remote-chat";
import type { FetchLike } from "./types";

const SECRET = "test-secret";
const HOST_URL = "https://host.test/rpc";
const CONSUMER_URL = "https://consumer.test/inbound";

function message(
  text: string,
  overrides: Partial<ConstructorParameters<typeof Message>[0]> = {},
): Message {
  return new Message({
    id: `m-${Math.random()}`,
    threadId: "mock:general:1",
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
    metadata: { dateSent: new Date("2024-01-01T00:00:00.000Z"), edited: false },
    attachments: [],
    ...overrides,
  });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  return { promise: new Promise<T>((r) => (resolve = r)), resolve };
}

interface Bridge {
  host: AdapterHost;
  remote: RemoteAdapter;
  chat: Chat;
  adapter: Adapter;
  hostChat: () => ChatInstance;
}

function bridge(overrides: Partial<Adapter> = {}): Bridge {
  let captured: ChatInstance | undefined;
  const adapter = createMockAdapter("mock", {
    initialize: vi.fn(async (chatInstance: ChatInstance) => {
      captured = chatInstance;
    }),
    ...overrides,
  });

  let host: AdapterHost;
  let chat: Chat;
  const loopback: FetchLike = async (input, init) => {
    const request = new Request(input, init);
    return new URL(request.url).hostname === "host.test"
      ? host.handleRequest(request)
      : chat.webhooks.mock(request, {});
  };

  host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch: loopback,
  });
  const remote = createRemoteAdapter({
    url: HOST_URL,
    secret: SECRET,
    name: "mock",
    fetch: loopback,
  });
  chat = new Chat({
    userName: "mock-bot",
    adapters: { mock: remote },
    state: createMemoryState(),
  });

  return {
    host,
    remote,
    chat,
    adapter,
    hostChat: () => {
      if (!captured) throw new Error("adapter was not initialized");
      return captured;
    },
  };
}

describe("inbound delivery", () => {
  it("delivers a message that has a replyTo", async () => {
    const b = bridge();
    await b.host.ready;
    const delivered = deferred<Message>();
    b.chat.onNewMention(async (_thread, received) =>
      delivered.resolve(received),
    );

    await b
      .hostChat()
      .processMessage(
        b.adapter,
        "mock:general:1",
        message("@mock-bot look at this", { replyTo: message("the original") }),
      );

    const received = await delivered.promise;
    expect(received.text).toBe("@mock-bot look at this");
    expect(received.replyTo?.text).toBe("the original");
  });

  it("delivers a message without a replyTo", async () => {
    const b = bridge();
    await b.host.ready;
    const delivered = deferred<Message>();
    b.chat.onNewMention(async (_thread, received) =>
      delivered.resolve(received),
    );

    await b
      .hostChat()
      .processMessage(b.adapter, "mock:general:1", message("@mock-bot plain"));

    expect((await delivered.promise).text).toBe("@mock-bot plain");
  });

  it("rejects a malformed inbound call loudly instead of acknowledging it", async () => {
    const b = bridge();
    await b.host.ready;
    const { sign } = await import("./rpc/signing");
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "processMessage",
      params: ["t"],
    });
    const timestamp = String(Date.now());

    const response = await b.remote.handleWebhook(
      new Request(CONSUMER_URL, {
        method: "POST",
        body,
        headers: {
          "x-chat-adapter-remote-signature": sign(body, timestamp, SECRET),
          "x-chat-adapter-remote-timestamp": timestamp,
        },
      }),
    );

    expect(response.status).toBe(400);
  });

  it("marks a DM thread so mention-routing works without a literal @mention", async () => {
    const b = bridge({
      isDM: vi.fn((threadId: string) => threadId.includes(":D")),
    });
    await b.host.ready;
    const delivered = deferred<Message>();
    b.chat.onNewMention(async (_thread, received) =>
      delivered.resolve(received),
    );

    await b
      .hostChat()
      .processMessage(
        b.adapter,
        "mock:D1:1",
        message("hi", { threadId: "mock:D1:1" }),
      );

    expect((await delivered.promise).text).toBe("hi");
    expect(b.remote.isDM("mock:D1:1")).toBe(true);
  });
});

describe("outbound dispatch", () => {
  it("reaches the real adapter for every bridged method", async () => {
    const b = bridge();
    await b.host.ready;

    await b.remote.postMessage("t", "hello");
    await b.remote.editMessage("t", "m", "edited");
    await b.remote.deleteMessage("t", "m");
    await b.remote.addReaction("t", "m", "fire");
    await b.remote.removeReaction("t", "m", "fire");
    await b.remote.fetchThread("t");
    await b.remote.startTyping("t", "on", { initiatorUserId: "u1" });
    await b.remote.disconnect();

    expect(b.adapter.postMessage).toHaveBeenCalledWith("t", "hello");
    expect(b.adapter.editMessage).toHaveBeenCalledWith("t", "m", "edited");
    expect(b.adapter.deleteMessage).toHaveBeenCalledWith("t", "m");
    expect(b.adapter.addReaction).toHaveBeenCalledWith("t", "m", "fire");
    expect(b.adapter.removeReaction).toHaveBeenCalledWith("t", "m", "fire");
    expect(b.adapter.fetchThread).toHaveBeenCalledWith("t");
    expect(b.adapter.startTyping).toHaveBeenCalledWith("t", "on", {
      initiatorUserId: "u1",
    });
    expect(b.adapter.disconnect).toHaveBeenCalled();
  });

  it("returns live Message instances from fetchMessages with dates intact", async () => {
    const b = bridge({
      fetchMessages: vi.fn().mockResolvedValue({
        messages: [message("from history")],
        nextCursor: "next",
      }),
    });
    await b.host.ready;

    const result = await b.remote.fetchMessages("mock:general:1");

    expect(result.nextCursor).toBe("next");
    expect(result.messages[0]!.text).toBe("from history");
    expect(result.messages[0]!.metadata.dateSent).toBeInstanceOf(Date);
    expect(Number.isNaN(result.messages[0]!.metadata.dateSent.getTime())).toBe(
      false,
    );
  });

  it("reconstructs the original error class across the boundary", async () => {
    const b = bridge({
      postMessage: vi
        .fn()
        .mockRejectedValue(new AdapterRateLimitError("mock", 42)),
    });
    await b.host.ready;

    await expect(b.remote.postMessage("t", "hi")).rejects.toMatchObject({
      name: "AdapterRateLimitError",
      retryAfter: 42,
    });
  });

  it("adopts capability flags from the handshake", async () => {
    const b = bridge({
      lockScope: "channel",
      persistThreadHistory: true,
      supportsTurnCancellation: true,
    });
    await b.host.ready;
    await b.chat.webhooks.mock(new Request(CONSUMER_URL, { method: "POST" }));

    expect(b.remote.lockScope).toBe("channel");
    expect(b.remote.persistThreadHistory).toBe(true);
    expect(b.remote.supportsTurnCancellation).toBe(true);
  });
});

describe("failure containment", () => {
  const unhandled: unknown[] = [];
  const record = (reason: unknown) => void unhandled.push(reason);

  beforeEach(() => {
    unhandled.length = 0;
    process.on("unhandledRejection", record);
  });
  afterEach(() => {
    process.off("unhandledRejection", record);
  });

  it("never rejects when the consumer is unreachable, since adapters call it unawaited", async () => {
    const hostChat = createRemoteChat({
      consumerUrl: CONSUMER_URL,
      secret: SECRET,
      logger: {
        debug: vi.fn(),
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        child: vi.fn().mockReturnThis(),
      },
      fetch: vi
        .fn()
        .mockRejectedValue(
          new Error("consumer down"),
        ) as unknown as typeof fetch,
    });

    await expect(
      hostChat.processMessage(
        createMockAdapter("mock"),
        "mock:general:1",
        message("hi"),
      ),
    ).resolves.toBeUndefined();
  });

  it("does not produce an unhandled rejection when the adapter fails to initialize", async () => {
    const host = serveAdapter(
      createMockAdapter("mock", {
        initialize: vi.fn().mockRejectedValue(new Error("auth expired")),
      }),
      { secret: SECRET, consumerUrl: CONSUMER_URL, fetch: vi.fn() },
    );

    await expect(host.ready).rejects.toThrow("auth expired");
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(unhandled).toEqual([]);
  });

  it("ignores unbridged ChatInstance members instead of throwing into the adapter event loop", async () => {
    const warn = vi.fn();
    const hostChat = createRemoteChat({
      consumerUrl: CONSUMER_URL,
      secret: SECRET,
      logger: {
        debug: vi.fn(),
        info: vi.fn(),
        warn,
        error: vi.fn(),
        child: vi.fn().mockReturnThis(),
      },
      fetch: vi.fn(),
    });

    expect(() =>
      hostChat.processReaction({} as never, undefined),
    ).not.toThrow();
    expect(() => hostChat.processAction({} as never, undefined)).not.toThrow();
    expect(warn).toHaveBeenCalled();
  });
});

describe("protocol", () => {
  it("refuses to initialize against a host speaking a different protocol version", async () => {
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      fetch: (async (_input: RequestInfo | URL, init?: RequestInit) =>
        Response.json({
          jsonrpc: "2.0",
          id: JSON.parse(String(init?.body)).id,
          result: { protocolVersion: 99, name: "mock", userName: "mock-bot" },
        })) as unknown as typeof fetch,
    });

    await expect(remote.initialize({} as never)).rejects.toThrow(
      /protocol mismatch/,
    );
  });
});

describe("request verification", () => {
  async function signed(
    body: string,
    secret = SECRET,
    timestamp = String(Date.now()),
  ) {
    const { sign } = await import("./rpc/signing");
    return new Request(HOST_URL, {
      method: "POST",
      body,
      headers: {
        "x-chat-adapter-remote-signature": sign(body, timestamp, secret),
        "x-chat-adapter-remote-timestamp": timestamp,
      },
    });
  }

  const call = (method: string, params: unknown[]) =>
    JSON.stringify({ jsonrpc: "2.0", id: 1, method, params });

  it("rejects a bad signature without dispatching", async () => {
    const b = bridge();
    await b.host.ready;
    const response = await b.host.handleRequest(
      await signed(call("postMessage", ["t", "hi"]), "wrong"),
    );
    expect(response.status).toBe(401);
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });

  it("rejects a stale timestamp without dispatching", async () => {
    const b = bridge();
    await b.host.ready;
    const stale = String(Date.now() - 60_000);
    const response = await b.host.handleRequest(
      await signed(call("postMessage", ["t", "hi"]), SECRET, stale),
    );
    expect(response.status).toBe(401);
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });

  it("rejects a method outside the allowlist", async () => {
    const b = bridge();
    await b.host.ready;
    const response = await b.host.handleRequest(
      await signed(call("__proto__", [])),
    );
    expect((await response.json()).error.code).toBeDefined();
  });

  it("rejects wrong arity without dispatching", async () => {
    const b = bridge();
    await b.host.ready;
    const response = await b.host.handleRequest(
      await signed(call("deleteMessage", ["t"])),
    );
    expect((await response.json()).error.code).toBeDefined();
    expect(b.adapter.deleteMessage).not.toHaveBeenCalled();
  });

  it("rejects an oversized body without buffering it", async () => {
    const b = bridge();
    await b.host.ready;
    const response = await b.host.handleRequest(
      await signed(call("postMessage", ["t", "x".repeat(6_000_000)])),
    );
    expect(response.status).toBe(413);
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });
});
