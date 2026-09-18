import { AdapterRateLimitError } from "@chat-adapter/shared";
import { createMockAdapter } from "@chat-adapter/tests";
import { Message } from "chat";
import { describe, expect, it, vi } from "vitest";

import { createRemoteAdapter } from "./adapter";
import { serializeMessage } from "./rpc/message-wire";
import { OPTIONAL_CAPABILITIES, type OptionalCapability } from "./rpc/methods";
import { sign } from "./rpc/signing";
import {
  CONSUMER_URL,
  HOST_URL,
  SECRET,
  bridge,
  deferred,
  handshake,
  message,
} from "./testing/bridge";

const THREAD = "mock:general:1";

describe("inbound messages", () => {
  it("carries a message, its reply chain and its dates to a handler", async () => {
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
        THREAD,
        message("@mock-bot look at this", { replyTo: message("the original") }),
      );

    const received = await delivered.promise;
    expect(received.text).toBe("@mock-bot look at this");
    expect(received.replyTo?.text).toBe("the original");
    expect(received.metadata.dateSent).toBeInstanceOf(Date);
  });

  it("accepts the lazy message factory form adapters use", async () => {
    const b = bridge();
    await b.host.ready;
    const delivered = deferred<Message>();
    b.chat.onNewMention(async (_thread, received) =>
      delivered.resolve(received),
    );

    await b
      .hostChat()
      .processMessage(b.adapter, THREAD, async () => message("@mock-bot late"));

    expect((await delivered.promise).text).toBe("@mock-bot late");
  });

  it("routes a DM without a literal mention", async () => {
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
      .processMessage(b.adapter, "mock:D1:1", message("no mention here"));

    expect((await delivered.promise).text).toBe("no mention here");
    expect(b.remote.isDM("mock:D1:1")).toBe(true);
  });
});

describe("outbound calls", () => {
  it("reaches the real adapter for every always-present method", async () => {
    const b = bridge();
    await b.host.ready;

    await b.remote.postMessage(THREAD, "hello");
    await b.remote.editMessage(THREAD, "m", "edited");
    await b.remote.deleteMessage(THREAD, "m");
    await b.remote.addReaction(THREAD, "m", "fire");
    await b.remote.removeReaction(THREAD, "m", "fire");
    await b.remote.fetchThread(THREAD);
    await b.remote.startTyping(THREAD, "on", { initiatorUserId: "u1" });

    expect(b.adapter.postMessage).toHaveBeenCalledWith(THREAD, "hello");
    expect(b.adapter.editMessage).toHaveBeenCalledWith(THREAD, "m", "edited");
    expect(b.adapter.deleteMessage).toHaveBeenCalledWith(THREAD, "m");
    expect(b.adapter.addReaction).toHaveBeenCalledWith(THREAD, "m", "fire");
    expect(b.adapter.removeReaction).toHaveBeenCalledWith(THREAD, "m", "fire");
    expect(b.adapter.startTyping).toHaveBeenCalledWith(THREAD, "on", {
      initiatorUserId: "u1",
    });
  });

  it("reaches the real adapter for every optional method it implements", async () => {
    const raw = { id: "r", threadId: THREAD, raw: {} };
    const b = bridge({
      reply: vi.fn(async () => raw),
      endTyping: vi.fn(async () => undefined),
      markAsRead: vi.fn(async () => undefined),
      getUser: vi.fn(async () => null),
      postObject: vi.fn(async () => raw),
      editObject: vi.fn(async () => raw),
      postEphemeral: vi.fn(async () => ({ ...raw, usedFallback: false })),
      fetchSubject: vi.fn(async () => null),
      onThreadSubscribe: vi.fn(async () => undefined),
      openModal: vi.fn(async () => ({ viewId: "v1" })),
      fetchChannelInfo: vi.fn(async () => ({ id: "c", metadata: {} })),
      fetchChannelMessages: vi.fn(async () => ({ messages: [message("top")] })),
      listThreads: vi.fn(async () => ({
        threads: [{ id: THREAD, rootMessage: message("root") }],
      })),
      fetchMessage: vi.fn(async () => message("found")),
    });
    await handshake(b);
    const sent = message("read me");

    const calls: Array<[OptionalCapability, unknown[]]> = [
      ["reply", [THREAD, "m1", "hi"]],
      ["endTyping", [THREAD, "closed"]],
      ["markAsRead", [THREAD, sent.id, sent]],
      ["getUser", ["u1"]],
      ["postObject", [THREAD, "plan", { steps: 1 }]],
      ["editObject", [THREAD, "m1", "plan", { steps: 2 }]],
      ["postEphemeral", [THREAD, "u1", "psst"]],
      ["fetchSubject", [{ kind: "ticket" }]],
      ["onThreadSubscribe", [THREAD]],
      ["openDM", ["u1"]],
      ["openModal", ["trigger", { type: "modal" }, "ctx"]],
      ["postChannelMessage", ["mock:general", "top level"]],
      ["fetchChannelInfo", ["mock:general"]],
      ["fetchChannelMessages", ["mock:general"]],
      ["listThreads", ["mock:general"]],
      ["fetchMessage", [THREAD, "m1"]],
      ["disconnect", []],
    ];

    for (const [name, args] of calls) {
      const method = b.remote[name] as (...a: unknown[]) => Promise<unknown>;
      await method.call(b.remote, ...args);
      expect(b.adapter[name]).toHaveBeenCalled();
    }

    // Anything carrying a Message arrives as a real instance, not wire JSON.
    expect(vi.mocked(b.adapter.markAsRead!).mock.calls[0]![2]).toBeInstanceOf(
      Message,
    );
    const threads = await b.remote.listThreads!("mock:general");
    expect(threads.threads[0]!.rootMessage).toBeInstanceOf(Message);
    expect(await b.remote.fetchMessage!(THREAD, "m1")).toBeInstanceOf(Message);
    expect(
      (await b.remote.fetchChannelMessages!("mock:general")).messages[0],
    ).toBeInstanceOf(Message);
  });

  it("returns live Message instances from fetchMessages", async () => {
    const b = bridge({
      fetchMessages: vi.fn(async () => ({
        messages: [message("from history")],
        nextCursor: "c1",
      })),
    });
    await b.host.ready;

    const result = await b.remote.fetchMessages(THREAD);

    expect(result.nextCursor).toBe("c1");
    expect(result.messages[0]).toBeInstanceOf(Message);
    expect(result.messages[0]!.metadata.dateSent).toBeInstanceOf(Date);
  });

  it("rebuilds the original error class on the consumer", async () => {
    const b = bridge({
      postMessage: vi
        .fn()
        .mockRejectedValue(new AdapterRateLimitError("mock", 42)),
    });
    await b.host.ready;

    await expect(b.remote.postMessage(THREAD, "hi")).rejects.toMatchObject({
      name: "AdapterRateLimitError",
      retryAfter: 42,
    });
  });

  it("throws from the synchronous members core never calls", async () => {
    const b = bridge();
    await b.host.ready;

    expect(() => b.remote.encodeThreadId()).toThrow(/synchronous/);
    expect(() => b.remote.decodeThreadId()).toThrow(/synchronous/);
    expect(() => b.remote.renderFormatted({} as never)).toThrow(/synchronous/);
    expect(() => b.remote.parseMessage()).toThrow(/synchronous/);
  });
});

describe("handshake", () => {
  it("adopts the wrapped adapter's identity and flags", async () => {
    const b = bridge({
      lockScope: "channel",
      persistThreadHistory: true,
      supportsTurnCancellation: true,
    });
    await handshake(b);

    expect(b.remote.userName).toBe(b.adapter.userName);
    expect(b.remote.lockScope).toBe("channel");
    expect(b.remote.persistThreadHistory).toBe(true);
    expect(b.remote.supportsTurnCancellation).toBe(true);
  });

  it("bridges exactly the optional members the wrapped adapter implements", async () => {
    const plain = createMockAdapter("mock") as unknown as Record<
      string,
      unknown
    >;
    const b = bridge({
      reply: vi.fn(async () => ({ id: "r", threadId: THREAD, raw: {} })),
    });
    await handshake(b);

    for (const name of OPTIONAL_CAPABILITIES) {
      const implemented = name === "reply" || typeof plain[name] === "function";
      expect({ name, bridged: typeof b.remote[name] === "function" }).toEqual({
        name,
        bridged: implemented,
      });
    }
  });

  it("refuses a host speaking a different protocol version", async () => {
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      fetch: async (_url, init) =>
        Response.json({
          jsonrpc: "2.0",
          id: JSON.parse(String(init?.body)).id,
          result: { protocolVersion: 99, name: "mock", userName: "mock-bot" },
        }),
    });

    await expect(remote.initialize({} as never)).rejects.toThrow(
      /protocol mismatch/,
    );
  });
});

describe("failure containment", () => {
  it("never rejects into the adapter's event loop when the consumer is unreachable", async () => {
    const onError = vi.fn();
    const { createRemoteChat } = await import("./host/remote-chat");
    const chat = createRemoteChat({
      consumerUrl: CONSUMER_URL,
      secret: SECRET,
      onError,
      fetch: async () => {
        throw new Error("connection refused");
      },
    });

    await expect(
      chat.processMessage(createMockAdapter("mock"), THREAD, message("hi")),
    ).resolves.toBeUndefined();
    expect(onError).toHaveBeenCalledWith(expect.any(Error), {
      phase: "forward",
      threadId: THREAD,
    });
  });

  it("no-ops unbridged ChatInstance members instead of throwing", async () => {
    const warn = vi.fn();
    const { createRemoteChat } = await import("./host/remote-chat");
    const chat = createRemoteChat({
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

    expect(() => chat.getState()).not.toThrow();
    expect(() => chat.processOptionsLoad({} as never)).not.toThrow();
    expect(warn).toHaveBeenCalled();
  });

  it("caps how many forwards are in flight at once", async () => {
    let inFlight = 0;
    let peak = 0;
    const { createRemoteChat } = await import("./host/remote-chat");
    const chat = createRemoteChat({
      consumerUrl: CONSUMER_URL,
      secret: SECRET,
      maxConcurrentForwards: 4,
      fetch: async (_url, init) => {
        inFlight++;
        peak = Math.max(peak, inFlight);
        await new Promise((resolve) => setTimeout(resolve, 5));
        inFlight--;
        return Response.json({
          jsonrpc: "2.0",
          id: JSON.parse(String(init?.body)).id,
          result: null,
        });
      },
    });
    const adapter = createMockAdapter("mock");

    await Promise.all(
      Array.from({ length: 20 }, (_, index) =>
        chat.processMessage(adapter, `mock:t${index}:1`, message("hi")),
      ),
    );

    expect(peak).toBe(4);
  });
});

describe("request verification", () => {
  function signed(
    body: string,
    secret = SECRET,
    timestamp = String(Date.now()),
  ) {
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

  it("rejects anything it cannot vouch for, without dispatching", async () => {
    const b = bridge();
    await b.host.ready;
    const cases = [
      [
        "a bad signature",
        signed(call("postMessage", ["t", "hi"]), "wrong"),
        401,
      ],
      [
        "a stale timestamp",
        signed(
          call("postMessage", ["t", "hi"]),
          SECRET,
          String(Date.now() - 60_000),
        ),
        401,
      ],
      [
        "an oversized body",
        signed(call("postMessage", ["t", "x".repeat(6_000_000)])),
        413,
      ],
    ] as const;

    for (const [, request, status] of cases) {
      expect((await b.host.handleRequest(request)).status).toBe(status);
    }
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });

  it("rejects a replayed request, and a method or arity outside the allowlist", async () => {
    const b = bridge();
    await b.host.ready;
    const body = call("postMessage", [THREAD, "hi"]);
    const timestamp = String(Date.now());
    const headers = {
      "x-chat-adapter-remote-signature": sign(body, timestamp, SECRET),
      "x-chat-adapter-remote-timestamp": timestamp,
    };
    const replay = () =>
      new Request(HOST_URL, { method: "POST", body, headers });

    expect((await b.host.handleRequest(replay())).ok).toBe(true);
    expect((await b.host.handleRequest(replay())).status).toBe(401);
    expect(b.adapter.postMessage).toHaveBeenCalledOnce();

    for (const bad of [call("__proto__", []), call("deleteMessage", ["t"])]) {
      const response = await b.host.handleRequest(signed(bad));
      expect((await response.json()).error.code).toBeDefined();
    }
    expect(b.adapter.deleteMessage).not.toHaveBeenCalled();
  });

  it("refuses an inbound event that arrives before the consumer initializes", async () => {
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      name: "mock",
      fetch: async () => new Response(null),
    });
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "processMessage",
      params: [
        THREAD,
        await serializeMessage(message("hi")),
        { channelId: "mock:general" },
      ],
    });
    const timestamp = String(Date.now());

    const response = await remote.handleWebhook(
      new Request(CONSUMER_URL, {
        method: "POST",
        body,
        headers: {
          "x-chat-adapter-remote-signature": sign(body, timestamp, SECRET),
          "x-chat-adapter-remote-timestamp": timestamp,
        },
      }),
    );

    expect(response.status).toBe(503);
  });
});
