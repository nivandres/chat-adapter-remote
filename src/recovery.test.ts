import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import { Chat, type ChatInstance, type StateAdapter } from "chat";
import { describe, expect, it, vi } from "vitest";

import { createRemoteAdapter } from "./adapter";
import { createStateForwardQueue, serveAdapter } from "./host";
import { CallLedger } from "./host/call-ledger";
import { forwardEntry } from "./host/delivery";
import { RpcErrorCode } from "./rpc/errors";
import {
  CONSUMER_URL,
  HOST_URL,
  SECRET,
  deferred,
  message,
} from "./testing/bridge";
import type { FetchLike, RemoteAdapterConfig } from "./types";

const THREAD = "mock:general:1";

/** A host that can be switched off and on, reached over the real request path. */
function flakyHost() {
  const adapter = createMockAdapter("mock");
  const host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch: vi.fn(),
  });
  const link = { up: false };
  const fetch: FetchLike = async (input, init) => {
    if (!link.up) throw new TypeError("fetch failed");
    return host.handleRequest(new Request(input, init));
  };
  return { adapter, host, link, fetch };
}

describe("recovery", () => {
  it("is not left broken by a host that was down when it started", async () => {
    const { adapter, host, link, fetch } = flakyHost();
    await host.ready;
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      name: "mock",
      fetch,
    });
    const chat = new Chat({
      userName: "bot",
      adapters: { mock: remote },
      state: createMemoryState(),
    });

    // Chat keeps the promise from its first init forever; it must not be a rejection.
    await expect(chat.initialize()).resolves.toBeUndefined();
    await expect(remote.postMessage(THREAD, "hi")).rejects.toMatchObject({
      code: RpcErrorCode.UNAVAILABLE,
    });

    link.up = true;

    await remote.postMessage(THREAD, "back");
    expect(adapter.postMessage).toHaveBeenCalledWith(THREAD, "back");
  });

  it("still fails loudly on a wrong secret, which no retry would fix", async () => {
    const { host, link, fetch } = flakyHost();
    await host.ready;
    link.up = true;
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: "not-the-secret",
      name: "mock",
      fetch,
    });

    await expect(remote.initialize({} as never)).rejects.toMatchObject({
      code: RpcErrorCode.UNAUTHORIZED,
    });
  });

  it("declines to stream while no host has answered, leaving the iterable unread", async () => {
    const { host, fetch } = flakyHost();
    await host.ready;
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      name: "mock",
      fetch,
    });
    let pulled = 0;

    const result = await remote.stream(THREAD, {
      async *[Symbol.asyncIterator]() {
        pulled++;
        yield "never read";
      },
    });

    expect(result).toBeNull();
    expect(pulled).toBe(0);
  });

  it("tells a timeout apart from a call that never arrived", async () => {
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      name: "mock",
      timeoutMs: 20,
      fetch: (_input, init) =>
        new Promise((_resolve, reject) =>
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("aborted", "AbortError")),
          ),
        ),
    });

    await expect(remote.postMessage(THREAD, "slow")).rejects.toMatchObject({
      code: RpcErrorCode.TIMEOUT,
    });
  });
});

/** A consumer the host forwards to, that can be down, slow, or refuse. */
function consumer(mode: { value: "up" | "down" | "slow" | "refuse" }) {
  const received: string[] = [];
  const fetch: FetchLike = async (_input, init) => {
    if (mode.value === "down") throw new TypeError("fetch failed");
    const body = JSON.parse(String(init?.body));
    if (mode.value === "slow") {
      return new Promise((_resolve, reject) =>
        init?.signal?.addEventListener("abort", () =>
          reject(new DOMException("aborted", "AbortError")),
        ),
      );
    }
    if (mode.value === "refuse") {
      return Response.json(
        {
          jsonrpc: "2.0",
          id: body.id,
          error: { code: RpcErrorCode.INVALID_PARAMS, message: "no" },
        },
        { status: 400 },
      );
    }
    received.push(body.params[1]?.text ?? body.method);
    return Response.json({ jsonrpc: "2.0", id: body.id, result: null });
  };
  return { received, fetch };
}

async function hostWith(
  mode: { value: "up" | "down" | "slow" | "refuse" },
  options: Record<string, unknown> = {},
) {
  const { received, fetch } = consumer(mode);
  let chat!: ChatInstance;
  const adapter = createMockAdapter("mock", {
    initialize: vi.fn(async (instance: ChatInstance) => void (chat = instance)),
  });
  const onDropped = vi.fn();
  const host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch,
    timeoutMs: 20,
    onDropped,
    ...options,
  });
  await host.ready;
  const redelivery = Reflect.get(host, "redelivery") as {
    drain(): Promise<void>;
  };
  return { adapter, host, chat: () => chat, received, onDropped, redelivery };
}

describe("redelivery", () => {
  it("delivers what arrived while the consumer was down once it is back", async () => {
    const mode = { value: "down" as "up" | "down" | "slow" | "refuse" };
    const h = await hostWith(mode, { forwardRetry: { intervalMs: 0 } });

    for (const text of ["one", "two", "three"]) {
      await h.chat().processMessage(h.adapter, THREAD, message(text));
    }
    expect(h.received).toEqual([]);

    mode.value = "up";
    await h.redelivery.drain();

    expect(h.received).toEqual(["one", "two", "three"]);
    expect(h.onDropped).not.toHaveBeenCalled();
  });

  it("never sends a timed-out forward twice, since it may have been handled", async () => {
    const mode = { value: "slow" as "up" | "down" | "slow" | "refuse" };
    const h = await hostWith(mode, { forwardRetry: { intervalMs: 0 } });

    await h.chat().processMessage(h.adapter, THREAD, message("maybe handled"));
    mode.value = "up";
    await h.redelivery.drain();

    expect(h.received).toEqual([]);
    expect(h.onDropped).not.toHaveBeenCalled();
  });

  it("drops at once what the consumer refused", async () => {
    const mode = { value: "refuse" as "up" | "down" | "slow" | "refuse" };
    const h = await hostWith(mode);

    await h.chat().processMessage(h.adapter, THREAD, message("bad"));

    expect(h.onDropped).toHaveBeenCalledWith(
      expect.objectContaining({ method: "processMessage", attempts: 1 }),
      "rejected",
      expect.anything(),
    );
  });

  it("gives up after the retention window", async () => {
    const mode = { value: "down" as "up" | "down" | "slow" | "refuse" };
    const h = await hostWith(mode, {
      forwardRetry: { intervalMs: 0, retentionMs: 0 },
    });

    await h.chat().processMessage(h.adapter, THREAD, message("too late"));
    await h.redelivery.drain();

    expect(h.onDropped).toHaveBeenCalledWith(
      expect.objectContaining({ method: "processMessage" }),
      "expired",
      expect.anything(),
    );
  });

  it("gives up after maxAttempts, whatever the retention", async () => {
    const mode = { value: "down" as "up" | "down" | "slow" | "refuse" };
    const h = await hostWith(mode, {
      forwardRetry: { intervalMs: 0, maxAttempts: 2 },
    });

    await h.chat().processMessage(h.adapter, THREAD, message("twice at most"));
    await h.redelivery.drain();

    expect(h.onDropped).toHaveBeenCalledWith(
      expect.objectContaining({ attempts: 2 }),
      "expired",
      expect.anything(),
    );
  });

  it("waits longer between attempts with a backoff, up to its ceiling", async () => {
    const { Redelivery, createMemoryForwardQueue, forwardEntry } =
      await import("./host/delivery");
    const queue = createMemoryForwardQueue();
    const redelivery = new Redelivery({
      queue,
      intervalMs: 1000,
      backoff: 2,
      maxIntervalMs: 3000,
      send: async () => {
        throw new (await import("./rpc/errors")).RemoteAdapterRpcError(
          RpcErrorCode.UNAVAILABLE,
          "down",
        );
      },
      isUndelivered: () => true,
      onDropped: () => {},
    });
    const entry = forwardEntry("processMessage", [], THREAD);
    await redelivery.keep({ ...entry, firstAttemptAt: 0, attempts: 3 });

    const [kept] = await queue.takeDue(Number.POSITIVE_INFINITY);
    // 1000 * 2^2 = 4000, held to the 3000 ceiling.
    expect(kept!.nextAttemptAt).toBe(3000);
  });

  it("drops one the consumer refuses once back, and still delivers the rest", async () => {
    const { Redelivery, createMemoryForwardQueue, forwardEntry } =
      await import("./host/delivery");
    const delivered: string[] = [];
    const onDropped = vi.fn();
    const redelivery = new Redelivery({
      queue: createMemoryForwardQueue(),
      intervalMs: 0,
      send: async (_method, params) => {
        const [text] = params as [string];
        if (text === "refused") throw new Error("invalid params");
        delivered.push(text);
      },
      isUndelivered: () => false,
      onDropped,
    });
    await redelivery.keep(forwardEntry("processMessage", ["refused"], THREAD));
    await redelivery.keep(forwardEntry("processMessage", ["kept"], THREAD));

    await redelivery.drain();

    expect(onDropped).toHaveBeenCalledWith(
      expect.objectContaining({ params: ["refused"] }),
      "rejected",
      expect.any(Error),
    );
    expect(delivered).toEqual(["kept"]);
  });

  it("drops the oldest entry once the memory queue is full", async () => {
    const { Redelivery, createMemoryForwardQueue, forwardEntry } =
      await import("./host/delivery");
    const overflowed: unknown[] = [];
    const queue = createMemoryForwardQueue(2);
    const redelivery = new Redelivery({
      queue,
      send: async () => undefined,
      isUndelivered: () => true,
      onDropped: (entry, reason) =>
        void overflowed.push([entry.threadId, reason]),
    });

    for (const threadId of ["first", "second", "third"]) {
      await redelivery.keep(forwardEntry("processMessage", [], threadId));
    }

    expect(overflowed).toEqual([["first", "overflow"]]);
    expect(
      (await queue.takeDue(Number.POSITIVE_INFINITY)).map((e) => e.threadId),
    ).toEqual(["second", "third"]);
  });

  it("retries on its own timer until stopped", async () => {
    vi.useFakeTimers();
    try {
      const { Redelivery, createMemoryForwardQueue, forwardEntry } =
        await import("./host/delivery");
      const send = vi.fn(async () => undefined);
      const redelivery = new Redelivery({
        queue: createMemoryForwardQueue(),
        intervalMs: 1000,
        send,
        isUndelivered: () => true,
        onDropped: () => {},
      });
      await redelivery.keep(forwardEntry("processMessage", [], THREAD));

      redelivery.start();
      await vi.advanceTimersByTimeAsync(1000);
      expect(send).toHaveBeenCalledOnce();

      redelivery.stop();
      await redelivery.keep(forwardEntry("processMessage", [], THREAD));
      await vi.advanceTimersByTimeAsync(5000);
      expect(send).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });

  it("reports what only this process held once it stops", async () => {
    const mode = { value: "down" as "up" | "down" | "slow" | "refuse" };
    const h = await hostWith(mode, { forwardRetry: { intervalMs: 60_000 } });
    await h.chat().processMessage(h.adapter, THREAD, message("stranded"));

    await h.host.stop();

    expect(h.onDropped).toHaveBeenCalledWith(
      expect.objectContaining({ method: "processMessage" }),
      "stopped",
      undefined,
    );
  });

  it("leaves a shared queue alone on stop, for the next host to retry", async () => {
    const takeDue = vi.fn(async () => []);
    const mode = { value: "down" as "up" | "down" | "slow" | "refuse" };
    const h = await hostWith(mode, {
      forwardQueue: { push: async () => {}, takeDue },
    });

    await h.host.stop();

    expect(takeDue).not.toHaveBeenCalled();
    expect(h.onDropped).not.toHaveBeenCalled();
  });

  it("holds no bytes the adapter can fetch again while the consumer is down", async () => {
    const queued: unknown[] = [];
    let chat!: ChatInstance;
    const adapter = createMockAdapter("mock", {
      initialize: vi.fn(
        async (instance: ChatInstance) => void (chat = instance),
      ),
      rehydrateAttachment: vi.fn((attachment) => attachment),
    });
    const host = serveAdapter(adapter, {
      secret: SECRET,
      consumerUrl: CONSUMER_URL,
      fetch: vi.fn().mockRejectedValue(new TypeError("fetch failed")),
      forwardQueue: {
        push: async (entry) => void queued.push(entry),
        takeDue: async () => [],
      },
    });
    await host.ready;

    await chat.processMessage(
      adapter,
      THREAD,
      message("@mock-bot photo", {
        attachments: [
          {
            type: "image",
            name: "photo.jpg",
            size: 1_000_000,
            fetchData: async () => Buffer.alloc(1_000_000, 1),
            fetchMetadata: { messageId: "p1" },
          },
        ],
      }),
    );

    expect(queued).toHaveLength(1);
    expect(JSON.stringify(queued[0]).length).toBeLessThan(100_000);
  });

  it("keeps entries in a queue of your own", async () => {
    const mode = { value: "down" as "up" | "down" | "slow" | "refuse" };
    const pushed: unknown[] = [];
    await (
      await hostWith(mode, {
        forwardQueue: {
          push: async (entry: unknown) => void pushed.push(entry),
          takeDue: async () => [],
        },
      })
    )
      .chat()
      .processMessage(createMockAdapter("mock"), THREAD, message("kept"));

    expect(pushed).toHaveLength(1);
    // JSON-safe, so a shared store can hold it.
    expect(() => JSON.stringify(pushed[0])).not.toThrow();
  });
});

/** A host whose forwards wait in `state`, which outlives it the way Redis would. */
async function durableHost(
  state: StateAdapter,
  fetch: FetchLike,
  options: Record<string, unknown> = {},
) {
  let chat!: ChatInstance;
  const adapter = createMockAdapter("mock", {
    initialize: vi.fn(async (instance: ChatInstance) => void (chat = instance)),
  });
  const queue = createStateForwardQueue(state);
  const onDropped = vi.fn();
  const host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch,
    forwardQueue: queue,
    forwardRetry: { intervalMs: 0 },
    onDropped,
    ...options,
  });
  await host.ready;
  const redelivery = Reflect.get(host, "redelivery") as {
    drain(): Promise<void>;
  };
  return { adapter, host, chat: () => chat, queue, onDropped, redelivery };
}

describe("durable forwards", () => {
  it("hands what one host kept to the next one sharing its store", async () => {
    const state = createMemoryState();
    const down = { value: "down" as "up" | "down" | "slow" | "refuse" };
    const first = await durableHost(state, consumer(down).fetch);
    await first.chat().processMessage(first.adapter, THREAD, message("kept"));
    await first.host.stop();
    expect(first.onDropped).not.toHaveBeenCalled();

    const up = consumer({ value: "up" });
    const next = await durableHost(state, up.fetch);
    await next.redelivery.drain();

    expect(up.received).toEqual(["kept"]);
    expect(await next.queue.size!()).toBe(0);
  });

  it("journals a forward until it is acknowledged, so a crash mid-send loses nothing", async () => {
    const state = createMemoryState();
    const answer = deferred<Response>();
    const first = await durableHost(state, () => answer.promise, {
      journalForwards: true,
      timeoutMs: 10_000,
    });
    void first.chat().processMessage(first.adapter, THREAD, message("mid"));
    await vi.waitFor(async () => expect(await first.queue.size!()).toBe(1));
    await first.redelivery.drain();
    expect(await first.queue.size!()).toBe(1);
    await first.host.stop();

    const up = consumer({ value: "up" });
    const next = await durableHost(state, up.fetch);
    await next.redelivery.drain();

    expect(up.received).toEqual(["mid"]);
  });

  it("forgets a journaled forward once acknowledged", async () => {
    const up = consumer({ value: "up" });
    const h = await durableHost(createMemoryState(), up.fetch, {
      journalForwards: true,
    });

    await h.chat().processMessage(h.adapter, THREAD, message("done"));

    expect(up.received).toEqual(["done"]);
    expect(await h.queue.size!()).toBe(0);
  });

  it("drops a refused forward once, leaving nothing journaled", async () => {
    const h = await durableHost(
      createMemoryState(),
      consumer({ value: "refuse" }).fetch,
      { journalForwards: true },
    );

    await h.chat().processMessage(h.adapter, THREAD, message("bad"));

    expect(h.onDropped).toHaveBeenCalledOnce();
    expect(h.onDropped).toHaveBeenCalledWith(
      expect.anything(),
      "rejected",
      expect.anything(),
    );
    expect(await h.queue.size!()).toBe(0);
  });

  it("keeps a forward journaled once while the consumer is down", async () => {
    const h = await durableHost(
      createMemoryState(),
      consumer({ value: "down" }).fetch,
      { journalForwards: true },
    );

    await h.chat().processMessage(h.adapter, THREAD, message("once"));

    expect(await h.queue.size!()).toBe(1);
  });

  it("still forwards when the store fails", async () => {
    const up = consumer({ value: "up" });
    const { host, chat, adapter } = await hostWith(
      { value: "up" },
      {
        fetch: up.fetch,
        journalForwards: true,
        forwardQueue: {
          push: async () => {
            throw new Error("redis down");
          },
          takeDue: async () => [],
          remove: async () => {},
        },
      },
    );

    await chat().processMessage(adapter, THREAD, message("anyway"));

    expect(up.received).toEqual(["anyway"]);
    await host.stop();
  });

  it("evicts the oldest past its capacity, as overflow", async () => {
    const queue = createStateForwardQueue(createMemoryState(), {
      maxEntries: 2,
    });
    const evicted: unknown[] = [];
    for (const threadId of ["first", "second", "third"]) {
      const out = await queue.push(
        forwardEntry("processMessage", [], threadId),
      );
      evicted.push(...(out ?? []).map((entry) => entry.threadId));
    }

    expect(evicted).toEqual(["first"]);
    expect(
      (await queue.takeDue(Number.POSITIVE_INFINITY)).map((e) => e.threadId),
    ).toEqual(["second", "third"]);
  });

  it("keeps hosts sharing a store apart by key", async () => {
    const state = createMemoryState();
    const one = createStateForwardQueue(state, { key: "tenant-1" });
    const two = createStateForwardQueue(state, { key: "tenant-2" });

    await one.push(forwardEntry("processMessage", [], THREAD));

    expect(await two.takeDue(Number.POSITIVE_INFINITY)).toEqual([]);
    expect(await one.takeDue(Number.POSITIVE_INFINITY)).toHaveLength(1);
  });

  it("refuses journalForwards without a queue that can forget an entry", () => {
    expect(() =>
      serveAdapter(createMockAdapter("mock"), {
        secret: SECRET,
        consumerUrl: CONSUMER_URL,
        fetch: vi.fn(),
        journalForwards: true,
      }),
    ).toThrow(/journalForwards/);
  });
});

/** Reaches the host the way a network does: an abort fails the caller while the host carries on. */
function slowHost(delayMs: number) {
  const adapter = createMockAdapter("mock", {
    postMessage: vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      return { id: "posted", threadId: THREAD, raw: {} };
    }),
  });
  const host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch: vi.fn(),
  });
  const link = { unreachable: 0, sent: 0, idempotent: true };
  const fetch: FetchLike = async (input, init) => {
    link.sent++;
    if (link.unreachable > 0) {
      link.unreachable--;
      throw new TypeError("fetch failed");
    }
    const answered = new Promise<Response>((resolve, reject) => {
      init?.signal?.addEventListener("abort", () =>
        reject(new DOMException("aborted", "AbortError")),
      );
      host.handleRequest(new Request(input, init)).then(resolve, reject);
    });
    if (link.idempotent || !String(init?.body).includes("__handshake")) {
      return answered;
    }
    // A host from before 0.8.3, which never announced it.
    const body = await (await answered).json();
    delete body.result.idempotentCalls;
    return Response.json(body);
  };
  const consumer = (options: Partial<RemoteAdapterConfig> = {}) =>
    createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      name: "mock",
      fetch,
      timeoutMs: 30,
      retry: { intervalMs: 0, maxAttempts: 5 },
      ...options,
    });
  return { adapter, link, consumer };
}

describe("retries", () => {
  it("answers a call that outlived its timeout, running it once however often it is retried", async () => {
    const h = slowHost(80);

    await expect(
      h.consumer().postMessage(THREAD, "slow"),
    ).resolves.toMatchObject({ id: "posted" });
    // The handshake, then more than one try at the call.
    expect(h.link.sent).toBeGreaterThan(2);
    expect(h.adapter.postMessage).toHaveBeenCalledOnce();
  });

  it("sends again a call that never arrived", async () => {
    const h = slowHost(0);
    const remote = h.consumer();
    await remote.postMessage(THREAD, "first");

    h.link.unreachable = 1;
    await expect(remote.postMessage(THREAD, "second")).resolves.toBeDefined();
    expect(h.adapter.postMessage).toHaveBeenCalledTimes(2);
  });

  it("does not retry against a host that would run the call twice", async () => {
    const h = slowHost(80);
    h.link.idempotent = false;

    await expect(
      h.consumer().postMessage(THREAD, "slow"),
    ).rejects.toMatchObject({ code: RpcErrorCode.TIMEOUT });
    expect(h.link.sent).toBe(2);
  });

  it("does not retry when told not to", async () => {
    const h = slowHost(80);

    await expect(
      h.consumer({ retry: false }).postMessage(THREAD, "slow"),
    ).rejects.toMatchObject({ code: RpcErrorCode.TIMEOUT });
    expect(h.link.sent).toBe(2);
  });

  it("never retries an answer, only the lack of one", async () => {
    const h = slowHost(0);
    vi.mocked(h.adapter.postMessage).mockRejectedValue(new Error("rejected"));

    await expect(h.consumer().postMessage(THREAD, "x")).rejects.toThrow();
    expect(h.adapter.postMessage).toHaveBeenCalledOnce();
  });
});

describe("call ledger", () => {
  it("runs a call once per id, and every time for reads and older consumers", async () => {
    const ledger = new CallLedger();
    const call = vi.fn(async () => "done");

    await ledger.run("a", "postMessage", call);
    await ledger.run("a", "postMessage", call);
    expect(call).toHaveBeenCalledTimes(1);

    await ledger.run(1, "postMessage", call);
    await ledger.run(1, "postMessage", call);
    await ledger.run("b", "fetchMessages", call);
    await ledger.run("b", "fetchMessages", call);
    expect(call).toHaveBeenCalledTimes(5);
  });

  it("gives a retry the first outcome, failures included", async () => {
    const ledger = new CallLedger();
    const call = vi.fn(async () => {
      throw new Error("rate limited");
    });

    await expect(ledger.run("a", "postMessage", call)).rejects.toThrow();
    await expect(ledger.run("a", "postMessage", call)).rejects.toThrow();
    expect(call).toHaveBeenCalledOnce();
  });

  it("forgets a finished call once its window has passed", async () => {
    vi.useFakeTimers();
    try {
      const ledger = new CallLedger();
      const call = vi.fn(async () => "done");
      await ledger.run("a", "postMessage", call);

      vi.advanceTimersByTime(130_000);
      await ledger.run("a", "postMessage", call);

      expect(call).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });
});
