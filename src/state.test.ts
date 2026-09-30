import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import { Chat, type ChatInstance } from "chat";
import { describe, expect, it, vi } from "vitest";

import { createRemoteAdapter } from "./adapter";
import { serveAdapter } from "./host";
import {
  CONSUMER_URL,
  HOST_URL,
  SECRET,
  bridge,
  handshake,
  message,
} from "./testing/bridge";
import type { FetchLike } from "./types";

describe("adapter state", () => {
  it("reaches the consumer's store, the way an adapter persists its own data", async () => {
    const b = bridge();
    await handshake(b);
    const state = b.hostChat().getState();

    await state.set("poll:1", { secret: "abc" });

    expect(await state.get("poll:1")).toEqual({ secret: "abc" });
    // Sandboxed: it landed under the adapter's own prefix, not the bare key.
    expect(await b.chat.getState().get("poll:1")).toBeNull();
    expect(await b.chat.getState().get("adapter:mock:poll:1")).toEqual({
      secret: "abc",
    });
  });

  it("carries the values the codec knows about", async () => {
    const b = bridge();
    await handshake(b);
    const state = b.hostChat().getState();

    await state.set("k", {
      at: new Date("2030-01-01T00:00:00.000Z"),
      raw: Buffer.from("x"),
    });
    const stored = await state.get<{ at: Date; raw: Buffer }>("k");

    expect(stored!.at).toEqual(new Date("2030-01-01T00:00:00.000Z"));
    expect(stored!.raw.toString()).toBe("x");
  });

  it("uses a store given to the host without reaching the consumer", async () => {
    const own = createMemoryState();
    const b = bridge({}, { state: own });
    await handshake(b);

    await b.hostChat().getState().set("local", 1);

    expect(await own.get("local")).toBe(1);
    // Never left the host, so the consumer's store knows nothing about it.
    expect(await b.chat.getState().get("local")).toBeNull();
  });

  it("refuses the operations scoped access does not cover", async () => {
    const b = bridge();
    await handshake(b);
    const state = b.hostChat().getState();

    // Locks and queues are Chat's business on the consumer side.
    await expect(state.acquireLock("mock:general:1", 1000)).rejects.toThrow(
      /not available/,
    );
  });

  it("lends every keyed operation, lists included, under the prefix", async () => {
    const b = bridge();
    await handshake(b);
    const state = b.hostChat().getState();

    await state.appendToList("votes", "yes");

    expect(await state.getList("votes")).toEqual(["yes"]);
    expect(await b.chat.getState().getList("adapter:mock:votes")).toEqual([
      "yes",
    ]);
  });

  it("lends the whole store when told to", async () => {
    const b = bridge({}, {}, { hostState: "full" });
    await handshake(b);
    const state = b.hostChat().getState();

    await state.set("bare", 1);

    expect(await b.chat.getState().get("bare")).toBe(1);
  });

  it("falls back to a local store when the consumer lends nothing", async () => {
    const b = bridge({}, {}, { hostState: "off" });
    await handshake(b);
    const state = b.hostChat().getState();

    await state.set("k", "kept locally");

    expect(await state.get("k")).toBe("kept locally");
    // Never reached the consumer.
    expect(await b.chat.getState().get("adapter:mock:k")).toBeNull();
  });

  it("refuses an operation outside the StateAdapter surface", async () => {
    const b = bridge();
    await handshake(b);
    const rpc = Reflect.get(b.hostChat(), "rpc") as {
      request: (method: string, params: unknown) => Promise<unknown>;
    };

    await expect(
      rpc.request("state", ["constructor", []]),
    ).rejects.toBeDefined();
  });

  it("survives a rejection thrown inside the adapter's own event loop", async () => {
    const onError = vi.fn();
    const b = bridge({}, { onError });
    await handshake(b);

    // What Baileys does: an async handler outside any request of ours.
    void Promise.reject(new Error("poll update blew up"));
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(onError).toHaveBeenCalledWith(expect.any(Error), {
      phase: "adapter",
    });
  });

  it("guards the process with one listener however many hosts run", async () => {
    const before = process.listenerCount("unhandledRejection");
    const bridges = Array.from({ length: 12 }, () => bridge());
    await Promise.all(bridges.map(handshake));
    expect(process.listenerCount("unhandledRejection")).toBeLessThanOrEqual(
      before + 1,
    );

    await Promise.all(bridges.map((b) => b.host.stop()));

    expect(process.listenerCount("unhandledRejection")).toBe(before);
  });
});

describe("host chat instance", () => {
  it("answers getState synchronously, as adapters call it", async () => {
    const b = bridge();
    await handshake(b);
    const chat: ChatInstance = b.hostChat();

    // Baileys does `this._chat.getState().set(...)` in one expression.
    expect(() => chat.getState().set("k", 1)).not.toThrow();
  });
});

describe("thread facts across instances", () => {
  it("lets an instance that never saw the message answer isDM", async () => {
    const shared = createMemoryState();
    let hostChat!: ChatInstance;
    const adapter = createMockAdapter("mock", {
      initialize: vi.fn(
        async (instance: ChatInstance) => void (hostChat = instance),
      ),
      isDM: vi.fn((threadId: string) => threadId.includes(":D")),
    });

    let consumerA!: Chat;
    const host = serveAdapter(adapter, {
      secret: SECRET,
      consumerUrl: CONSUMER_URL,
      fetch: async (input, init) =>
        consumerA.webhooks.mock!(new Request(input, init), {}),
    });
    const toHost: FetchLike = (input, init) =>
      host.handleRequest(new Request(input, init));
    const consumer = () =>
      createRemoteAdapter({
        url: HOST_URL,
        secret: SECRET,
        name: "mock",
        fetch: toHost,
      });

    const remoteA = consumer();
    consumerA = new Chat({
      userName: "bot",
      adapters: { mock: remoteA },
      state: shared,
    });
    await consumerA.initialize();
    await host.ready;

    await hostChat.processMessage(adapter, "mock:D1:1", message("a DM"));
    await new Promise((resolve) => setTimeout(resolve, 20));

    // A second instance: same store, never received that message.
    const remoteB = consumer();
    const consumerB = new Chat({
      userName: "bot",
      adapters: { mock: remoteB },
      state: shared,
    });
    expect(remoteB.isDM("mock:D1:1")).toBe(false);
    await consumerB.initialize();

    expect(remoteB.isDM("mock:D1:1")).toBe(true);
    expect(remoteB.channelIdFromThreadId("mock:D1:1")).toBe(
      remoteA.channelIdFromThreadId("mock:D1:1"),
    );
  });

  it("lists a thread once however many instances meet it", async () => {
    const shared = createMemoryState();
    let hostChat!: ChatInstance;
    const adapter = createMockAdapter("mock", {
      initialize: vi.fn(
        async (instance: ChatInstance) => void (hostChat = instance),
      ),
    });
    let target!: Chat;
    const host = serveAdapter(adapter, {
      secret: SECRET,
      consumerUrl: CONSUMER_URL,
      fetch: async (input, init) =>
        target.webhooks.mock!(new Request(input, init), {}),
    });
    const instance = async () => {
      const chat = new Chat({
        userName: "bot",
        adapters: {
          mock: createRemoteAdapter({
            url: HOST_URL,
            secret: SECRET,
            name: "mock",
            fetch: (input, init) =>
              host.handleRequest(new Request(input, init)),
          }),
        },
        state: shared,
      });
      await chat.initialize();
      return chat;
    };
    const [first, second] = [await instance(), await instance()];
    await host.ready;

    for (const chat of [first, second]) {
      target = chat;
      await hostChat.processMessage(adapter, "mock:C1:1", message("hi"));
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    expect(await shared.getList("remote:mock:threads")).toEqual(["mock:C1:1"]);
  });

  it("falls back to the default for a thread no instance has seen", async () => {
    const b = bridge();
    await handshake(b);

    expect(b.remote.isDM("mock:never:1")).toBe(false);
  });
});
