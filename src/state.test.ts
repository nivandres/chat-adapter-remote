import { createMemoryState } from "@chat-adapter/state-memory";
import type { ChatInstance } from "chat";
import { describe, expect, it, vi } from "vitest";

import { bridge, handshake } from "./testing/bridge";

describe("adapter state", () => {
  it("reaches the consumer's store, the way an adapter persists its own data", async () => {
    const b = bridge();
    await handshake(b);
    const state = b.hostChat().getState();

    await state.set("poll:1", { secret: "abc" });

    expect(await state.get("poll:1")).toEqual({ secret: "abc" });
    expect(await b.chat.getState().get("poll:1")).toEqual({ secret: "abc" });
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
    const escaped = Promise.reject(new Error("poll update blew up"));
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(onError).toHaveBeenCalledWith(expect.any(Error), {
      phase: "adapter",
    });
    await escaped.catch(() => undefined);
  });

  it("stops guarding the process once the host is stopped", async () => {
    const before = process.listenerCount("unhandledRejection");
    const b = bridge();
    await handshake(b);
    expect(process.listenerCount("unhandledRejection")).toBeGreaterThan(before);

    await b.host.stop();

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
