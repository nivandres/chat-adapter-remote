import type { StreamChunk } from "chat";
import { describe, expect, it, vi } from "vitest";

import { bridge, handshake, type Bridge } from "./testing/bridge";

const THREAD = "mock:general:1";

/** Counts what the consumer actually pulled, so a delegated stream can be proved untouched. */
function source(chunks: Array<string | StreamChunk>) {
  const state = { pulled: 0 };
  return {
    state,
    iterable: {
      async *[Symbol.asyncIterator]() {
        for (const chunk of chunks) {
          state.pulled++;
          yield chunk;
        }
      },
    } as AsyncIterable<string | StreamChunk>,
  };
}

function streamingBridge(
  stream: (
    threadId: string,
    chunks: AsyncIterable<string | StreamChunk>,
    options?: unknown,
  ) => Promise<unknown>,
): Bridge {
  return bridge({ stream: vi.fn(stream) as never });
}

describe("streaming", () => {
  it("delivers every chunk in order and returns the adapter's message", async () => {
    const received: Array<string | StreamChunk> = [];
    const b = streamingBridge(async (_threadId, chunks) => {
      for await (const chunk of chunks) received.push(chunk);
      return { id: "streamed", threadId: THREAD, raw: {} };
    });
    await handshake(b);

    const result = await b.remote.stream!(
      THREAD,
      source(["a", "b", { type: "markdown_text", text: "c" }]).iterable,
    );

    expect(received).toEqual(["a", "b", { type: "markdown_text", text: "c" }]);
    expect(result).toEqual({ id: "streamed", threadId: THREAD, raw: {} });
  });

  it("leaves the caller's iterable untouched when the adapter declines", async () => {
    const b = streamingBridge(async () => null);
    await handshake(b);
    const { state, iterable } = source(["a", "b"]);

    const result = await b.remote.stream!(THREAD, iterable);

    expect(result).toBeNull();
    // Chat's post+edit fallback re-reads this same iterable, so consuming it
    // here would silently produce an empty message.
    expect(state.pulled).toBe(0);
  });

  it("sends the first chunk immediately and coalesces the rest", async () => {
    const b = streamingBridge(async (_threadId, chunks) => {
      for await (const _chunk of chunks) void _chunk;
      return null;
    });
    await handshake(b);
    const pushes: number[] = [];
    const rpc = Reflect.get(b.remote, "rpc") as {
      request: (method: string, params: unknown) => Promise<unknown>;
    };
    const original = rpc.request.bind(rpc);
    rpc.request = async (method, params) => {
      if (method === "streamPush")
        pushes.push((params as [string, unknown[]])[1].length);
      return original(method, params);
    };

    const chunks = Array.from({ length: 64 }, (_, index) => `c${index}`);
    await b.remote.stream!(THREAD, source(chunks).iterable);

    expect(pushes[0]).toBe(1);
    expect(pushes.length).toBeLessThan(chunks.length);
    expect(pushes.reduce((total, size) => total + size, 0)).toBe(64);
  });

  it("does not hold buffered chunks back while the producer pauses", async () => {
    const seen: string[] = [];
    const b = streamingBridge(async (_threadId, chunks) => {
      for await (const chunk of chunks) seen.push(chunk as string);
      return null;
    });
    await handshake(b);

    await b.remote.stream!(THREAD, {
      async *[Symbol.asyncIterator]() {
        yield "before the pause";
        await new Promise((resolve) => setTimeout(resolve, 60));
        expect(seen).toEqual(["before the pause"]);
        yield "after the pause";
      },
    });

    expect(seen).toEqual(["before the pause", "after the pause"]);
  });

  it("stops reading and returns null once the turn is aborted", async () => {
    const b = streamingBridge(async (_threadId, chunks) => {
      for await (const _chunk of chunks) void _chunk;
      return { id: "partial", threadId: THREAD, raw: {} };
    });
    await handshake(b);
    const controller = new AbortController();
    const { state, iterable } = source(["a", "b", "c", "d"]);

    const result = await b.remote.stream!(
      THREAD,
      {
        async *[Symbol.asyncIterator]() {
          for await (const chunk of iterable) {
            yield chunk;
            controller.abort();
          }
        },
      },
      { signal: controller.signal },
    );

    expect(result).toBeNull();
    // `for await` cannot cancel a pull already in flight, so one more chunk is
    // read after the abort; the rest is left alone.
    expect(state.pulled).toBe(2);
  });

  it("forwards stream options but never the abort signal", async () => {
    const seen: unknown[] = [];
    const b = streamingBridge(async (_threadId, chunks, options) => {
      seen.push(options);
      for await (const _chunk of chunks) void _chunk;
      return null;
    });
    await handshake(b);

    await b.remote.stream!(THREAD, source(["a"]).iterable, {
      recipientUserId: "u1",
      signal: new AbortController().signal,
    });

    expect(seen[0]).toEqual({ recipientUserId: "u1" });
  });

  it("surfaces a failure the adapter raises, before or during the stream", async () => {
    const early = streamingBridge(async () => {
      throw new Error("no streaming session available");
    });
    await handshake(early);
    await expect(
      early.remote.stream!(THREAD, source(["a"]).iterable),
    ).rejects.toThrow(/Internal adapter error/);

    const late = streamingBridge(async (_threadId, chunks) => {
      for await (const _chunk of chunks) break;
      throw new Error("platform rejected the stream");
    });
    await handshake(late);
    await expect(
      late.remote.stream!(THREAD, source(["a", "b"]).iterable),
    ).rejects.toThrow(/Internal adapter error/);
  });

  it("refuses a push for a stream it does not know", async () => {
    const b = streamingBridge(async () => null);
    await handshake(b);
    const rpc = Reflect.get(b.remote, "rpc") as {
      request: (method: string, params: unknown) => Promise<unknown>;
    };

    await expect(rpc.request("streamPush", ["s404", ["a"]])).rejects.toThrow(
      /unknown or has expired/,
    );
  });
});
