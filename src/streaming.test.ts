import type { StreamChunk } from "chat";
import { describe, expect, it, vi } from "vitest";

import { StreamDiscardedError } from "./rpc/errors";
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
      return { id: "streamed", threadId: THREAD, raw: {} };
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
      return { id: "streamed", threadId: THREAD, raw: {} };
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

  it("stops reading once the turn is aborted and lets the adapter decide", async () => {
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

    // Native mode: the adapter got the abort and decides what it posts.
    expect(result).toMatchObject({ id: "partial" });
    // `for await` cannot cancel a pull already in flight, so one more chunk is
    // read after the abort; the rest is left alone.
    expect(state.pulled).toBe(2);
  });

  it("forwards stream options and gives the adapter a signal of its own", async () => {
    const seen: Array<{ recipientUserId?: string; signal?: AbortSignal }> = [];
    const b = streamingBridge(async (_threadId, chunks, options) => {
      seen.push(options as never);
      for await (const _chunk of chunks) void _chunk;
      return { id: "streamed", threadId: THREAD, raw: {} };
    });
    await handshake(b);
    const consumerSignal = new AbortController().signal;

    await b.remote.stream!(THREAD, source(["a"]).iterable, {
      recipientUserId: "u1",
      signal: consumerSignal,
    });

    expect(seen[0]!.recipientUserId).toBe("u1");
    // The consumer's signal cannot cross the wire; the host makes its own.
    expect(seen[0]!.signal).toBeInstanceOf(AbortSignal);
    expect(seen[0]!.signal).not.toBe(consumerSignal);
  });

  it("aborts the adapter's signal when the turn is cut off", async () => {
    let signal!: AbortSignal;
    const b = streamingBridge(async (_threadId, chunks, options) => {
      signal = (options as { signal: AbortSignal }).signal;
      for await (const _chunk of chunks) void _chunk;
      return null;
    });
    await handshake(b);
    const controller = new AbortController();

    await expect(
      b.remote.stream!(
        THREAD,
        {
          async *[Symbol.asyncIterator]() {
            yield "partial";
            controller.abort();
            yield "never";
          },
        },
        { signal: controller.signal },
      ),
    ).rejects.toBeInstanceOf(StreamDiscardedError);

    // Before this the adapter saw the iterable end exactly as it would on success.
    expect(signal.aborted).toBe(true);
  });

  it("leaves the signal alone when the stream finishes normally", async () => {
    let signal!: AbortSignal;
    const b = streamingBridge(async (_threadId, chunks, options) => {
      signal = (options as { signal: AbortSignal }).signal;
      for await (const _chunk of chunks) void _chunk;
      return { id: "done", threadId: THREAD, raw: {} };
    });
    await handshake(b);

    await b.remote.stream!(THREAD, source(["a", "b"]).iterable);

    expect(signal.aborted).toBe(false);
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

  it("gives up on an adapter that never starts streaming", async () => {
    // Shorter than the client timeout, so the host answers before the consumer
    // gives up and nothing is left holding the request open.
    const b = bridge(
      { stream: vi.fn(() => new Promise(() => {})) as never },
      { streamStartTimeoutMs: 20 },
    );
    await handshake(b);

    await expect(
      b.remote.stream!(THREAD, source(["a"]).iterable),
    ).rejects.toThrow(/did not start streaming/);
  });

  it("ends the stream when a push fails mid-flight", async () => {
    const b = streamingBridge(async (_threadId, chunks) => {
      for await (const _chunk of chunks) void _chunk;
      return null;
    });
    await handshake(b);
    const rpc = Reflect.get(b.remote, "rpc") as {
      request: (method: string, params: unknown) => Promise<unknown>;
    };
    const original = rpc.request.bind(rpc);
    let ended: boolean | undefined;
    rpc.request = async (method, params) => {
      if (method === "streamPush") throw new Error("network died");
      if (method === "streamEnd") ended = (params as [string, boolean])[1];
      return original(method, params);
    };

    await expect(
      b.remote.stream!(THREAD, source(["a", "b"]).iterable),
    ).rejects.toThrow(/network died/);
    expect(ended).toBe(true);
  });

  it("reclaims a stream abandoned mid-flight without another stream call", async () => {
    vi.useFakeTimers();
    try {
      let released = false;
      const b = bridge(
        {
          stream: vi.fn(async (_t: string, chunks: AsyncIterable<unknown>) => {
            for await (const _chunk of chunks) void _chunk;
            // Reached only once the queue is closed, which is what proves the
            // adapter was released rather than left blocked forever.
            released = true;
            return null;
          }) as never,
        },
        { streamTtlMs: 1000 },
      );
      await handshake(b);
      const rpc = Reflect.get(b.remote, "rpc") as {
        request: (method: string, params: unknown) => Promise<unknown>;
      };

      await rpc.request("streamStart", [THREAD, undefined]);
      expect(released).toBe(false);

      // No further stream call: only the timer can reclaim it.
      await vi.advanceTimersByTimeAsync(2500);

      expect(released).toBe(true);
    } finally {
      vi.useRealTimers();
    }
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
