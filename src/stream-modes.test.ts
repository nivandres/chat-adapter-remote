import type { Adapter } from "chat";
import { describe, expect, it, vi } from "vitest";

import type { StreamModeOptions } from "./host/stream-modes";
import { StreamDiscardedError } from "./rpc/errors";
import { bridge, handshake } from "./testing/bridge";

const THREAD = "mock:general:1";

function reply(chunks: string[], abortAfter?: number) {
  const controller = new AbortController();
  return {
    signal: controller.signal,
    iterable: {
      async *[Symbol.asyncIterator]() {
        for (const [index, chunk] of chunks.entries()) {
          yield chunk;
          if (index + 1 === abortAfter) controller.abort();
        }
      },
    } as AsyncIterable<string>,
  };
}

/** The mock adapter has no `stream` of its own, which is the case these modes exist for. */
async function streamingWith(
  stream: StreamModeOptions = {},
  overrides: Partial<Adapter> = {},
) {
  const b = bridge(overrides, { stream });
  await handshake(b);
  return b;
}

describe("buffer mode", () => {
  it("is what an adapter without native streaming gets, and posts one message", async () => {
    const b = await streamingWith();
    const { iterable } = reply(["Hel", "lo ", "there"]);

    await b.remote.stream!(THREAD, iterable);

    expect(b.adapter.postMessage).toHaveBeenCalledOnce();
    expect(b.adapter.postMessage).toHaveBeenCalledWith(THREAD, {
      markdown: "Hello there",
    });
    expect(b.adapter.editMessage).not.toHaveBeenCalled();
  });

  it("keeps the typing indicator up while it gathers the reply", async () => {
    const b = await streamingWith();

    await b.remote.stream!(THREAD, reply(["a"]).iterable);

    expect(b.adapter.startTyping).toHaveBeenCalledWith(THREAD);
  });

  it("posts nothing for an empty reply, and says so with a typed error", async () => {
    const b = await streamingWith();

    await expect(
      b.remote.stream!(THREAD, reply(["", "  "]).iterable),
    ).rejects.toBeInstanceOf(StreamDiscardedError);
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });

  it("does not report a discarded reply as a failure", async () => {
    const onError = vi.fn();
    const b = bridge({}, { onError });
    await handshake(b);

    await expect(
      b.remote.stream!(THREAD, reply([]).iterable),
    ).rejects.toBeInstanceOf(StreamDiscardedError);
    expect(onError).not.toHaveBeenCalled();
  });

  it("discards a cut-off reply by default", async () => {
    const b = await streamingWith();
    const { iterable, signal } = reply(["half a sen", "tence"], 1);

    await expect(
      b.remote.stream!(THREAD, iterable, { signal }),
    ).rejects.toBeInstanceOf(StreamDiscardedError);
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });

  it("posts what arrived before the cut when asked to", async () => {
    const b = await streamingWith({ publishOnAbort: "partial" });
    const { iterable, signal } = reply(["kept", " dropped"], 1);

    await b.remote.stream!(THREAD, iterable, { signal });

    expect(b.adapter.postMessage).toHaveBeenCalledWith(THREAD, {
      markdown: "kept",
    });
  });

  it("posts a cut-off reply only once it is long enough", async () => {
    const short = await streamingWith({ publishOnAbort: { minChars: 20 } });
    const cut = reply(["too short", " more"], 1);
    await expect(
      short.remote.stream!(THREAD, cut.iterable, { signal: cut.signal }),
    ).rejects.toBeInstanceOf(StreamDiscardedError);

    const long = await streamingWith({ publishOnAbort: { minChars: 5 } });
    const kept = reply(["long enough", " more"], 1);
    await long.remote.stream!(THREAD, kept.iterable, { signal: kept.signal });
    expect(long.adapter.postMessage).toHaveBeenCalledWith(THREAD, {
      markdown: "long enough",
    });
  });
});

describe("edit mode", () => {
  it("posts once there is text and edits it as more arrives, with no placeholder", async () => {
    const b = await streamingWith({ mode: "edit", editIntervalMs: 0 });

    await b.remote.stream!(THREAD, reply(["", "first", " second"]).iterable);

    expect(b.adapter.postMessage).toHaveBeenCalledOnce();
    expect(b.adapter.postMessage).toHaveBeenCalledWith(THREAD, {
      markdown: "first",
    });
    expect(b.adapter.editMessage).toHaveBeenLastCalledWith(
      THREAD,
      expect.any(String),
      { markdown: "first second" },
    );
  });

  it("keeps editing the posted message when an edit answers with a new id", async () => {
    let edits = 0;
    const b = await streamingWith(
      { mode: "edit", editIntervalMs: 0 },
      {
        editMessage: vi.fn(async () => ({
          id: `edit-${++edits}`,
          threadId: THREAD,
          raw: {},
        })),
      },
    );

    const result = await b.remote.stream!(
      THREAD,
      reply(["a", "b", "c"]).iterable,
    );

    expect(b.adapter.editMessage).toHaveBeenCalledTimes(2);
    for (const [, messageId] of vi.mocked(b.adapter.editMessage).mock.calls) {
      expect(messageId).toBe("msg-1");
    }
    expect(result).toMatchObject({ id: "msg-1" });
  });

  it("carries on past an edit that fails midway", async () => {
    const editMessage = vi
      .fn()
      .mockRejectedValueOnce(new Error("rate limited"))
      .mockResolvedValue({ id: "msg-1", threadId: THREAD, raw: {} });
    const b = await streamingWith(
      { mode: "edit", editIntervalMs: 0 },
      { editMessage },
    );

    await b.remote.stream!(THREAD, reply(["a", "b", "c"]).iterable);

    expect(editMessage).toHaveBeenLastCalledWith(THREAD, "msg-1", {
      markdown: "abc",
    });
  });

  it("leaves what it posted as it was when the turn is cut off", async () => {
    const b = await streamingWith({ mode: "edit", editIntervalMs: 0 });
    const { iterable, signal } = reply(["posted", " then cut"], 1);

    const result = await b.remote.stream!(THREAD, iterable, { signal });

    // Streaming already sent and edited it: nothing is taken back.
    expect(result).toBeDefined();
    expect(b.adapter.deleteMessage).not.toHaveBeenCalled();
  });

  it("reports a cut before anything was posted as a discard, not a blank", async () => {
    const b = await streamingWith({ mode: "edit", editIntervalMs: 0 });
    const { iterable, signal } = reply(["", ""], 1);

    await expect(
      b.remote.stream!(THREAD, iterable, { signal }),
    ).rejects.toBeInstanceOf(StreamDiscardedError);
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });
});

describe("off and native", () => {
  it("announces no streaming when off, so Chat uses its own", async () => {
    const b = await streamingWith({ mode: "off" });

    expect(b.remote.stream).toBeUndefined();
  });

  it("falls back to off when native is asked of an adapter that has none", async () => {
    const b = await streamingWith({ mode: "native" });

    expect(b.remote.stream).toBeUndefined();
  });

  it("uses the adapter's own stream when it has one", async () => {
    const stream = vi.fn(async (_t: string, chunks: AsyncIterable<unknown>) => {
      for await (const _chunk of chunks) void _chunk;
      return { id: "native", threadId: THREAD, raw: {} };
    });
    const b = await streamingWith({}, { stream: stream as never });

    const result = await b.remote.stream!(THREAD, reply(["a"]).iterable);

    expect(result).toMatchObject({ id: "native" });
    expect(b.adapter.postMessage).not.toHaveBeenCalled();
  });
});
