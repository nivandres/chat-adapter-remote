import type { RawMessage, StreamChunk } from "chat";

import { RemoteAdapterRpcError, RpcErrorCode } from "../rpc/errors";

type Chunk = string | StreamChunk;

interface Queue {
  push(chunk: Chunk): void;
  close(): void;
  iterable: AsyncIterable<Chunk>;
}

function createQueue(onFirstPull: () => void): Queue {
  const pending: Chunk[] = [];
  let wake: (() => void) | undefined;
  let closed = false;

  const notify = () => {
    wake?.();
    wake = undefined;
  };

  return {
    push(chunk) {
      pending.push(chunk);
      notify();
    },
    close() {
      closed = true;
      notify();
    },
    iterable: {
      async *[Symbol.asyncIterator]() {
        onFirstPull();
        for (;;) {
          if (pending.length > 0) {
            yield pending.shift()!;
            continue;
          }
          if (closed) return;
          await new Promise<void>((resolve) => (wake = resolve));
        }
      },
    },
  };
}

type StreamRun = (
  chunks: AsyncIterable<Chunk>,
) => Promise<RawMessage<unknown> | null>;

/** `done` means the adapter answered without reading anything, which is how it delegates back to Chat SDK's own fallback. */
export type StreamStart =
  { streamId: string } | { done: true; result: RawMessage<unknown> | null };

interface PendingStream {
  queue: Queue;
  result: Promise<RawMessage<unknown> | null>;
  expiresAt: number;
}

export interface StreamRegistryOptions {
  /** Streams idle for longer than this are dropped. Default 5 minutes. */
  ttlMs?: number;
  /** How long the adapter may take to either answer or start reading. Default 10 seconds. */
  startTimeoutMs?: number;
}

/** Rebuilds the AsyncIterable `Adapter.stream` expects from the consumer's open/push/end calls. */
export class StreamRegistry {
  private readonly streams = new Map<string, PendingStream>();
  private readonly ttlMs: number;
  private readonly startTimeoutMs: number;
  private sequence = 0;

  constructor(options: StreamRegistryOptions = {}) {
    this.ttlMs = options.ttlMs ?? 300_000;
    this.startTimeoutMs = options.startTimeoutMs ?? 10_000;
  }

  async open(run: StreamRun): Promise<StreamStart> {
    this.sweep();
    let consuming!: () => void;
    const started = new Promise<void>((resolve) => (consuming = resolve));
    const queue = createQueue(consuming);
    const result = run(queue.iterable);
    // end() observes this; the no-op keeps a mid-stream failure from being unhandled meanwhile.
    result.catch(() => {});

    // An adapter that neither answers nor starts reading would otherwise hold
    // the request open with no id, leaving nothing for sweep() to reclaim.
    let expire: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<"timeout">((resolve) => {
      expire = setTimeout(() => resolve("timeout"), this.startTimeoutMs);
    });

    const settled = await Promise.race([
      result.then(
        (value) => ({ value }),
        (error: unknown) => ({ error }),
      ),
      started.then(() => undefined),
      timedOut,
    ]);
    clearTimeout(expire);

    if (settled === "timeout") {
      // Not awaited: an adapter that hung here may never settle, which is the
      // very case being handled. The catch above keeps it from going unhandled.
      queue.close();
      throw new RemoteAdapterRpcError(
        RpcErrorCode.STREAM_NOT_FOUND,
        `chat-adapter-remote: the adapter did not start streaming within ${this.startTimeoutMs}ms`,
      );
    }
    if (settled) {
      if ("error" in settled) throw settled.error;
      return { done: true, result: settled.value };
    }

    const streamId = `s${++this.sequence}`;
    this.streams.set(streamId, {
      queue,
      result,
      expiresAt: Date.now() + this.ttlMs,
    });
    return { streamId };
  }

  push(streamId: string, chunks: Chunk[]): void {
    const stream = this.require(streamId);
    stream.expiresAt = Date.now() + this.ttlMs;
    for (const chunk of chunks) stream.queue.push(chunk);
  }

  async end(
    streamId: string,
    aborted?: boolean,
  ): Promise<RawMessage<unknown> | null> {
    const stream = this.require(streamId);
    this.streams.delete(streamId);
    stream.queue.close();
    if (!aborted) return stream.result;
    await stream.result.catch(() => undefined);
    return null;
  }

  /** Closes every open stream, releasing whatever the adapter holds for them. */
  clear(): void {
    for (const stream of this.streams.values()) stream.queue.close();
    this.streams.clear();
  }

  private require(streamId: string): PendingStream {
    // Expiry is enforced on use, so nothing runs in the background.
    this.sweep();
    const stream = this.streams.get(streamId);
    if (!stream) {
      throw new RemoteAdapterRpcError(
        RpcErrorCode.STREAM_NOT_FOUND,
        `chat-adapter-remote: stream ${streamId} is unknown or has expired`,
      );
    }
    return stream;
  }

  private sweep(): void {
    const now = Date.now();
    for (const [streamId, stream] of this.streams) {
      if (stream.expiresAt > now) continue;
      this.streams.delete(streamId);
      stream.queue.close();
    }
  }
}
