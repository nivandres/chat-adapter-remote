import type {
  Adapter,
  Logger,
  RawMessage,
  StreamChunk,
  StreamOptions,
} from "chat";

import { StreamDiscardedError } from "../rpc/errors";

/** `native` uses the adapter's own; `buffer` posts once; `edit` posts then edits; `off` leaves it to Chat. */
export type StreamMode = "native" | "buffer" | "edit" | "off";

export type PublishOnAbort = "partial" | "discard" | { minChars: number };

export interface StreamModeOptions {
  /** Default `native` when the adapter streams, otherwise `off`, as with any other adapter. */
  mode?: StreamMode;
  /** `buffer` only: what to do with a cut-off reply. Default `"partial"`, as the adapters that gather do. */
  publishOnAbort?: PublishOnAbort;
  /** Typing renewal. Default 4s, under the shortest platform timeout. */
  typingIntervalMs?: number;
  /** Default 1.5s. */
  editIntervalMs?: number;
}

type Chunk = string | StreamChunk;
type Streamer = (
  threadId: string,
  chunks: AsyncIterable<Chunk>,
  options: StreamOptions,
) => Promise<RawMessage<unknown> | null>;

function textOf(chunk: Chunk): string {
  if (typeof chunk === "string") return chunk;
  return chunk.type === "markdown_text" ? chunk.text : "";
}

function keepsPartial(policy: PublishOnAbort, text: string): boolean {
  if (policy === "partial") return true;
  if (policy === "discard") return false;
  return text.trim().length >= policy.minChars;
}

/** The returned stop also ends the indicator, which Chat cannot: it never started it. */
function keepTyping(
  adapter: Adapter,
  threadId: string,
  intervalMs: number,
  logger: Logger,
): () => Promise<void> {
  const failed = (error: unknown) =>
    logger.debug("typing indicator failed", { threadId, error });
  const renew = () => adapter.startTyping(threadId).catch(failed);
  void renew();
  const timer = setInterval(() => void renew(), intervalMs);
  timer.unref?.();
  return async () => {
    clearInterval(timer);
    await adapter.endTyping?.(threadId).catch(failed);
  };
}

export function resolveStreamMode(
  adapter: Adapter,
  requested?: StreamMode,
): StreamMode {
  const native = typeof adapter.stream === "function";
  const mode = requested ?? (native ? "native" : "off");
  return mode === "native" && !native ? "off" : mode;
}

/** A deliberate non-post is `StreamDiscardedError`, never `null`. */
export function createStreamer(
  adapter: Adapter,
  mode: Exclude<StreamMode, "off">,
  options: StreamModeOptions,
  logger: Logger,
): Streamer {
  const policy = options.publishOnAbort ?? "partial";
  const typingIntervalMs = options.typingIntervalMs ?? 4_000;
  const editIntervalMs = options.editIntervalMs ?? 1_500;

  if (mode === "native") {
    return (threadId, chunks, streamOptions) =>
      adapter.stream!(threadId, chunks, streamOptions);
  }

  if (mode === "buffer") {
    return async (threadId, chunks, streamOptions) => {
      let text = "";
      const stopTyping = keepTyping(
        adapter,
        threadId,
        typingIntervalMs,
        logger,
      );
      try {
        for await (const chunk of chunks) text += textOf(chunk);
      } finally {
        await stopTyping();
      }
      if (streamOptions.signal?.aborted && !keepsPartial(policy, text)) {
        throw new StreamDiscardedError(
          "chat-adapter-remote: the reply was cut off",
        );
      }
      if (!text.trim())
        throw new StreamDiscardedError(
          "chat-adapter-remote: the reply was empty",
        );
      return adapter.postMessage(threadId, { markdown: text });
    };
  }

  return async (threadId, chunks) => {
    let text = "";
    let shown = "";
    let posted: RawMessage<unknown> | undefined;
    let lastEdit = 0;

    const show = async () => {
      if (!text.trim() || text === shown) return;
      // Some platforms answer an edit with a new id; the posted message stays the target.
      if (posted) {
        await adapter.editMessage(threadId, posted.id, { markdown: text });
      } else {
        posted = await adapter.postMessage(threadId, { markdown: text });
      }
      shown = text;
      lastEdit = Date.now();
    };

    const stopTyping = keepTyping(adapter, threadId, typingIntervalMs, logger);
    try {
      for await (const chunk of chunks) {
        text += textOf(chunk);
        if (Date.now() - lastEdit >= editIntervalMs) {
          await show().catch((error: unknown) => {
            logger.warn("stream edit failed", { threadId, error });
          });
        }
      }
    } finally {
      await stopTyping();
    }

    // What was posted and edited stays; a cut only stops further edits.
    await show();
    if (!posted) {
      throw new StreamDiscardedError("chat-adapter-remote: nothing was posted");
    }
    return posted;
  };
}
