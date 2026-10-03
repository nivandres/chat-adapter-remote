import { decode, encode } from "./codec";
import { readBody } from "./dispatch";
import { JsonRpcResponseSchema, isErrorResponse } from "./envelope";
import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  deserializeError,
} from "./errors";
import {
  NONCE_HEADER,
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
  secretsOf,
  sign,
  type Secret,
} from "./signing";
import type { FetchLike } from "../types";

/** How long notifications are skipped after one fails to arrive. */
const NOTIFY_PAUSE_MS = 30_000;
/** Past this many unanswered, further notifications are skipped. */
const MAX_NOTIFY_IN_FLIGHT = 16;

export interface RpcClientOptions {
  url: string;
  secret: Secret;
  /** Read for every request. */
  headers?: () => Record<string, string>;
  timeoutMs?: number;
  maxBodyBytes?: number;
  fetch?: FetchLike;
  /** Notifications skipped while the far side was unreachable or backed up, reported once one arrives again. */
  onSkipped?: (count: number) => void;
}

export interface RpcClient {
  request(method: string, params: unknown): Promise<unknown>;
  /** Best effort: never retried, and skipped while the far side is unreachable. Never throws and never rejects. */
  notify(method: string, params: unknown): void;
}

export function createRpcClient(options: RpcClientOptions): RpcClient {
  const doFetch = options.fetch ?? fetch;
  const timeoutMs = options.timeoutMs ?? 10_000;
  const maxBodyBytes = options.maxBodyBytes ?? Number.POSITIVE_INFINITY;
  let nextId = 1;
  let pausedUntil = 0;
  let inFlight = 0;
  let skipped = 0;

  function reached(): void {
    pausedUntil = 0;
    if (!skipped) return;
    const count = skipped;
    skipped = 0;
    options.onSkipped?.(count);
  }

  async function send(
    method: string,
    params: unknown,
    id?: number,
  ): Promise<Response> {
    const body = JSON.stringify({
      jsonrpc: "2.0",
      ...(id === undefined ? {} : { id }),
      method,
      params: await encode(params),
    });
    const timestamp = String(Date.now());
    const nonce = crypto.randomUUID();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await doFetch(options.url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...options.headers?.(),
          [SIGNATURE_HEADER]: sign(
            body,
            timestamp,
            nonce,
            secretsOf(options.secret)[0]!,
          ),
          [TIMESTAMP_HEADER]: timestamp,
          [NONCE_HEADER]: nonce,
        },
        body,
        signal: controller.signal,
      });
      if (response.status < 500) reached();
      return response;
    } catch (error) {
      if (controller.signal.aborted) {
        throw new RemoteAdapterRpcError(
          RpcErrorCode.TIMEOUT,
          `chat-adapter-remote: ${method} got no answer within ${timeoutMs}ms`,
        );
      }
      throw new RemoteAdapterRpcError(
        RpcErrorCode.UNAVAILABLE,
        `chat-adapter-remote: ${options.url} is unreachable (${(error as Error).message})`,
      );
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    async request(method, params) {
      const id = nextId++;
      const response = await send(method, params, id);
      const text = await readBody(response, maxBodyBytes);
      if (text === null) {
        throw new RemoteAdapterRpcError(
          RpcErrorCode.INVALID_REQUEST,
          `chat-adapter-remote: response larger than ${maxBodyBytes} bytes`,
        );
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = undefined;
      }
      // Never handled, so the one case a caller can safely retry.
      if (response.status >= 500) {
        const reason = (parsed as { error?: { message?: string } } | undefined)
          ?.error?.message;
        throw new RemoteAdapterRpcError(
          RpcErrorCode.UNAVAILABLE,
          `chat-adapter-remote: ${method} answered HTTP ${response.status}${reason ? ` (${reason})` : ""}`,
        );
      }
      if (parsed === undefined) {
        throw new RemoteAdapterRpcError(
          RpcErrorCode.INTERNAL_ERROR,
          `chat-adapter-remote: non-JSON response (HTTP ${response.status})`,
        );
      }
      const envelope = JsonRpcResponseSchema.parse(parsed);
      // Pre-parse rejections answer with `id: null`, so read the error first.
      if (isErrorResponse(envelope)) throw deserializeError(envelope.error);
      if (envelope.id !== id) {
        throw new RemoteAdapterRpcError(
          RpcErrorCode.INTERNAL_ERROR,
          "chat-adapter-remote: response id did not match request",
        );
      }
      return decode(envelope.result);
    },
    notify(method, params) {
      const now = Date.now();
      if (now < pausedUntil || inFlight >= MAX_NOTIFY_IN_FLIGHT) {
        skipped++;
        return;
      }
      // Once a pause runs out, one goes through as a probe and the rest wait on it.
      if (pausedUntil) pausedUntil = now + NOTIFY_PAUSE_MS;
      inFlight++;
      void send(method, params)
        .then(
          (response) => response.status < 500,
          () => false,
        )
        .then((arrived) => {
          inFlight--;
          if (!arrived) pausedUntil = Date.now() + NOTIFY_PAUSE_MS;
        });
    },
  };
}
