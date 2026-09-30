import { decode, encode } from "./codec";
import { readBody } from "./dispatch";
import { JsonRpcResponseSchema, isErrorResponse } from "./envelope";
import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  deserializeError,
} from "./errors";
import { SIGNATURE_HEADER, TIMESTAMP_HEADER, sign } from "./signing";
import type { FetchLike } from "../types";

export interface RpcClientOptions {
  url: string;
  secret: string;
  timeoutMs?: number;
  maxBodyBytes?: number;
  fetch?: FetchLike;
}

export interface RpcClient {
  request(method: string, params: unknown): Promise<unknown>;
  /** Sends without awaiting a response. Never throws and never rejects. */
  notify(method: string, params: unknown): void;
}

export function createRpcClient(options: RpcClientOptions): RpcClient {
  const doFetch = options.fetch ?? fetch;
  const timeoutMs = options.timeoutMs ?? 10_000;
  const maxBodyBytes = options.maxBodyBytes ?? Number.POSITIVE_INFINITY;
  let nextId = 1;

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
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await doFetch(options.url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          [SIGNATURE_HEADER]: sign(body, timestamp, options.secret),
          [TIMESTAMP_HEADER]: timestamp,
        },
        body,
        signal: controller.signal,
      });
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
      void send(method, params).catch(() => {});
    },
  };
}
