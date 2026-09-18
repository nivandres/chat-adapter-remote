import { decode, encode } from "./codec";
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
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    async request(method, params) {
      const id = nextId++;
      const response = await send(method, params, id);
      const text = await response.text();
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch {
        throw new RemoteAdapterRpcError(
          RpcErrorCode.INTERNAL_ERROR,
          `chat-adapter-remote: non-JSON response (HTTP ${response.status})`,
        );
      }
      const envelope = JsonRpcResponseSchema.parse(parsed);
      if (envelope.id !== id) {
        throw new RemoteAdapterRpcError(
          RpcErrorCode.INTERNAL_ERROR,
          "chat-adapter-remote: response id did not match request",
        );
      }
      if (isErrorResponse(envelope)) throw deserializeError(envelope.error);
      return decode(envelope.result);
    },
    notify(method, params) {
      void send(method, params).catch(() => {});
    },
  };
}
