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
    } finally {
      clearTimeout(timer);
    }
  }

  return {
    async request(method, params) {
      const id = nextId++;
      const response = await send(method, params, id);
      // Bounded like a request: a deferred attachment is fetched through here,
      // and the whole point of deferring it was that it did not fit a body.
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
        throw new RemoteAdapterRpcError(
          RpcErrorCode.INTERNAL_ERROR,
          `chat-adapter-remote: non-JSON response (HTTP ${response.status})`,
        );
      }
      const envelope = JsonRpcResponseSchema.parse(parsed);
      // Read the error before matching the id: a request rejected before it
      // was parsed (bad signature, oversized body, malformed JSON) answers
      // with `id: null` per JSON-RPC, and matching first would report every
      // one of those as an id mismatch instead of the actual reason.
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
