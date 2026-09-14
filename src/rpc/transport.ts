import { decodeBuffers, encodeBuffers } from "./buffers";
import { JsonRpcResponseSchema, isErrorResponse } from "./envelope";
import { deserializeError } from "./errors";
import { sign, SIGNATURE_HEADER, TIMESTAMP_HEADER } from "./signing";

export interface RpcClientOptions {
  url: string;
  secret: string;
  timeoutMs?: number;
  fetch?: typeof fetch;
}

export interface RpcClient {
  /** Awaited call — expects a matching JSON-RPC response. */
  request(method: string, params: unknown): Promise<unknown>;
  /** Fire-and-forget JSON-RPC notification (no `id`, no response awaited). */
  notify(method: string, params: unknown): Promise<void>;
}

let nextId = 1;

/** A signed HTTP JSON-RPC client. The injectable `fetch` lets callers substitute a different transport, e.g. an in-process handler for tests. */
export function createRpcClient(options: RpcClientOptions): RpcClient {
  const doFetch = options.fetch ?? fetch;
  const timeoutMs = options.timeoutMs ?? 10_000;

  async function send(
    method: string,
    params: unknown,
    id: number | undefined,
  ): Promise<unknown> {
    const encodedParams = await encodeBuffers(params);
    const body = JSON.stringify({
      jsonrpc: "2.0",
      ...(id !== undefined ? { id } : {}),
      method,
      params: encodedParams,
    });
    const timestamp = String(Date.now());
    const signature = sign(body, timestamp, options.secret);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    let response: Response;
    try {
      response = await doFetch(options.url, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          [SIGNATURE_HEADER]: signature,
          [TIMESTAMP_HEADER]: timestamp,
        },
        body,
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    if (id === undefined) return undefined; // notification: no response expected

    const text = await response.text();
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      throw new Error(
        `chat-adapter-remote: non-JSON response (HTTP ${response.status}): ${text.slice(0, 200)}`,
      );
    }

    const envelope = JsonRpcResponseSchema.parse(parsed);
    if (isErrorResponse(envelope)) throw deserializeError(envelope.error);
    return decodeBuffers(envelope.result);
  }

  return {
    request: (method, params) => send(method, params, nextId++),
    notify: (method, params) =>
      send(method, params, undefined).then(() => undefined),
  };
}
