import type { Adapter, AdapterPostableMessage, Logger } from "chat";

import { decodeBuffers, encodeBuffers } from "../rpc/buffers";
import { verifyAndParse, type DispatchOptions } from "../rpc/dispatch";
import { RpcErrorCode, serializeError } from "../rpc/errors";
import { OUTBOUND_CALLS } from "../rpc/methods";
import { RemoteChat, type RemoteChatOptions } from "./remote-chat";

export interface ServeAdapterOptions extends DispatchOptions {
  /** URL of the consumer's inbound endpoint (RemoteAdapter.handleWebhook). */
  consumerUrl: string;
  timeoutMs?: number;
  logger?: Logger;
  /** Override the fetch implementation RemoteChat uses to forward inbound events. Mainly for tests. */
  fetch?: typeof fetch;
}

function jsonRpcSuccess(id: string | number | null, result: unknown): Response {
  return Response.json({ jsonrpc: "2.0", id, result });
}

function jsonRpcError(
  id: string | number | null,
  code: number,
  message: string,
  data?: unknown,
): Response {
  return Response.json({ jsonrpc: "2.0", id, error: { code, message, data } });
}

/**
 * Wraps a real, unmodified `Adapter` instance and exposes it over signed
 * HTTP JSON-RPC. Hands the real adapter a fake `ChatInstance` (`RemoteChat`)
 * so its own `initialize()`/`processMessage()` calls forward back to the
 * consumer instead of running local handlers.
 */
export class AdapterHost<TThreadId = unknown, TRawMessage = unknown> {
  private readonly remoteChat: RemoteChat;
  private readonly initializePromise: Promise<void>;

  constructor(
    private readonly adapter: Adapter<TThreadId, TRawMessage>,
    private readonly options: ServeAdapterOptions,
  ) {
    const remoteChatOptions: RemoteChatOptions = {
      consumerUrl: options.consumerUrl,
      secret: options.secret,
      timeoutMs: options.timeoutMs,
      logger: options.logger,
      fetch: options.fetch,
    };
    this.remoteChat = new RemoteChat(remoteChatOptions);
    this.initializePromise = this.adapter.initialize(this.remoteChat);
  }

  /** Resolves once the wrapped adapter's own initialize() has completed. */
  get ready(): Promise<void> {
    return this.initializePromise;
  }

  async handleRequest(request: Request): Promise<Response> {
    const verified = await verifyAndParse(request, this.options);
    if (!verified.ok) return verified.response;

    const id = verified.id ?? null;
    if (typeof id !== "string" && typeof id !== "number") {
      return jsonRpcError(
        null,
        RpcErrorCode.INVALID_REQUEST,
        "Outbound calls must carry an id",
      );
    }

    try {
      await this.initializePromise;
    } catch (error) {
      const wireError = serializeError(error);
      return jsonRpcError(
        id,
        wireError.code,
        wireError.message,
        wireError.data,
      );
    }

    const call = OUTBOUND_CALLS.safeParse({
      method: verified.method,
      id,
      params: verified.params,
    });
    if (!call.success) {
      return jsonRpcError(
        id,
        RpcErrorCode.METHOD_NOT_FOUND,
        `Unknown or invalid outbound method: ${verified.method}`,
      );
    }

    try {
      const result = await this.dispatch(call.data);
      return jsonRpcSuccess(id, await encodeBuffers(result));
    } catch (error) {
      const wireError = serializeError(error);
      return jsonRpcError(
        id,
        wireError.code,
        wireError.message,
        wireError.data,
      );
    }
  }

  private async dispatch(
    call: ReturnType<typeof OUTBOUND_CALLS.parse>,
  ): Promise<unknown> {
    const adapter = this.adapter;
    // params was already validated against the schema above; the `as` casts
    // below just recover the per-method types Zod already enforced.
    const params = decodeBuffers(call.params) as unknown[];
    switch (call.method) {
      case "__handshake":
        return {
          name: adapter.name,
          userName: adapter.userName,
          botUserId: adapter.botUserId,
        };
      case "postMessage":
        return adapter.postMessage(
          params[0] as string,
          params[1] as AdapterPostableMessage,
        );
      case "editMessage":
        return adapter.editMessage(
          params[0] as string,
          params[1] as string,
          params[2] as AdapterPostableMessage,
        );
      case "deleteMessage":
        return adapter.deleteMessage(params[0] as string, params[1] as string);
      case "addReaction":
        return adapter.addReaction(
          params[0] as string,
          params[1] as string,
          params[2] as string,
        );
      case "removeReaction":
        return adapter.removeReaction(
          params[0] as string,
          params[1] as string,
          params[2] as string,
        );
      case "fetchMessages":
        return adapter.fetchMessages(
          params[0] as string,
          (params[1] ?? undefined) as Parameters<
            typeof adapter.fetchMessages
          >[1],
        );
      case "fetchThread":
        return adapter.fetchThread(params[0] as string);
      case "startTyping":
        return adapter.startTyping(
          params[0] as string,
          (params[1] ?? undefined) as string | undefined,
          (params[2] ?? undefined) as Parameters<typeof adapter.startTyping>[2],
        );
      case "disconnect":
        if (typeof adapter.disconnect !== "function") {
          const error = new Error(
            `Adapter "${adapter.name}" does not implement disconnect()`,
          );
          error.name = "MethodNotImplementedError";
          throw error;
        }
        return adapter.disconnect();
    }
  }
}

export function serveAdapter<TThreadId = unknown, TRawMessage = unknown>(
  adapter: Adapter<TThreadId, TRawMessage>,
  options: ServeAdapterOptions,
): AdapterHost<TThreadId, TRawMessage> {
  return new AdapterHost(adapter, options);
}
