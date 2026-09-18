import type {
  Adapter,
  AdapterPostableMessage,
  FetchResult,
  Logger,
  Message,
} from "chat";

import { decode, encode } from "../rpc/codec";
import { verifyRequest, type DispatchOptions } from "../rpc/dispatch";
import { RpcErrorCode, serializeError } from "../rpc/errors";
import { serializeMessage } from "../rpc/message-wire";
import { OUTBOUND_CALLS, PROTOCOL_VERSION } from "../rpc/methods";
import { createRemoteChat } from "./remote-chat";
import type { FetchLike } from "../types";

export interface ServeAdapterOptions extends DispatchOptions {
  /** URL of the consumer's inbound endpoint. */
  consumerUrl: string;
  timeoutMs?: number;
  logger?: Logger;
  fetch?: FetchLike;
}

function success(id: string | number, result: unknown): Response {
  // JSON.stringify drops an undefined property, which would leave the
  // response without the `result` member the envelope requires.
  return Response.json({ jsonrpc: "2.0", id, result: result ?? null });
}

function failure(
  id: string | number | null,
  code: number,
  message: string,
  data?: unknown,
): Response {
  return Response.json({ jsonrpc: "2.0", id, error: { code, message, data } });
}

/**
 * Wraps a real adapter and exposes it over signed HTTP JSON-RPC, handing it a
 * stand-in `ChatInstance` whose calls forward back to the consumer.
 */
export class AdapterHost<TThreadId = unknown, TRawMessage = unknown> {
  private readonly initialized: Promise<void>;

  constructor(
    private readonly adapter: Adapter<TThreadId, TRawMessage>,
    private readonly options: ServeAdapterOptions,
  ) {
    this.initialized = adapter.initialize(
      createRemoteChat({
        consumerUrl: options.consumerUrl,
        secret: options.secret,
        timeoutMs: options.timeoutMs,
        logger: options.logger,
        fetch: options.fetch,
      }),
    );
    // `ready` still surfaces the failure; this only stops an unobserved
    // rejection from killing the process before a request arrives.
    this.initialized.catch(() => {});
  }

  /** Resolves once the wrapped adapter's own initialize() has completed. */
  get ready(): Promise<void> {
    return this.initialized;
  }

  async handleRequest(request: Request): Promise<Response> {
    const verified = await verifyRequest(request, this.options);
    if (!verified.ok) return verified.response;

    const id = verified.id;
    if (typeof id !== "string" && typeof id !== "number") {
      return failure(
        null,
        RpcErrorCode.INVALID_REQUEST,
        "Outbound calls must carry an id",
      );
    }

    const call = OUTBOUND_CALLS.safeParse({
      method: verified.method,
      id,
      params: verified.params,
    });
    if (!call.success) {
      return failure(
        id,
        RpcErrorCode.METHOD_NOT_FOUND,
        `Unknown or invalid outbound method: ${verified.method}`,
      );
    }

    try {
      await this.initialized;
      return success(id, await encode(await this.dispatch(call.data)));
    } catch (error) {
      const wire = serializeError(error);
      return failure(id, wire.code, wire.message, wire.data);
    }
  }

  private async dispatch(
    call: ReturnType<typeof OUTBOUND_CALLS.parse>,
  ): Promise<unknown> {
    const adapter = this.adapter;
    const params = decode(call.params) as unknown[];
    const threadId = params[0] as string;

    switch (call.method) {
      case "__handshake":
        return {
          protocolVersion: PROTOCOL_VERSION,
          name: adapter.name,
          userName: adapter.userName,
          botUserId: adapter.botUserId,
          lockScope: adapter.lockScope,
          persistThreadHistory:
            adapter.persistThreadHistory ?? adapter.persistMessageHistory,
          supportsTurnCancellation: adapter.supportsTurnCancellation,
        };
      case "postMessage":
        return adapter.postMessage(
          threadId,
          params[1] as AdapterPostableMessage,
        );
      case "editMessage":
        return adapter.editMessage(
          threadId,
          params[1] as string,
          params[2] as AdapterPostableMessage,
        );
      case "deleteMessage":
        return adapter.deleteMessage(threadId, params[1] as string);
      case "addReaction":
        return adapter.addReaction(
          threadId,
          params[1] as string,
          params[2] as string,
        );
      case "removeReaction":
        return adapter.removeReaction(
          threadId,
          params[1] as string,
          params[2] as string,
        );
      case "fetchMessages":
        return this.serializeFetchResult(
          await adapter.fetchMessages(
            threadId,
            (params[1] ?? undefined) as Parameters<
              typeof adapter.fetchMessages
            >[1],
          ),
        );
      case "fetchThread":
        return adapter.fetchThread(threadId);
      case "startTyping":
        return adapter.startTyping(
          threadId,
          (params[1] ?? undefined) as string | undefined,
          (params[2] ?? undefined) as Parameters<typeof adapter.startTyping>[2],
        );
      case "disconnect":
        if (typeof adapter.disconnect !== "function") {
          throw new Error(
            `Adapter "${adapter.name}" does not implement disconnect()`,
          );
        }
        return adapter.disconnect();
    }
  }

  /** Real adapters return live Message instances here; they need the same treatment as inbound messages. */
  private async serializeFetchResult(
    result: FetchResult<TRawMessage>,
  ): Promise<unknown> {
    return {
      ...result,
      messages: await Promise.all(
        result.messages.map((message) => serializeMessage(message as Message)),
      ),
    };
  }
}

export function serveAdapter<TThreadId = unknown, TRawMessage = unknown>(
  adapter: Adapter<TThreadId, TRawMessage>,
  options: ServeAdapterOptions,
): AdapterHost<TThreadId, TRawMessage> {
  return new AdapterHost(adapter, options);
}
