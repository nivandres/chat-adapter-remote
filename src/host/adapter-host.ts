import type {
  Adapter,
  AdapterPostableMessage,
  FetchResult,
  Logger,
  Message,
  WebhookOptions,
} from "chat";

import { decode, encode } from "../rpc/codec";
import { verifyRequest, type DispatchOptions } from "../rpc/dispatch";
import { RpcErrorCode, serializeError } from "../rpc/errors";
import { serializeMessage } from "../rpc/message-wire";
import { OUTBOUND_CALLS, PROTOCOL_VERSION } from "../rpc/methods";
import type { FetchLike } from "../types";
import type { LogLevel } from "./logger-bridge";
import { createRemoteChat, type HostErrorHandler } from "./remote-chat";

export interface ServeAdapterOptions extends DispatchOptions {
  /** URL of the consumer's inbound endpoint. */
  consumerUrl: string;
  timeoutMs?: number;
  logger?: Logger;
  fetch?: FetchLike;
  /** Receives failures from every phase, so they can be routed to alerting. */
  onError?: HostErrorHandler;
  /** Called once the wrapped adapter has initialized. */
  onReady?: () => void;
  /** Lines below this level stay on the host instead of crossing the wire. Default "info". */
  logForwardLevel?: LogLevel;
  /** Inbound messages forwarded at once. Default 8. */
  maxConcurrentForwards?: number;
  /** Initialize during construction. Default true; set false to control startup with start(). */
  autoStart?: boolean;
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
  private readonly chat;
  private starting?: Promise<void>;
  private stopped = false;

  constructor(
    private readonly adapter: Adapter<TThreadId, TRawMessage>,
    private readonly options: ServeAdapterOptions,
  ) {
    this.chat = createRemoteChat({
      consumerUrl: options.consumerUrl,
      secret: options.secret,
      timeoutMs: options.timeoutMs,
      logger: options.logger,
      fetch: options.fetch,
      onError: options.onError,
      logForwardLevel: options.logForwardLevel,
      maxConcurrentForwards: options.maxConcurrentForwards,
    });

    // `start()` and `ready` still surface the failure; this only stops an
    // unobserved rejection from killing the process before a request arrives.
    if (options.autoStart !== false) this.start().catch(() => {});
  }

  /** Initializes the wrapped adapter. Idempotent, and rejects loudly on failure. */
  start(): Promise<void> {
    this.starting ??= this.initialize();
    return this.starting;
  }

  private async initialize(): Promise<void> {
    try {
      await this.adapter.initialize(this.chat);
      this.options.onReady?.();
    } catch (error) {
      this.options.onError?.(error, { phase: "initialize" });
      throw error;
    }
  }

  /** Closes the adapter's connection and stops serving. Wire this to SIGTERM. */
  async stop(): Promise<void> {
    this.stopped = true;
    try {
      await this.adapter.disconnect?.();
    } catch (error) {
      this.options.onError?.(error, { phase: "shutdown" });
      throw error;
    }
  }

  /** Resolves once the wrapped adapter's own initialize() has completed. */
  get ready(): Promise<void> {
    return this.start();
  }

  /**
   * Entry point for adapters driven by platform webhooks rather than a socket.
   * Mount alongside handleRequest; it waits for startup so an early delivery
   * cannot reach a half-initialized adapter.
   */
  async handlePlatformWebhook(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    await this.start();
    return this.adapter.handleWebhook(request, options);
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
    if (this.stopped) {
      return failure(id, RpcErrorCode.INTERNAL_ERROR, "Host is stopped");
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
      await this.start();
      return success(id, await encode(await this.dispatch(call.data)));
    } catch (error) {
      this.options.onError?.(error, { phase: "dispatch" });
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
