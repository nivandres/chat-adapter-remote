import type {
  Adapter,
  AdapterPostableMessage,
  AgentSessionStatus,
  Attachment,
  FetchOptions,
  FetchResult,
  ListThreadsOptions,
  ListThreadsResult,
  Logger,
  Message,
  ModalElement,
  ScheduledMessage,
  StreamOptions,
  WebhookOptions,
} from "chat";
import { ConsoleLogger } from "chat";

import { decode, encode } from "../rpc/codec";
import { verifyRequest, type DispatchOptions } from "../rpc/dispatch";
import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  serializeError,
} from "../rpc/errors";
import { deserializeMessage, serializeMessage } from "../rpc/message-wire";
import {
  OPTIONAL_CAPABILITIES,
  OUTBOUND_CALLS,
  PROTOCOL_VERSION,
  type OptionalCapability,
} from "../rpc/methods";
import { createReplayGuard } from "../rpc/security";
import type { FetchLike } from "../types";
import type { LogLevel } from "./logger-bridge";
import { createRemoteChat, type HostErrorHandler } from "./remote-chat";
import { StreamRegistry } from "./streams";

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
  /** Streams abandoned by the consumer are dropped after this long. Default 5 minutes. */
  streamTtlMs?: number;
}

function success(id: string | number, result: unknown): Response {
  // JSON.stringify drops an undefined property, leaving no `result` member.
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

/** Serves a real adapter over signed HTTP JSON-RPC, handing it a stand-in `ChatInstance` that forwards back to the consumer. */
export class AdapterHost<TThreadId = unknown, TRawMessage = unknown> {
  /** Bound, so it can be handed straight to any Fetch-API router. */
  readonly fetch = (request: Request): Promise<Response> =>
    this.handleRequest(request);

  private readonly chat;
  private readonly logger: Logger;
  private readonly dispatchOptions: DispatchOptions;
  private readonly capabilities: OptionalCapability[];
  private readonly streams: StreamRegistry;
  private readonly scheduled = new Map<string, ScheduledMessage<TRawMessage>>();
  private starting?: Promise<void>;
  private stopped = false;

  constructor(
    private readonly adapter: Adapter<TThreadId, TRawMessage>,
    private readonly options: ServeAdapterOptions,
  ) {
    this.logger =
      options.logger ?? new ConsoleLogger("info", "chat-adapter-remote");

    this.dispatchOptions = {
      ...options,
      replayGuard: options.replayGuard ?? createReplayGuard(),
    };
    this.capabilities = OPTIONAL_CAPABILITIES.filter(
      (name) => typeof adapter[name] === "function",
    );
    this.streams = new StreamRegistry({ ttlMs: options.streamTtlMs });

    this.chat = createRemoteChat({
      consumerUrl: options.consumerUrl,
      secret: options.secret,
      timeoutMs: options.timeoutMs,
      logger: this.logger,
      fetch: options.fetch,
      onError: options.onError,
      userName: adapter.userName,
      logForwardLevel: options.logForwardLevel,
      maxConcurrentForwards: options.maxConcurrentForwards,
    });

    // `start()` and `ready` still surface the failure; this only keeps an
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
    this.streams.clear();
    this.scheduled.clear();
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

  /** For adapters driven by platform webhooks; waits for startup so an early delivery cannot reach a half-initialized adapter. */
  async handleWebhook(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    await this.start();
    return this.adapter.handleWebhook(request, options);
  }

  async handleRequest(request: Request): Promise<Response> {
    const verified = await verifyRequest(request, this.dispatchOptions);
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

  /** Bound accessor for an optional member the wrapped adapter may not have. */
  private required<K extends OptionalCapability>(
    name: K,
  ): NonNullable<Adapter<TThreadId, TRawMessage>[K]> {
    const method = this.adapter[name];
    if (typeof method !== "function") {
      throw new RemoteAdapterRpcError(
        RpcErrorCode.METHOD_NOT_IMPLEMENTED,
        `Adapter "${this.adapter.name}" does not implement ${name}()`,
      );
    }
    return (method as (...args: unknown[]) => unknown).bind(
      this.adapter,
    ) as NonNullable<Adapter<TThreadId, TRawMessage>[K]>;
  }

  private async dispatch(
    call: ReturnType<typeof OUTBOUND_CALLS.parse>,
  ): Promise<unknown> {
    const adapter = this.adapter;
    const params = decode(call.params) as unknown[];
    const first = params[0] as string;

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
          capabilities: this.capabilities,
        };
      case "postMessage":
        return adapter.postMessage(first, params[1] as AdapterPostableMessage);
      case "editMessage":
        return adapter.editMessage(
          first,
          params[1] as string,
          params[2] as AdapterPostableMessage,
        );
      case "deleteMessage":
        return adapter.deleteMessage(first, params[1] as string);
      case "addReaction":
        return adapter.addReaction(
          first,
          params[1] as string,
          params[2] as string,
        );
      case "removeReaction":
        return adapter.removeReaction(
          first,
          params[1] as string,
          params[2] as string,
        );
      case "fetchMessages":
        return this.serializeFetchResult(
          await adapter.fetchMessages(
            first,
            (params[1] ?? undefined) as FetchOptions | undefined,
          ),
        );
      case "fetchThread":
        return adapter.fetchThread(first);
      case "startTyping":
        return adapter.startTyping(
          first,
          (params[1] ?? undefined) as string | undefined,
          (params[2] ?? undefined) as Parameters<typeof adapter.startTyping>[2],
        );
      case "disconnect":
        return this.required("disconnect")();
      case "reply":
        return this.required("reply")(
          first,
          params[1] as string,
          params[2] as AdapterPostableMessage,
        );
      case "endTyping":
        return this.required("endTyping")(
          first,
          (params[1] ?? undefined) as AgentSessionStatus | undefined,
        );
      case "markAsRead":
        return this.required("markAsRead")(
          first,
          params[1] as string,
          params[2]
            ? (deserializeMessage(params[2]) as Message<TRawMessage>)
            : undefined,
        );
      case "listThreads":
        return this.serializeListThreads(
          await this.required("listThreads")(
            first,
            (params[1] ?? undefined) as ListThreadsOptions | undefined,
          ),
        );
      case "getUser":
        return this.required("getUser")(first);
      case "postObject":
        return this.required("postObject")(
          first,
          params[1] as string,
          params[2],
        );
      case "editObject":
        return this.required("editObject")(
          first,
          params[1] as string,
          params[2] as string,
          params[3],
        );
      case "openDM":
        return this.required("openDM")(first);
      case "openModal":
        return this.required("openModal")(
          first,
          params[1] as ModalElement,
          (params[2] ?? undefined) as string | undefined,
        );
      case "postEphemeral":
        return this.required("postEphemeral")(
          first,
          params[1] as string,
          params[2] as AdapterPostableMessage,
        );
      case "postChannelMessage":
        return this.required("postChannelMessage")(
          first,
          params[1] as AdapterPostableMessage,
        );
      case "fetchMessage": {
        const message = await this.required("fetchMessage")(
          first,
          params[1] as string,
        );
        return message ? serializeMessage(message as Message) : null;
      }
      case "fetchChannelInfo":
        return this.required("fetchChannelInfo")(first);
      case "fetchChannelMessages":
        return this.serializeFetchResult(
          await this.required("fetchChannelMessages")(
            first,
            (params[1] ?? undefined) as FetchOptions | undefined,
          ),
        );
      case "fetchSubject":
        return this.required("fetchSubject")(params[0] as TRawMessage);
      case "onThreadSubscribe":
        return this.required("onThreadSubscribe")(first);
      case "streamStart":
        return this.streams.open((chunks) =>
          this.required("stream")(
            first,
            chunks,
            (params[1] ?? undefined) as StreamOptions | undefined,
          ),
        );
      case "streamPush":
        return this.streams.push(
          first,
          params[1] as Parameters<StreamRegistry["push"]>[1],
        );
      case "streamEnd":
        return this.streams.end(
          first,
          (params[1] ?? undefined) as boolean | undefined,
        );
      case "scheduleMessage":
        return this.schedule(
          first,
          params[1] as AdapterPostableMessage,
          params[2] as { postAt: Date },
        );
      case "cancelScheduledMessage": {
        const scheduled = this.scheduled.get(first);
        if (!scheduled) {
          throw new RemoteAdapterRpcError(
            RpcErrorCode.NOT_CANCELLABLE,
            `chat-adapter-remote: scheduled message ${first} is unknown or already due`,
          );
        }
        this.scheduled.delete(first);
        return scheduled.cancel();
      }
      case "rehydrateAttachment": {
        const rebuilt = this.required("rehydrateAttachment")(
          params[0] as Attachment,
        );
        return rebuilt.fetchData ? rebuilt.fetchData() : null;
      }
    }
  }

  /** The returned `cancel()` is a live closure, so the object stays here and is reached by id. Entries past their delivery time are dropped. */
  private async schedule(
    threadId: string,
    message: AdapterPostableMessage,
    options: { postAt: Date },
  ): Promise<unknown> {
    const now = Date.now();
    for (const [id, entry] of this.scheduled) {
      if (entry.postAt.getTime() > now) continue;
      this.scheduled.delete(id);
    }

    const result = await this.required("scheduleMessage")(
      threadId,
      message,
      options,
    );
    this.scheduled.set(result.scheduledMessageId, result);
    return {
      scheduledMessageId: result.scheduledMessageId,
      channelId: result.channelId,
      postAt: result.postAt,
      raw: result.raw,
    };
  }

  /** Live Message instances need the same treatment as inbound ones. */
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

  private async serializeListThreads(
    result: ListThreadsResult<TRawMessage>,
  ): Promise<unknown> {
    return {
      ...result,
      threads: await Promise.all(
        result.threads.map(async (thread) => ({
          ...thread,
          rootMessage: await serializeMessage(thread.rootMessage as Message),
        })),
      ),
    };
  }
}

/** Serves the adapter and starts it immediately. */
export function serveAdapter<TThreadId = unknown, TRawMessage = unknown>(
  adapter: Adapter<TThreadId, TRawMessage>,
  options: ServeAdapterOptions,
): AdapterHost<TThreadId, TRawMessage> {
  return new AdapterHost(adapter, options);
}

/** Same host, left stopped: call `start()` and `stop()` yourself. */
export function createAdapterHost<TThreadId = unknown, TRawMessage = unknown>(
  adapter: Adapter<TThreadId, TRawMessage>,
  options: ServeAdapterOptions,
): AdapterHost<TThreadId, TRawMessage> {
  return new AdapterHost(adapter, { ...options, autoStart: false });
}
