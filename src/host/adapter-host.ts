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
  StateAdapter,
  StreamOptions,
  WebhookOptions,
} from "chat";
import { ValidationError } from "@chat-adapter/shared";
import { ConsoleLogger } from "chat";

import { decode, encode } from "../rpc/codec";
import { verifyRequest, type DispatchOptions } from "../rpc/dispatch";
import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  serializeError,
} from "../rpc/errors";
import {
  deserializeMessage,
  serializeMessage,
  type AttachmentPolicy,
} from "../rpc/message-wire";
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
import {
  AttachmentBudget,
  AttachmentRegistry,
  DEFAULT_ATTACHMENT_BUDGET,
  type InlineAttachments,
} from "./attachments";
import { resolveCustomMethods } from "./custom-methods";
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
  /** Messages allowed to queue behind those. Default 1000. */
  maxQueuedForwards?: number;
  /** Initialize during construction. Default true; set false to control startup with start(). */
  autoStart?: boolean;
  /** Streams abandoned by the consumer are dropped after this long. Default 5 minutes. */
  streamTtlMs?: number;
  /** How long the adapter may take to start streaming. Default 10 seconds. */
  streamStartTimeoutMs?: number;
  /**
   * Whether attachment bytes travel inside the message. Default `"auto"`,
   * which inlines them while the body budget allows and otherwise leaves them
   * here for the consumer to fetch by id.
   */
  inlineAttachments?: InlineAttachments;
  /** How long an attachment the consumer never fetched is kept. Default 5 minutes. */
  attachmentTtlMs?: number;
  /**
   * Adapter methods outside the `Adapter` interface that the consumer may
   * call. `true` exposes the adapter's own public methods.
   */
  customMethods?: string[] | true;
  /**
   * Backs the `getState()` the wrapped adapter uses for its own persistence.
   * Without one, every operation reaches the consumer's store instead.
   */
  state?: StateAdapter;
  /**
   * Keeps a rejection thrown inside the adapter's own event loop from ending
   * the process. Those surface nowhere else: they belong to no request, so
   * nothing here can wrap them. Default true.
   */
  catchUnhandledRejections?: boolean;
}

/** Both are deployment mistakes: failing here beats failing on the first request. */
function requireOption(value: string | undefined, option: string): string {
  if (!value) {
    const variable = option === "secret" ? "SECRET" : "CONSUMER_URL";
    throw new ValidationError(
      "remote",
      `"${option}" is required; pass it or set CHAT_ADAPTER_REMOTE_${variable}`,
    );
  }
  return value;
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
  private readonly customMethods: string[];
  private onUnhandled?: (error: unknown) => void;
  private readonly streams: StreamRegistry;
  private readonly attachments: AttachmentRegistry;
  private readonly scheduled = new Map<string, ScheduledMessage<TRawMessage>>();
  private starting?: Promise<void>;
  private stopped = false;

  constructor(
    private readonly adapter: Adapter<TThreadId, TRawMessage>,
    private readonly options: ServeAdapterOptions,
  ) {
    const consumerUrl = requireOption(
      options.consumerUrl ?? process.env.CHAT_ADAPTER_REMOTE_CONSUMER_URL,
      "consumerUrl",
    );
    const secret = requireOption(
      options.secret ?? process.env.CHAT_ADAPTER_REMOTE_SECRET,
      "secret",
    );
    this.logger =
      options.logger ?? new ConsoleLogger("info", "chat-adapter-remote");

    this.dispatchOptions = {
      ...options,
      secret,
      replayGuard: options.replayGuard ?? createReplayGuard(),
    };
    this.capabilities = OPTIONAL_CAPABILITIES.filter(
      (name) => typeof adapter[name] === "function",
    );
    this.customMethods = resolveCustomMethods(adapter, options.customMethods);
    this.streams = new StreamRegistry({
      ttlMs: options.streamTtlMs,
      startTimeoutMs: options.streamStartTimeoutMs,
    });
    this.attachments = new AttachmentRegistry(
      options.attachmentTtlMs ?? 300_000,
    );

    this.chat = createRemoteChat({
      consumerUrl,
      secret,
      timeoutMs: options.timeoutMs,
      logger: this.logger,
      fetch: options.fetch,
      onError: options.onError,
      userName: adapter.userName,
      attachments: () => this.attachmentPolicy(),
      state: options.state,
      logForwardLevel: options.logForwardLevel,
      maxConcurrentForwards: options.maxConcurrentForwards,
      maxQueuedForwards: options.maxQueuedForwards,
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

  private guardProcess(): void {
    if (this.options.catchUnhandledRejections === false || this.onUnhandled) {
      return;
    }
    this.onUnhandled = (error: unknown) => {
      this.logger.error("unhandled rejection inside the adapter", { error });
      this.options.onError?.(error, { phase: "adapter" });
    };
    process.on("unhandledRejection", this.onUnhandled);
  }

  private async initialize(): Promise<void> {
    this.guardProcess();
    try {
      // Handed to the host, so its lifecycle belongs to the host.
      await this.options.state?.connect();
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
    if (this.onUnhandled) {
      process.off("unhandledRejection", this.onUnhandled);
      this.onUnhandled = undefined;
    }
    this.streams.clear();
    this.scheduled.clear();
    this.attachments.clear();
    try {
      await this.adapter.disconnect?.();
      await this.options.state?.disconnect();
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
    // Nothing here may throw past this point: an unhandled rejection in a
    // request handler would take down the process holding the connection.
    try {
      return await this.route(request);
    } catch (error) {
      this.logger.error("request could not be handled", { error });
      this.options.onError?.(error, { phase: "dispatch" });
      const wire = serializeError(error);
      return failure(null, wire.code, wire.message, wire.data);
    }
  }

  private async route(request: Request): Promise<Response> {
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
      // Logged in full here because the wire form is deliberately sanitised;
      // without this an adapter failure leaves no trace on either side.
      this.logger.error(`${call.data.method} failed`, { error });
      this.options.onError?.(error, { phase: "dispatch" });
      const wire = serializeError(error);
      return failure(id, wire.code, wire.message, wire.data);
    }
  }

  /** A fresh budget per message, since the limit is on one body. */
  private attachmentPolicy(): AttachmentPolicy {
    return {
      budget: new AttachmentBudget(
        this.options.inlineAttachments ?? "auto",
        this.options.maxBodyBytes ?? DEFAULT_ATTACHMENT_BUDGET,
      ),
      registry: this.attachments,
      rehydratable: typeof this.adapter.rehydrateAttachment === "function",
    };
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
          attachments: () => this.attachmentPolicy(),
          botUserId: adapter.botUserId,
          lockScope: adapter.lockScope,
          persistThreadHistory:
            adapter.persistThreadHistory ?? adapter.persistMessageHistory,
          supportsTurnCancellation: adapter.supportsTurnCancellation,
          capabilities: this.capabilities,
          customMethods: this.customMethods,
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
        return message
          ? serializeMessage(message as Message, this.attachmentPolicy())
          : null;
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
      case "custom": {
        if (!this.customMethods.includes(first)) {
          throw new RemoteAdapterRpcError(
            RpcErrorCode.METHOD_NOT_IMPLEMENTED,
            `chat-adapter-remote: "${first}" is not exposed by this host`,
          );
        }
        const method = this.adapter[first as keyof Adapter] as unknown as (
          ...args: unknown[]
        ) => unknown;
        return method.apply(this.adapter, params[1] as unknown[]);
      }
      case "fetchAttachment": {
        const bytes = this.attachments.read(first);
        if (!bytes) {
          throw new RemoteAdapterRpcError(
            RpcErrorCode.STREAM_NOT_FOUND,
            `chat-adapter-remote: attachment ${first} is unknown or has expired`,
          );
        }
        return bytes;
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
    // One budget for the whole response: it is one body, however many
    // messages share it.
    const policy = this.attachmentPolicy();
    return {
      ...result,
      messages: await Promise.all(
        result.messages.map((message) =>
          serializeMessage(message as Message, policy),
        ),
      ),
    };
  }

  private async serializeListThreads(
    result: ListThreadsResult<TRawMessage>,
  ): Promise<unknown> {
    const policy = this.attachmentPolicy();
    return {
      ...result,
      threads: await Promise.all(
        result.threads.map(async (thread) => ({
          ...thread,
          rootMessage: await serializeMessage(
            thread.rootMessage as Message,
            policy,
          ),
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
