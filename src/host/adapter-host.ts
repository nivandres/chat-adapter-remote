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
import { createRpcClient, type RpcClient } from "../rpc/transport";
import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  StreamDiscardedError,
  isUndelivered,
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
  type StreamEnding,
} from "../rpc/methods";
import { createReplayGuard } from "../rpc/security";
import type { FetchLike, HostEvent } from "../types";
import type { LogLevel } from "./logger-bridge";
import { createRemoteChat, type HostErrorHandler } from "./remote-chat";
import {
  AttachmentBudget,
  AttachmentRegistry,
  DEFAULT_ATTACHMENT_BUDGET,
  type InlineAttachments,
} from "./attachments";
import { resolveCustomMethods } from "./custom-methods";
import {
  Redelivery,
  createMemoryForwardQueue,
  type DroppedForwardHandler,
  type ForwardQueue,
  type ForwardRetryOptions,
} from "./delivery";
import {
  createStreamer,
  resolveStreamMode,
  type StreamModeOptions,
} from "./stream-modes";
import { guardUnhandledRejections } from "./process-guard";
import { StreamRegistry } from "./streams";

export interface ServeAdapterOptions extends DispatchOptions {
  /** URL of the consumer's inbound endpoint. */
  consumerUrl: string;
  timeoutMs?: number;
  logger?: Logger;
  fetch?: FetchLike;
  /** Receives failures from every phase. */
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
  /** Inline attachment bytes. Default `"auto"`: inline while they fit. */
  inlineAttachments?: InlineAttachments;
  /** How long an attachment the consumer never fetched is kept. Default 5 minutes. */
  attachmentTtlMs?: number;
  /** Methods outside the `Adapter` interface to expose; `true` for the adapter's own. */
  customMethods?: string[] | true;
  /** Store for the adapter's own `getState()`. Default: the consumer's. */
  state?: StateAdapter;
  /** Keep rejections from the adapter's event loop from ending the process. Default true. */
  catchUnhandledRejections?: boolean;
  /** Resend forwards the consumer never received. Default every minute for 24 hours; `false` disables. */
  forwardRetry?: ForwardRetryOptions | false;
  /** Where those forwards wait. Default in memory, capped at 1000. */
  forwardQueue?: ForwardQueue;
  /** Called for a forward that will not be delivered, with why. */
  onDropped?: DroppedForwardHandler;
  /** How replies stream; see `StreamModeOptions`. */
  stream?: StreamModeOptions;
}

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

/** Serves a real adapter over signed HTTP JSON-RPC. */
export class AdapterHost<TThreadId = unknown, TRawMessage = unknown> {
  /** Bound, so it can be handed straight to any Fetch-API router. */
  readonly fetch = (request: Request): Promise<Response> =>
    this.handleRequest(request);

  private readonly chat;
  private readonly logger: Logger;
  private readonly dispatchOptions: DispatchOptions;
  private readonly capabilities: OptionalCapability[];
  private readonly customMethods: string[];
  private unguard?: () => void;
  private readonly redelivery?: Redelivery;
  private readonly toConsumer: RpcClient;
  private readonly streams: StreamRegistry;
  private readonly streamer?: ReturnType<typeof createStreamer>;
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
    const streamMode = resolveStreamMode(adapter, options.stream?.mode);
    this.streamer =
      streamMode === "off"
        ? undefined
        : createStreamer(
            adapter,
            streamMode,
            options.stream ?? {},
            this.logger,
          );
    // `buffer` and `edit` give streaming to an adapter that has none.
    this.capabilities = OPTIONAL_CAPABILITIES.filter((name) =>
      name === "stream"
        ? streamMode !== "off"
        : typeof adapter[name] === "function",
    );
    this.customMethods = resolveCustomMethods(adapter, options.customMethods);
    this.streams = new StreamRegistry({
      ttlMs: options.streamTtlMs,
      startTimeoutMs: options.streamStartTimeoutMs,
    });
    this.attachments = new AttachmentRegistry(
      options.attachmentTtlMs ?? 300_000,
    );

    const dropped: DroppedForwardHandler = (entry, reason, error) => {
      this.logger.error(
        `dropped ${entry.method} after ${entry.attempts} attempt(s): ${reason}`,
        { threadId: entry.threadId, error },
      );
      options.onDropped?.(entry, reason, error);
    };
    this.toConsumer = createRpcClient({
      url: consumerUrl,
      secret,
      timeoutMs: options.timeoutMs,
      fetch: options.fetch,
    });
    if (options.forwardRetry !== false) {
      this.redelivery = new Redelivery({
        queue:
          options.forwardQueue ??
          createMemoryForwardQueue((entry) =>
            dropped(entry, "overflow", undefined),
          ),
        ...options.forwardRetry,
        send: (method, params) => this.toConsumer.request(method, params),
        isUndelivered,
        onDropped: dropped,
      });
    }

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
      redeliver: this.redelivery
        ? (entry) => this.redelivery!.keep(entry)
        : undefined,
      dropped,
      logForwardLevel: options.logForwardLevel,
      maxConcurrentForwards: options.maxConcurrentForwards,
      maxQueuedForwards: options.maxQueuedForwards,
    });

    // Surfaced by `start()`; this only keeps it from killing the process first.
    if (options.autoStart !== false) this.start().catch(() => {});
  }

  /** Idempotent; rejects if the adapter fails to connect. */
  start(): Promise<void> {
    this.starting ??= this.initialize();
    return this.starting;
  }

  private guardProcess(): void {
    if (this.options.catchUnhandledRejections === false || this.unguard) {
      return;
    }
    this.unguard = guardUnhandledRejections((error) => {
      this.logger.error("unhandled rejection inside the adapter", { error });
      this.options.onError?.(error, { phase: "adapter" });
    });
  }

  private async initialize(): Promise<void> {
    this.guardProcess();
    this.redelivery?.start();
    try {
      await this.options.state?.connect();
      await this.adapter.initialize(this.chat);
      this.options.onReady?.();
    } catch (error) {
      this.options.onError?.(error, { phase: "initialize" });
      throw error;
    }
  }

  /** Disconnects the adapter and stops serving. */
  async stop(): Promise<void> {
    this.stopped = true;
    this.redelivery?.stop();
    this.unguard?.();
    this.unguard = undefined;
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

  /** Reaches the consumer's `onEvent`. Best effort, never retried. */
  emit(event: HostEvent): void {
    this.toConsumer.notify("hostEvent", [event]);
  }

  /** Resolves once the wrapped adapter's own initialize() has completed. */
  get ready(): Promise<void> {
    return this.start();
  }

  /** Platform webhooks; waits for startup first. */
  async handleWebhook(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    await this.start();
    return this.adapter.handleWebhook(request, options);
  }

  async handleRequest(request: Request): Promise<Response> {
    // An unhandled rejection here would end the process holding the connection.
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
      if (!(error instanceof StreamDiscardedError)) {
        // The wire form is sanitised, so this is the only full record.
        this.logger.error(`${call.data.method} failed`, { error });
        this.options.onError?.(error, { phase: "dispatch" });
      }
      const wire = serializeError(error);
      return failure(id, wire.code, wire.message, wire.data);
    }
  }

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
      case "streamStart": {
        const streamer = this.streamer;
        if (!streamer) {
          throw new RemoteAdapterRpcError(
            RpcErrorCode.METHOD_NOT_IMPLEMENTED,
            "chat-adapter-remote: streaming is off on this host",
          );
        }
        return this.streams.open((chunks, signal) =>
          streamer(first, chunks, {
            ...((params[1] ?? {}) as StreamOptions),
            signal,
          }),
        );
      }
      case "streamPush":
        return this.streams.push(
          first,
          params[1] as Parameters<StreamRegistry["push"]>[1],
        );
      case "streamEnd":
        return this.streams.end(
          first,
          (params[1] ?? undefined) as StreamEnding | undefined,
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

  /** `cancel()` is a closure, so the object stays here and is reached by id. */
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

  private async serializeFetchResult(
    result: FetchResult<TRawMessage>,
  ): Promise<unknown> {
    // One body, so one budget however many messages share it.
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
