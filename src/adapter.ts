import type {
  Adapter,
  AdapterPostableMessage,
  AgentSessionStatus,
  Attachment,
  ChannelInfo,
  ChannelVisibility,
  ChatInstance,
  EmojiValue,
  EphemeralMessage,
  FetchOptions,
  FetchResult,
  FormattedContent,
  ListThreadsOptions,
  ListThreadsResult,
  LockScope,
  Logger,
  Message,
  MessageSubject,
  ModalElement,
  RawMessage,
  ScheduledMessage,
  StreamChunk,
  StreamOptions,
  ThreadInfo,
  TypingOptions,
  UserInfo,
  WebhookOptions,
} from "chat";
import { ConsoleLogger, getEmoji } from "chat";

import { decode } from "./rpc/codec";
import { verifyRequest } from "./rpc/dispatch";
import { RpcErrorCode, serializeError } from "./rpc/errors";
import { deserializeMessage, serializeMessage } from "./rpc/message-wire";
import {
  EVENT_MESSAGE_KEYS,
  HandshakeSchema,
  INBOUND_CALLS,
  OPTIONAL_CAPABILITIES,
  PROTOCOL_VERSION,
} from "./rpc/methods";
import { createReplayGuard, type ReplayGuard } from "./rpc/security";
import { createRpcClient, type RpcClient } from "./rpc/transport";
import type { RemoteAdapterConfig } from "./types";

/** Thrown by the synchronous `Adapter` members, which cannot be answered over RPC. Chat core never calls these on an adapter it holds. */
export class RemoteAdapterUnsupportedSyncMethodError extends Error {
  constructor(method: string) {
    super(
      `chat-adapter-remote: Adapter.${method} is synchronous and cannot be bridged`,
    );
    this.name = "RemoteAdapterUnsupportedSyncMethodError";
  }
}

interface ThreadFacts {
  channelId: string;
  isDM: boolean;
  channelVisibility?: ChannelVisibility;
}

/** Every inbound call except `log`, which is a notification and answered separately. */
type InboundEvent = Exclude<
  ReturnType<typeof INBOUND_CALLS.parse>,
  { method: "log" }
>;

/**
 * A missing secret would mean accepting unsigned calls, and a missing url
 * would mean dropping every message; both are deployment mistakes worth
 * failing on at startup rather than on the first request.
 */
function requireOption(value: string | undefined, option: string): void {
  if (!value) throw new Error(`chat-adapter-remote: "${option}" is required`);
}

/** Either the host is consuming, or it already finished without reading. */
type StreamStart =
  { streamId: string } | { done: true; result: RawMessage<unknown> | null };

const DEFAULT_THREAD_CACHE = 1000;

/** `signal` is watched locally and never sent. */
function toWireStreamOptions(
  options?: StreamOptions,
): Record<string, unknown> | undefined {
  if (!options) return undefined;
  const { signal: _signal, ...rest } = options;
  return rest;
}

/** The consumer-side stand-in, registered on a real `Chat` like any other adapter. */
export class RemoteAdapter<
  TThreadId = unknown,
  TRawMessage = unknown,
> implements Adapter<TThreadId, TRawMessage> {
  readonly name: string;
  userName: string;
  botUserId?: string;
  lockScope?: LockScope;
  persistThreadHistory?: boolean;
  supportsTurnCancellation?: boolean;

  private chat: ChatInstance | null = null;
  private readonly rpc: RpcClient;
  private readonly logger: Logger;
  private readonly replayGuard: ReplayGuard;
  private readonly threads = new Map<string, ThreadFacts>();
  private readonly maxCachedThreads: number;

  constructor(private readonly config: RemoteAdapterConfig) {
    requireOption(config.url, "url");
    requireOption(config.secret, "secret");
    this.name = config.name ?? "remote";
    this.userName = config.userName ?? this.name;
    this.logger =
      config.logger ?? new ConsoleLogger("info", "chat-adapter-remote");
    this.maxCachedThreads = config.maxCachedThreads ?? DEFAULT_THREAD_CACHE;
    this.replayGuard = config.replayGuard ?? createReplayGuard();
    this.rpc = createRpcClient({
      url: config.url,
      secret: config.secret,
      timeoutMs: config.timeoutMs,
      fetch: config.fetch,
    });
  }

  async initialize(chat: ChatInstance): Promise<void> {
    this.chat = chat;
    const handshake = HandshakeSchema.parse(
      await this.rpc.request("__handshake", []),
    );
    if (handshake.protocolVersion !== PROTOCOL_VERSION) {
      throw new Error(
        `chat-adapter-remote: protocol mismatch, host speaks v${handshake.protocolVersion} and this consumer speaks v${PROTOCOL_VERSION}`,
      );
    }
    if (handshake.name !== this.name) {
      this.logger.warn(
        `host adapter is named "${handshake.name}" but this adapter is registered as "${this.name}"`,
      );
    }
    if (!this.config.userName) this.userName = handshake.userName;
    this.botUserId = handshake.botUserId;
    this.lockScope = handshake.lockScope;
    this.persistThreadHistory = handshake.persistThreadHistory;
    this.supportsTurnCancellation = handshake.supportsTurnCancellation;
    this.applyCapabilities(handshake.capabilities);
  }

  /** Chat decides what an adapter can do with `adapter.x?.()`, so anything the host did not report is removed from this instance. */
  private applyCapabilities(capabilities?: string[]): void {
    if (!capabilities) return;
    const supported = new Set(capabilities);
    for (const name of OPTIONAL_CAPABILITIES) {
      if (!supported.has(name)) Reflect.set(this, name, undefined);
    }
  }

  private rememberThread(threadId: string, facts: ThreadFacts): void {
    // Bounded: insertion order makes the oldest entry the first key.
    this.threads.delete(threadId);
    while (this.threads.size >= this.maxCachedThreads)
      this.threads.delete(this.threads.keys().next().value!);
    this.threads.set(threadId, facts);
  }

  /** Answered from facts the host sends with each inbound message. */
  channelIdFromThreadId(threadId: string): string {
    return (
      this.threads.get(threadId)?.channelId ??
      threadId.split(":").slice(0, 2).join(":")
    );
  }

  isDM(threadId: string): boolean {
    return this.threads.get(threadId)?.isDM ?? false;
  }

  getChannelVisibility(threadId: string): ChannelVisibility {
    return this.threads.get(threadId)?.channelVisibility ?? "unknown";
  }

  encodeThreadId(): never {
    throw new RemoteAdapterUnsupportedSyncMethodError("encodeThreadId");
  }
  decodeThreadId(): never {
    throw new RemoteAdapterUnsupportedSyncMethodError("decodeThreadId");
  }
  renderFormatted(_content: FormattedContent): never {
    throw new RemoteAdapterUnsupportedSyncMethodError("renderFormatted");
  }
  parseMessage(): never {
    throw new RemoteAdapterUnsupportedSyncMethodError("parseMessage");
  }

  async postMessage(
    threadId: string,
    message: AdapterPostableMessage,
  ): Promise<RawMessage<TRawMessage>> {
    return (await this.rpc.request("postMessage", [
      threadId,
      message,
    ])) as RawMessage<TRawMessage>;
  }

  async editMessage(
    threadId: string,
    messageId: string,
    message: AdapterPostableMessage,
  ): Promise<RawMessage<TRawMessage>> {
    return (await this.rpc.request("editMessage", [
      threadId,
      messageId,
      message,
    ])) as RawMessage<TRawMessage>;
  }

  async deleteMessage(threadId: string, messageId: string): Promise<void> {
    await this.rpc.request("deleteMessage", [threadId, messageId]);
  }

  async reply(
    threadId: string,
    messageId: string,
    message: AdapterPostableMessage,
  ): Promise<RawMessage<TRawMessage>> {
    return (await this.rpc.request("reply", [
      threadId,
      messageId,
      message,
    ])) as RawMessage<TRawMessage>;
  }

  async addReaction(
    threadId: string,
    messageId: string,
    emoji: EmojiValue | string,
  ): Promise<void> {
    await this.rpc.request("addReaction", [
      threadId,
      messageId,
      typeof emoji === "string" ? emoji : emoji.name,
    ]);
  }

  async removeReaction(
    threadId: string,
    messageId: string,
    emoji: EmojiValue | string,
  ): Promise<void> {
    await this.rpc.request("removeReaction", [
      threadId,
      messageId,
      typeof emoji === "string" ? emoji : emoji.name,
    ]);
  }

  async markAsRead(
    threadId: string,
    messageId: string,
    message?: Message<TRawMessage>,
  ): Promise<void> {
    await this.rpc.request("markAsRead", [
      threadId,
      messageId,
      message ? await serializeMessage(message as Message) : undefined,
    ]);
  }

  private toFetchResult(result: unknown): FetchResult<TRawMessage> {
    const { messages, nextCursor } = result as {
      messages: unknown[];
      nextCursor?: string;
    };
    return {
      messages: messages.map(
        (wire) => deserializeMessage(wire) as Message<TRawMessage>,
      ),
      nextCursor,
    };
  }

  async fetchMessages(
    threadId: string,
    options?: FetchOptions,
  ): Promise<FetchResult<TRawMessage>> {
    return this.toFetchResult(
      await this.rpc.request("fetchMessages", [threadId, options]),
    );
  }

  async fetchMessage(
    threadId: string,
    messageId: string,
  ): Promise<Message<TRawMessage> | null> {
    const wire = await this.rpc.request("fetchMessage", [threadId, messageId]);
    return wire ? (deserializeMessage(wire) as Message<TRawMessage>) : null;
  }

  async fetchThread(threadId: string): Promise<ThreadInfo> {
    const info = (await this.rpc.request("fetchThread", [
      threadId,
    ])) as ThreadInfo;
    this.rememberThread(threadId, {
      channelId: info.channelId,
      isDM: info.isDM ?? this.isDM(threadId),
      channelVisibility: this.threads.get(threadId)?.channelVisibility,
    });
    return info;
  }

  async fetchChannelInfo(channelId: string): Promise<ChannelInfo> {
    return (await this.rpc.request("fetchChannelInfo", [
      channelId,
    ])) as ChannelInfo;
  }

  async fetchChannelMessages(
    channelId: string,
    options?: FetchOptions,
  ): Promise<FetchResult<TRawMessage>> {
    return this.toFetchResult(
      await this.rpc.request("fetchChannelMessages", [channelId, options]),
    );
  }

  async fetchSubject(raw: TRawMessage): Promise<MessageSubject | null> {
    return (await this.rpc.request("fetchSubject", [
      raw,
    ])) as MessageSubject | null;
  }

  async listThreads(
    channelId: string,
    options?: ListThreadsOptions,
  ): Promise<ListThreadsResult<TRawMessage>> {
    const result = (await this.rpc.request("listThreads", [
      channelId,
      options,
    ])) as {
      threads: Array<Record<string, unknown>>;
      nextCursor?: string;
    };
    return {
      threads: result.threads.map((thread) => ({
        ...thread,
        rootMessage: deserializeMessage(thread.rootMessage),
      })) as ListThreadsResult<TRawMessage>["threads"],
      nextCursor: result.nextCursor,
    };
  }

  async getUser(userId: string): Promise<UserInfo | null> {
    return (await this.rpc.request("getUser", [userId])) as UserInfo | null;
  }

  async openDM(userId: string): Promise<string> {
    return (await this.rpc.request("openDM", [userId])) as string;
  }

  async openModal(
    triggerId: string,
    modal: ModalElement,
    contextId?: string,
  ): Promise<{ viewId: string }> {
    return (await this.rpc.request("openModal", [
      triggerId,
      modal,
      contextId,
    ])) as { viewId: string };
  }

  async postObject(
    threadId: string,
    kind: string,
    data: unknown,
  ): Promise<RawMessage<TRawMessage>> {
    return (await this.rpc.request("postObject", [
      threadId,
      kind,
      data,
    ])) as RawMessage<TRawMessage>;
  }

  async editObject(
    threadId: string,
    messageId: string,
    kind: string,
    data: unknown,
  ): Promise<RawMessage<TRawMessage>> {
    return (await this.rpc.request("editObject", [
      threadId,
      messageId,
      kind,
      data,
    ])) as RawMessage<TRawMessage>;
  }

  async postEphemeral(
    threadId: string,
    userId: string,
    message: AdapterPostableMessage,
  ): Promise<EphemeralMessage<TRawMessage>> {
    return (await this.rpc.request("postEphemeral", [
      threadId,
      userId,
      message,
    ])) as EphemeralMessage<TRawMessage>;
  }

  async postChannelMessage(
    channelId: string,
    message: AdapterPostableMessage,
  ): Promise<RawMessage<TRawMessage>> {
    return (await this.rpc.request("postChannelMessage", [
      channelId,
      message,
    ])) as RawMessage<TRawMessage>;
  }

  async onThreadSubscribe(threadId: string): Promise<void> {
    await this.rpc.request("onThreadSubscribe", [threadId]);
  }

  async startTyping(
    threadId: string,
    status?: string,
    options?: TypingOptions,
  ): Promise<void> {
    await this.rpc.request("startTyping", [threadId, status, options]);
  }

  async endTyping(
    threadId: string,
    status?: AgentSessionStatus,
  ): Promise<void> {
    await this.rpc.request("endTyping", [threadId, status]);
  }

  /**
   * An AsyncIterable cannot be an RPC argument, so the stream is opened, pushed
   * to in batches, then closed. When the host's adapter declines to stream this
   * returns without touching `textStream`, which Chat's own fallback re-reads.
   */
  async stream(
    threadId: string,
    textStream: AsyncIterable<string | StreamChunk>,
    options?: StreamOptions,
  ): Promise<RawMessage<TRawMessage> | null> {
    const start = (await this.rpc.request("streamStart", [
      threadId,
      toWireStreamOptions(options),
    ])) as StreamStart;
    if (!("streamId" in start)) {
      return start.result as RawMessage<TRawMessage> | null;
    }

    const { streamId } = start;
    const signal = options?.signal;
    let batch: Array<string | StreamChunk> = [];
    let pump: Promise<void> | undefined;
    let failure: unknown;

    // One push in flight at a time, so the first chunk leaves immediately and
    // batching follows the round trip rather than a fixed interval.
    const drain = async (): Promise<void> => {
      while (batch.length > 0) {
        const chunks = batch;
        batch = [];
        await this.rpc.request("streamPush", [streamId, chunks]);
      }
      pump = undefined;
    };

    try {
      for await (const chunk of textStream) {
        if (signal?.aborted || failure) break;
        batch.push(chunk);
        pump ??= drain().catch((error: unknown) => {
          failure ??= error;
        });
      }
      await pump;
      if (failure) throw failure;
      if (batch.length > 0)
        await this.rpc.request("streamPush", [streamId, batch]);
    } catch (error) {
      await this.rpc
        .request("streamEnd", [streamId, true])
        .catch(() => undefined);
      throw error;
    }

    return (await this.rpc.request("streamEnd", [
      streamId,
      signal?.aborted ?? false,
    ])) as RawMessage<TRawMessage> | null;
  }

  async scheduleMessage(
    threadId: string,
    message: AdapterPostableMessage,
    options: { postAt: Date },
  ): Promise<ScheduledMessage<TRawMessage>> {
    const scheduled = (await this.rpc.request("scheduleMessage", [
      threadId,
      message,
      options,
    ])) as Omit<ScheduledMessage<TRawMessage>, "cancel">;

    return {
      ...scheduled,
      cancel: async () => {
        await this.rpc.request("cancelScheduledMessage", [
          scheduled.scheduledMessageId,
        ]);
      },
    };
  }

  /** Synchronous by contract, so the deferred `fetchData` is what crosses the wire. */
  rehydrateAttachment(attachment: Attachment): Attachment {
    if (attachment.data) return attachment;
    const { data: _data, fetchData: _fetchData, ...metadata } = attachment;

    return {
      ...attachment,
      fetchData: async () => {
        const data = await this.rpc.request("rehydrateAttachment", [metadata]);
        if (!data) {
          throw new Error(
            `chat-adapter-remote: no data available for attachment ${attachment.name ?? ""}`.trim(),
          );
        }
        return data as Buffer;
      },
    };
  }

  async disconnect(): Promise<void> {
    await this.rpc.request("disconnect", []);
  }

  /** Receives the events the host forwards, acknowledging as soon as one is accepted. */
  async handleWebhook(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    // Nothing here may throw past this point; the caller is a webhook route.
    try {
      return await this.receive(request, options);
    } catch (error) {
      this.report(error, "handleWebhook");
      return Response.json(
        { jsonrpc: "2.0", id: null, error: serializeError(error) },
        { status: 500 },
      );
    }
  }

  private async receive(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    const verified = await verifyRequest(request, {
      secret: this.config.secret,
      timestampToleranceMs: this.config.timestampToleranceMs,
      maxBodyBytes: this.config.maxBodyBytes,
      replayGuard: this.replayGuard,
    });
    if (!verified.ok) return verified.response;

    const call = INBOUND_CALLS.safeParse({
      method: verified.method,
      id: verified.id,
      params: verified.params,
    });
    if (!call.success) {
      this.logger.error("rejected inbound call", {
        method: verified.method,
        issues: call.error.issues,
      });
      return Response.json(
        {
          jsonrpc: "2.0",
          id: verified.id ?? null,
          error: {
            code: RpcErrorCode.INVALID_PARAMS,
            message: `Unsupported or malformed inbound call: ${verified.method}`,
          },
        },
        { status: 400 },
      );
    }

    if (call.data.method === "log") {
      const [level, prefix, message, args] = call.data.params;
      this.chat?.getLogger(prefix || undefined)[level](message, ...args);
      return new Response(null, { status: 204 });
    }

    const chat = this.chat;
    if (!chat) {
      // Acknowledging would drop it: the host takes 200 as delivered.
      this.logger.error("inbound call arrived before initialize()", {
        method: call.data.method,
      });
      return Response.json(
        {
          jsonrpc: "2.0",
          id: call.data.id,
          error: {
            code: RpcErrorCode.INTERNAL_ERROR,
            message: "Adapter is not initialized",
          },
        },
        { status: 503 },
      );
    }

    try {
      const answer = await this.deliver(chat, call.data, options);
      return Response.json({
        jsonrpc: "2.0",
        id: call.data.id,
        result: answer ?? null,
      });
    } catch (error) {
      const wireError = serializeError(error);
      return Response.json(
        { jsonrpc: "2.0", id: call.data.id, error: wireError },
        { status: 500 },
      );
    }
  }

  /** Handlers run under the caller's `waitUntil`, so work is started rather than awaited; an unhandled rejection would end the process. */
  private detach(work: unknown, method: string): void {
    void Promise.resolve(work).catch((error: unknown) =>
      this.report(error, method),
    );
  }

  /** The consumer usually runs where no debugger can be attached, so failures have to be routable. */
  private report(error: unknown, method: string): void {
    this.logger.error(`inbound ${method} failed`, { error });
    this.config.onError?.(error, { method });
  }

  /** Rebuilds what the host had to strip: dates, buffers, messages, the emoji singleton, and this adapter. */
  private event<T>(payload: Record<string, unknown>): T {
    const restored: Record<string, unknown> = {
      ...(decode(payload) as Record<string, unknown>),
      adapter: this,
    };
    for (const key of EVENT_MESSAGE_KEYS) {
      if (restored[key]) restored[key] = deserializeMessage(restored[key]);
    }
    if (typeof restored.emoji === "string")
      restored.emoji = getEmoji(restored.emoji);
    return restored as T;
  }

  private deliver(
    chat: ChatInstance,
    call: InboundEvent,
    options?: WebhookOptions,
  ): Promise<unknown> | void {
    switch (call.method) {
      case "processMessage": {
        const [threadId, wire, facts] = call.params;
        this.rememberThread(threadId, {
          channelId: facts.channelId,
          isDM: facts.isDM ?? false,
          channelVisibility: facts.channelVisibility as
            ChannelVisibility | undefined,
        });
        this.detach(
          chat.processMessage(
            this,
            threadId,
            deserializeMessage(wire),
            options,
          ),
          call.method,
        );
        return;
      }
      case "processReaction":
        this.detach(
          chat.processReaction(this.event(call.params[0]), options),
          call.method,
        );
        return;
      case "processMessageUpdated":
        this.detach(
          chat.processMessageUpdated(this.event(call.params[0]), options),
          call.method,
        );
        return;
      case "processMessageDeleted":
        this.detach(
          chat.processMessageDeleted(this.event(call.params[0]), options),
          call.method,
        );
        return;
      case "processAction":
        this.detach(
          chat.processAction(this.event(call.params[0]), options),
          call.method,
        );
        return;
      case "processSlashCommand":
        this.detach(
          chat.processSlashCommand(this.event(call.params[0]), options),
          call.method,
        );
        return;
      case "abortTurn":
        this.detach(chat.abortTurn(call.params[0]), call.method);
        return;
      case "processModalClose":
        this.detach(
          chat.processModalClose(
            this.event(call.params[0]),
            call.params[1] ?? undefined,
            options,
          ),
          call.method,
        );
        return;
      case "processAgentSessionStopped":
      case "processAgentSessionTitleChanged":
      case "processAppHomeOpened":
      case "processAppContextChanged":
      case "processAssistantThreadStarted":
      case "processAssistantContextChanged":
      case "processMemberJoinedChannel":
        this.detach(
          chat[call.method](this.event(call.params[0]), options),
          call.method,
        );
        return;
      // Awaited, not detached: the host is relaying the answer to the platform.
      case "processModalSubmit":
        return chat.processModalSubmit(
          this.event(call.params[0]),
          call.params[1] ?? undefined,
          options,
        );
      case "processOptionsLoad":
        return chat.processOptionsLoad(this.event(call.params[0]), options);
    }
  }
}

export function createRemoteAdapter<TThreadId = unknown, TRawMessage = unknown>(
  config: RemoteAdapterConfig,
): RemoteAdapter<TThreadId, TRawMessage> {
  return new RemoteAdapter(config);
}
