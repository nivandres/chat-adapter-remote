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
import { ValidationError } from "@chat-adapter/shared";
import { ConsoleLogger, getEmoji } from "chat";

import { decode, encode } from "./rpc/codec";
import { verifyRequest } from "./rpc/dispatch";
import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  isTransient,
  serializeError,
} from "./rpc/errors";
import {
  ATTACHMENT_REF,
  deserializeMessage,
  serializeMessage,
} from "./rpc/message-wire";
import {
  EVENT_MESSAGE_KEYS,
  HandshakeSchema,
  INBOUND_CALLS,
  OPTIONAL_CAPABILITIES,
  PROTOCOL_VERSION,
  SCOPED_STATE_OPERATIONS,
  type StateOperation,
} from "./rpc/methods";
import { createReplayGuard, type ReplayGuard } from "./rpc/security";
import { createRpcClient, type RpcClient } from "./rpc/transport";
import { translateIds, translateParams } from "./thread-ids";
import type { HostEvent, RemoteAdapterConfig } from "./types";

/** Thrown by the synchronous `Adapter` members, which a round trip cannot answer. */
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

type InboundEvent = Exclude<
  ReturnType<typeof INBOUND_CALLS.parse>,
  { method: "log" }
>;

function requireOption(value: string | undefined, option: string): string {
  if (!value) {
    const variable = option === "url" ? "URL" : "SECRET";
    throw new ValidationError(
      "remote",
      `"${option}" is required; pass it or set CHAT_ADAPTER_REMOTE_${variable}`,
    );
  }
  return value;
}

type StreamStart =
  { streamId: string } | { done: true; result: RawMessage<unknown> | null };

const DEFAULT_THREAD_CACHE = 1000;
const THREAD_FACTS_TTL_MS = 30 * 24 * 60 * 60 * 1000;

function toWireStreamOptions(
  options?: StreamOptions,
): Record<string, unknown> | undefined {
  if (!options) return undefined;
  const { signal: _signal, ...rest } = options;
  return rest;
}

/** The consumer-side stand-in, registered on a real `Chat`. */
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
  private readonly link: RpcClient;
  private readonly logger: Logger;
  private readonly replayGuard: ReplayGuard;
  private readonly secret: string;
  private handshaken = false;
  private hostName?: string;
  private streams = true;
  private handshaking?: Promise<void>;
  private readonly threads = new Map<string, ThreadFacts>();
  private readonly maxCachedThreads: number;

  constructor(private readonly config: RemoteAdapterConfig) {
    const url = requireOption(
      config.url ?? process.env.CHAT_ADAPTER_REMOTE_URL,
      "url",
    );
    this.secret = requireOption(
      config.secret ?? process.env.CHAT_ADAPTER_REMOTE_SECRET,
      "secret",
    );
    this.name = config.name ?? "remote";
    this.userName = config.userName ?? this.name;
    this.logger =
      config.logger ?? new ConsoleLogger("info", "chat-adapter-remote");
    this.maxCachedThreads = config.maxCachedThreads ?? DEFAULT_THREAD_CACHE;
    this.replayGuard = config.replayGuard ?? createReplayGuard();
    this.link = createRpcClient({
      url,
      secret: this.secret,
      timeoutMs: config.timeoutMs,
      fetch: config.fetch,
    });
    this.rpc = {
      request: async (method, params) => {
        await this.handshake();
        const result = await this.link.request(method, this.outward(params));
        return this.inward(method, result);
      },
      notify: (method, params) => this.link.notify(method, params),
    };
  }

  async initialize(chat: ChatInstance): Promise<void> {
    this.chat = chat;
    await this.preloadThreads();
    try {
      await this.handshake();
    } catch (error) {
      // Unreachable may clear on its own; a wrong secret or protocol will not.
      if (!isTransient(error)) throw error;
      this.logger.warn("host unreachable, will retry on the next call", {
        error,
      });
      this.config.onError?.(error, { method: "__handshake" });
    }
  }

  /** Chat keeps its first init promise forever, so a failure here must not be remembered. */
  private async handshake(): Promise<void> {
    if (this.handshaken) return;
    this.handshaking ??= this.negotiate().finally(() => {
      this.handshaking = undefined;
    });
    await this.handshaking;
  }

  private async negotiate(): Promise<void> {
    const handshake = HandshakeSchema.parse(
      await this.link.request("__handshake", []),
    );
    if (handshake.protocolVersion !== PROTOCOL_VERSION) {
      throw new Error(
        `chat-adapter-remote: protocol mismatch, host speaks v${handshake.protocolVersion} and this consumer speaks v${PROTOCOL_VERSION}`,
      );
    }
    this.hostName = handshake.name;
    if (!this.config.userName) this.userName = handshake.userName;
    this.botUserId = handshake.botUserId;
    this.lockScope = handshake.lockScope;
    this.persistThreadHistory = handshake.persistThreadHistory;
    this.supportsTurnCancellation = handshake.supportsTurnCancellation;
    this.applyCapabilities(handshake.capabilities);
    this.applyCustomMethods(handshake.customMethods);
    this.handshaken = true;
  }

  /** Chat checks `adapter.x?.()`, so what the host did not report is removed. */
  private applyCapabilities(capabilities?: string[]): void {
    if (!capabilities) return;
    const supported = new Set(capabilities);
    this.streams = supported.has("stream");
    for (const name of OPTIONAL_CAPABILITIES) {
      if (!supported.has(name)) Reflect.set(this, name, undefined);
    }
  }

  /** Never replaces an existing member, so the host cannot redefine the interface. */
  private applyCustomMethods(names?: string[]): void {
    for (const name of names ?? []) {
      if (name in this) continue;
      Reflect.set(this, name, (...args: unknown[]) =>
        this.rpc.request("custom", [name, args]),
      );
    }
  }

  /** The host may call before this side reached it, so its name is resolved here too. */
  private async inbound(method: string, params: unknown): Promise<unknown> {
    if (method === "state" || method === "log" || !Array.isArray(params)) {
      return params;
    }
    await this.handshake().catch((error: unknown) => {
      this.logger.warn("could not name the host; ids pass through as sent", {
        error,
      });
    });
    return this.hostName
      ? translateParams(params, this.hostName, this.name)
      : params;
  }

  private outward(params: unknown): unknown {
    if (!this.hostName || !Array.isArray(params)) return params;
    return translateParams(params, this.name, this.hostName);
  }

  private inward(method: string, result: unknown): unknown {
    if (!this.hostName) return result;
    if (method === "openDM" && typeof result === "string") {
      return translateParams([result], this.hostName, this.name)[0];
    }
    return translateIds(result, this.hostName, this.name);
  }

  private rememberThread(threadId: string, facts: ThreadFacts): void {
    const known = this.threads.get(threadId);
    // Insertion order makes the oldest entry the first key.
    this.threads.delete(threadId);
    while (this.threads.size >= this.maxCachedThreads)
      this.threads.delete(this.threads.keys().next().value!);
    this.threads.set(threadId, facts);
    if (!known || JSON.stringify(known) !== JSON.stringify(facts)) {
      void this.persistThread(threadId, facts);
    }
  }

  /** `isDM` is synchronous, so another instance can only answer it from what was loaded ahead. */
  private async persistThread(
    threadId: string,
    facts: ThreadFacts,
  ): Promise<void> {
    try {
      const state = this.chat?.getState();
      if (!state) return;
      const key = this.threadKey(threadId);
      // Only a thread new to the store joins the list, however often instances restart.
      if (await state.setIfNotExists(key, facts, THREAD_FACTS_TTL_MS)) {
        await state.appendToList(this.recentThreadsKey, threadId, {
          maxLength: this.maxCachedThreads,
        });
      } else {
        await state.set(key, facts, THREAD_FACTS_TTL_MS);
      }
    } catch (error) {
      this.logger.debug("could not persist thread facts", { threadId, error });
    }
  }

  private async preloadThreads(): Promise<void> {
    try {
      const state = this.chat?.getState();
      if (!state) return;
      const recent = [
        ...new Set(await state.getList<string>(this.recentThreadsKey)),
      ]
        .slice(-this.maxCachedThreads)
        .filter((threadId) => !this.threads.has(threadId));
      // In parallel: a store over HTTP would otherwise take a round trip per thread.
      const facts = await Promise.all(
        recent.map((threadId) =>
          state.get<ThreadFacts>(this.threadKey(threadId)),
        ),
      );
      recent.forEach((threadId, index) => {
        if (facts[index]) this.threads.set(threadId, facts[index]);
      });
    } catch (error) {
      this.logger.warn("could not preload thread facts", { error });
    }
  }

  private threadKey(threadId: string): string {
    return `remote:${this.name}:thread:${threadId}`;
  }

  private get recentThreadsKey(): string {
    return `remote:${this.name}:threads`;
  }

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

  private rebuild(wire: unknown): Message {
    return deserializeMessage(wire, (wired) => {
      const held = wired[ATTACHMENT_REF];
      if (held) {
        let pending: Promise<Buffer> | undefined;
        return () => {
          pending ??= (
            this.rpc.request("fetchAttachment", [held]) as Promise<Buffer>
          ).catch((error: unknown) => {
            pending = undefined;
            throw error;
          });
          return pending;
        };
      }
      if (!wired.fetchMetadata || !this.rehydrateAttachment) return undefined;
      return () =>
        this.rpc.request("rehydrateAttachment", [wired]) as Promise<Buffer>;
    });
  }

  private toFetchResult(result: unknown): FetchResult<TRawMessage> {
    const { messages, nextCursor } = result as {
      messages: unknown[];
      nextCursor?: string;
    };
    return {
      messages: messages.map(
        (wire) => this.rebuild(wire) as Message<TRawMessage>,
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
    return wire ? (this.rebuild(wire) as Message<TRawMessage>) : null;
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
        rootMessage: this.rebuild(thread.rootMessage),
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

  /** Declining before reading leaves `textStream` for Chat's own fallback, which re-reads it. */
  async stream(
    threadId: string,
    textStream: AsyncIterable<string | StreamChunk>,
    options?: StreamOptions,
  ): Promise<RawMessage<TRawMessage> | null> {
    try {
      await this.handshake();
    } catch (error) {
      if (isTransient(error)) return null;
      throw error;
    }
    if (!this.streams) return null;

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

    // One push in flight: batching follows the round trip, not a fixed interval.
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
      // As in-process: the adapter sees the failure, and keeps a reply only if it chooses to.
      const kept = await this.rpc
        .request("streamEnd", [streamId, "failed"])
        .catch(() => null);
      if (kept) return kept as RawMessage<TRawMessage>;
      throw error;
    }

    return (await this.rpc.request("streamEnd", [
      streamId,
      signal?.aborted ? "aborted" : "finished",
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

  async handleWebhook(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    // An unhandled rejection here would end the process.
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
      secret: this.secret,
      timestampToleranceMs: this.config.timestampToleranceMs,
      maxBodyBytes: this.config.maxBodyBytes,
      replayGuard: this.replayGuard,
    });
    if (!verified.ok) return verified.response;

    const call = INBOUND_CALLS.safeParse({
      method: verified.method,
      id: verified.id,
      params: await this.inbound(verified.method, verified.params),
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

    if (call.data.method === "hostEvent") {
      try {
        this.config.onEvent?.(decode(call.data.params[0]) as HostEvent);
      } catch (error) {
        this.report(error, "hostEvent");
      }
      return new Response(null, { status: 204 });
    }

    if (call.data.method === "log") {
      const [level, prefix, message, args] = call.data.params;
      this.chat?.getLogger(prefix || undefined)[level](message, ...args);
      return new Response(null, { status: 204 });
    }

    const chat = this.chat;
    if (!chat) {
      // A 200 would tell the host it was delivered.
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
        result: (await encode(answer)) ?? null,
      });
    } catch (error) {
      // A refusal of ours is an answer; a 5xx tells the host it may send again.
      const wireError = serializeError(error);
      return Response.json(
        { jsonrpc: "2.0", id: call.data.id, error: wireError },
        { status: error instanceof RemoteAdapterRpcError ? 200 : 500 },
      );
    }
  }

  private detach(work: unknown, method: string): void {
    void Promise.resolve(work).catch((error: unknown) =>
      this.report(error, method),
    );
  }

  private report(error: unknown, method: string): void {
    this.logger.error(`inbound ${method} failed`, { error });
    this.config.onError?.(error, { method });
  }

  /** Scoped keys are prefixed here, never trusted from the wire. */
  private runStateOperation(
    chat: ChatInstance,
    operation: StateOperation,
    args: unknown[],
  ): Promise<unknown> {
    const access = this.config.hostState ?? "scoped";
    // Lending nothing tells the host to use its own store; a narrower scope refuses the call.
    if (access === "off") {
      throw new RemoteAdapterRpcError(
        RpcErrorCode.STATE_UNAVAILABLE,
        "chat-adapter-remote: this consumer does not lend its state",
      );
    }
    if (
      access === "scoped" &&
      !(SCOPED_STATE_OPERATIONS as readonly string[]).includes(operation)
    ) {
      throw new RemoteAdapterRpcError(
        RpcErrorCode.METHOD_NOT_IMPLEMENTED,
        `chat-adapter-remote: state access is scoped, so ${operation} is not available`,
      );
    }

    const store = chat.getState() as unknown as Record<
      string,
      (...a: unknown[]) => Promise<unknown>
    >;
    const scoped =
      access === "scoped"
        ? [`adapter:${this.name}:${String(args[0])}`, ...args.slice(1)]
        : args;
    return store[operation]!(...scoped);
  }

  private event<T>(payload: Record<string, unknown>): T {
    const restored: Record<string, unknown> = {
      ...(decode(payload) as Record<string, unknown>),
      adapter: this,
    };
    for (const key of EVENT_MESSAGE_KEYS) {
      if (restored[key]) restored[key] = this.rebuild(restored[key]);
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
          chat.processMessage(this, threadId, this.rebuild(wire), options),
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
      case "state":
        return this.runStateOperation(chat, call.params[0], call.params[1]);
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
      // Awaited: the platform is waiting on this answer.
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

type ThreadIdOf<A> = A extends Adapter<infer T, infer _R> ? T : unknown;
type RawMessageOf<A> = A extends Adapter<infer _T, infer R> ? R : unknown;

/** Crossing the wire makes every custom method async. */
type CustomOf<A> = {
  [
    K in keyof A as K extends keyof Adapter
      ? never
      : K extends `_${string}`
        ? never
        : A[K] extends (...args: never[]) => unknown
          ? K
          : never
  ]: A[K] extends (...args: infer P) => infer R
    ? (...args: P) => Promise<Awaited<R>>
    : never;
};

export type RemoteOf<A extends Adapter> = RemoteAdapter<
  ThreadIdOf<A>,
  RawMessageOf<A>
> &
  CustomOf<A>;

export function createRemoteAdapter<TAdapter extends Adapter = Adapter>(
  config: RemoteAdapterConfig,
): RemoteOf<TAdapter> {
  return new RemoteAdapter(config) as RemoteOf<TAdapter>;
}
