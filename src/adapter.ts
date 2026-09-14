import type {
  Adapter,
  AdapterPostableMessage,
  ChatInstance,
  EmojiValue,
  FetchOptions,
  FetchResult,
  FormattedContent,
  Logger,
  Message,
  RawMessage,
  ThreadInfo,
  TypingOptions,
  WebhookOptions,
} from "chat";
import { ConsoleLogger } from "chat";

import { deserializeMessageFromWire } from "./rpc/message-wire";
import { verifyAndParse } from "./rpc/dispatch";
import { RpcErrorCode, serializeError } from "./rpc/errors";
import { INBOUND_CALLS } from "./rpc/methods";
import { createRpcClient, type RpcClient } from "./rpc/transport";
import type { RemoteAdapterConfig } from "./types";

/**
 * Thrown by the three `Adapter` members declared synchronous, which cannot
 * be bridged over an async RPC call. Chat SDK core never calls these on an
 * adapter it holds — they exist only for an adapter's own internal use,
 * unlike `channelIdFromThreadId`, which core does call and which
 * `RemoteAdapter` answers locally instead (see below).
 */
export class RemoteAdapterUnsupportedSyncMethodError extends Error {
  constructor(method: string) {
    super(
      `chat-adapter-remote: Adapter.${method} is synchronous and cannot be bridged over RPC. ` +
        `Chat SDK core never calls this method on an adapter it holds, so this should not be reachable in practice.`,
    );
    this.name = "RemoteAdapterUnsupportedSyncMethodError";
  }
}

function jsonRpcResponse(id: unknown, result: unknown): Response {
  return Response.json({ jsonrpc: "2.0", id: id ?? null, result });
}

/**
 * The consumer-side fake `Adapter`, registered on the real `Chat` instance
 * like any other adapter. Every outbound call forwards to the host over
 * signed HTTP JSON-RPC; `handleWebhook` is the real inbound receiver the
 * host calls into.
 */
export class RemoteAdapter<
  TThreadId = unknown,
  TRawMessage = unknown,
> implements Adapter<TThreadId, TRawMessage> {
  name: string;
  userName: string;
  botUserId?: string;

  private chat: ChatInstance | null = null;
  private readonly rpc: RpcClient;
  private readonly logger: Logger;
  private readonly secret: string;
  private readonly replayWindowMs?: number;
  private readonly maxBodyBytes?: number;
  /** threadId -> channelId, populated from every inbound `processMessage`; lets `channelIdFromThreadId()` answer synchronously. */
  private readonly channelIdCache = new Map<string, string>();

  constructor(private readonly config: RemoteAdapterConfig) {
    this.name = config.name ?? "remote";
    this.userName = config.userName ?? this.name;
    this.logger =
      config.logger ?? new ConsoleLogger("info", "chat-adapter-remote");
    this.secret = config.secret;
    this.rpc = createRpcClient({
      url: config.url,
      secret: config.secret,
      timeoutMs: config.timeoutMs,
      fetch: config.fetch,
    });
  }

  async initialize(chat: ChatInstance): Promise<void> {
    this.chat = chat;
    const info = (await this.rpc.request("__handshake", [])) as {
      name: string;
      userName: string;
      botUserId?: string;
    };
    if (!this.config.name) this.name = info.name;
    if (!this.config.userName) this.userName = info.userName;
    this.botUserId = info.botUserId;
  }

  /** Cached value from inbound traffic, or the standard `{adapter}:{channel}` fallback for a threadId not seen yet. Never RPC-forwarded. */
  channelIdFromThreadId(threadId: string): string {
    const cached = this.channelIdCache.get(threadId);
    if (cached) return cached;
    return threadId.split(":").slice(0, 2).join(":");
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
    throw new Error(
      "RemoteAdapter.parseMessage is never called — messages arrive pre-parsed via handleWebhook.",
    );
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

  async addReaction(
    threadId: string,
    messageId: string,
    emoji: EmojiValue | string,
  ): Promise<void> {
    // EmojiValue.toJSON() returns a placeholder string, not .name, so this normalizes before the wire.
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

  async fetchMessages(
    threadId: string,
    options?: FetchOptions,
  ): Promise<FetchResult<TRawMessage>> {
    const result = (await this.rpc.request("fetchMessages", [
      threadId,
      options,
    ])) as { messages: unknown[]; nextCursor?: string };
    return {
      messages: result.messages.map(
        (wire) =>
          deserializeMessageFromWire(wire) as unknown as Message<TRawMessage>,
      ),
      nextCursor: result.nextCursor,
    };
  }

  async fetchThread(threadId: string): Promise<ThreadInfo> {
    return (await this.rpc.request("fetchThread", [threadId])) as ThreadInfo;
  }

  async startTyping(
    threadId: string,
    status?: string,
    options?: TypingOptions,
  ): Promise<void> {
    await this.rpc.request("startTyping", [threadId, status, options]);
  }

  async disconnect(): Promise<void> {
    await this.rpc.request("disconnect", []);
  }

  /** The real inbound entry point — the host calls this to deliver events forwarded from the real adapter, then runs `chat.processMessage()` locally. */
  async handleWebhook(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    const verified = await verifyAndParse(request, {
      secret: this.secret,
      replayWindowMs: this.replayWindowMs,
      maxBodyBytes: this.maxBodyBytes,
    });
    if (!verified.ok) return verified.response;

    const call = INBOUND_CALLS.safeParse({
      method: verified.method,
      id: verified.id,
      params: verified.params,
    });
    if (!call.success) {
      return jsonRpcResponse(verified.id, null); // unknown/invalid inbound call: acknowledge, do nothing
    }

    if (call.data.method === "log") {
      const [level, prefix, message, args] = call.data.params;
      this.chat?.getLogger(prefix || undefined)[level](message, ...args);
      return new Response(null, { status: 204 });
    }

    // processMessage
    const [threadId, wireMessage, channelId] = call.data.params;
    this.channelIdCache.set(threadId, channelId);

    try {
      const message = deserializeMessageFromWire(wireMessage);
      await this.chat?.processMessage(this, threadId, message, options);
      return jsonRpcResponse(verified.id, null);
    } catch (error) {
      const wireError = serializeError(error);
      return Response.json(
        { jsonrpc: "2.0", id: verified.id ?? null, error: wireError },
        { status: wireError.code === RpcErrorCode.INTERNAL_ERROR ? 500 : 200 },
      );
    }
  }
}

export function createRemoteAdapter<TThreadId = unknown, TRawMessage = unknown>(
  config: RemoteAdapterConfig,
): RemoteAdapter<TThreadId, TRawMessage> {
  return new RemoteAdapter(config);
}
