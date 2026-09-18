import type {
  Adapter,
  AdapterPostableMessage,
  ChatInstance,
  EmojiValue,
  FetchOptions,
  FetchResult,
  FormattedContent,
  LockScope,
  Logger,
  Message,
  RawMessage,
  ThreadInfo,
  TypingOptions,
  WebhookOptions,
} from "chat";
import { ConsoleLogger } from "chat";

import { verifyRequest } from "./rpc/dispatch";
import { RpcErrorCode, serializeError } from "./rpc/errors";
import { deserializeMessage } from "./rpc/message-wire";
import {
  HandshakeSchema,
  INBOUND_CALLS,
  PROTOCOL_VERSION,
} from "./rpc/methods";
import { createRpcClient, type RpcClient } from "./rpc/transport";
import type { RemoteAdapterConfig } from "./types";

/**
 * Thrown by the `Adapter` members that are synchronous and therefore cannot
 * be answered over RPC. Chat SDK core never calls these on an adapter it
 * holds; they exist for an adapter's own internal use.
 */
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
}

/**
 * The consumer-side stand-in, registered on a real `Chat` like any other
 * adapter. Outbound calls forward to the host; `handleWebhook` receives the
 * events the host forwards back.
 */
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
  private readonly threads = new Map<string, ThreadFacts>();

  constructor(private readonly config: RemoteAdapterConfig) {
    this.name = config.name ?? "remote";
    this.userName = config.userName ?? this.name;
    this.logger =
      config.logger ?? new ConsoleLogger("info", "chat-adapter-remote");
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
  }

  /** Answered from facts the host sends with each inbound message, falling back to the `{adapter}:{channel}` convention. */
  channelIdFromThreadId(threadId: string): string {
    return (
      this.threads.get(threadId)?.channelId ??
      threadId.split(":").slice(0, 2).join(":")
    );
  }

  isDM(threadId: string): boolean {
    return this.threads.get(threadId)?.isDM ?? false;
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

  async fetchMessages(
    threadId: string,
    options?: FetchOptions,
  ): Promise<FetchResult<TRawMessage>> {
    const result = (await this.rpc.request("fetchMessages", [
      threadId,
      options,
    ])) as {
      messages: unknown[];
      nextCursor?: string;
    };
    return {
      messages: result.messages.map(
        (wire) => deserializeMessage(wire) as unknown as Message<TRawMessage>,
      ),
      nextCursor: result.nextCursor,
    };
  }

  async fetchThread(threadId: string): Promise<ThreadInfo> {
    const info = (await this.rpc.request("fetchThread", [
      threadId,
    ])) as ThreadInfo;
    this.threads.set(threadId, {
      channelId: info.channelId,
      isDM: info.isDM ?? this.isDM(threadId),
    });
    return info;
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

  /**
   * Receives events the host forwards. Acknowledges as soon as the message is
   * accepted; the handler runs under the caller's `waitUntil`, matching how
   * every other adapter's webhook behaves.
   */
  async handleWebhook(
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    const verified = await verifyRequest(request, {
      secret: this.config.secret,
      timestampToleranceMs: this.config.timestampToleranceMs,
      maxBodyBytes: this.config.maxBodyBytes,
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

    const [threadId, wire, channelId, isDM] = call.data.params;
    this.threads.set(threadId, { channelId, isDM: isDM ?? false });

    try {
      const message = deserializeMessage(wire);
      void this.chat?.processMessage(this, threadId, message, options);
      return Response.json({ jsonrpc: "2.0", id: call.data.id, result: null });
    } catch (error) {
      const wireError = serializeError(error);
      return Response.json(
        { jsonrpc: "2.0", id: call.data.id, error: wireError },
        { status: 500 },
      );
    }
  }
}

export function createRemoteAdapter<TThreadId = unknown, TRawMessage = unknown>(
  config: RemoteAdapterConfig,
): RemoteAdapter<TThreadId, TRawMessage> {
  return new RemoteAdapter(config);
}
