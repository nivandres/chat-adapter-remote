import type {
  Adapter,
  ChatInstance,
  HistoryApi,
  Logger,
  Message,
  StateAdapter,
  TranscriptsApi,
  WebhookOptions,
} from "chat";
import { ConsoleLogger } from "chat";

import { serializeMessageForWire } from "../rpc/message-wire";
import { createRpcClient, type RpcClient } from "../rpc/transport";
import { createBridgingLogger } from "./logger-bridge";

/** Thrown by every `ChatInstance` member RemoteChat doesn't bridge — a documented v1 capability boundary. Only `processMessage` and `getLogger` cross the wire; see rpc/methods.ts. */
export class RemoteChatUnsupportedMethodError extends Error {
  constructor(method: string) {
    super(
      `chat-adapter-remote: ChatInstance.${method} is not supported through the remote bridge in v1. ` +
        `Only processMessage and getLogger are bridged to the consumer process.`,
    );
    this.name = "RemoteChatUnsupportedMethodError";
  }
}

export interface RemoteChatOptions {
  consumerUrl: string;
  secret: string;
  timeoutMs?: number;
  logger?: Logger;
  /** Override the fetch implementation used for inbound-forwarding RPC calls. Mainly for tests. */
  fetch?: typeof fetch;
}

function unsupported(method: string): never {
  throw new RemoteChatUnsupportedMethodError(method);
}

/** The host-side fake `ChatInstance` handed to the real adapter's `initialize()`. `processMessage`/`getLogger` forward to the consumer; everything else throws. */
export class RemoteChat implements ChatInstance {
  private readonly rpc: RpcClient;
  private readonly localLogger: Logger;

  constructor(options: RemoteChatOptions) {
    this.localLogger =
      options.logger ?? new ConsoleLogger("info", "chat-adapter-remote:host");
    this.rpc = createRpcClient({
      url: options.consumerUrl,
      secret: options.secret,
      timeoutMs: options.timeoutMs,
      fetch: options.fetch,
    });
  }

  async processMessage(
    adapter: Adapter,
    threadId: string,
    message: Message | (() => Promise<Message>),
    _options?: WebhookOptions,
  ): Promise<void> {
    const resolved = typeof message === "function" ? await message() : message;
    // Answers RemoteAdapter.channelIdFromThreadId() without an RPC round trip.
    const channelId = adapter.channelIdFromThreadId(threadId);
    const wire = await serializeMessageForWire(resolved, (attachment, error) =>
      this.localLogger.warn(
        "chat-adapter-remote: failed to fetch attachment data, forwarding metadata only",
        { attachment, error },
      ),
    );
    await this.rpc.request("processMessage", [threadId, wire, channelId]);
    // `adapter` and `_options` are not serializable and are never sent.
  }

  getLogger(prefix?: string): Logger {
    return createBridgingLogger({
      prefix,
      localLogger: prefix ? this.localLogger.child(prefix) : this.localLogger,
      notify: (level, pfx, message, args) =>
        this.rpc
          .notify("log", [level, pfx, message, args])
          .then(() => undefined),
    });
  }

  abortTurn(): Promise<void> {
    return unsupported("abortTurn");
  }
  getState(): StateAdapter {
    return unsupported("getState");
  }
  getUserName(): string {
    return unsupported("getUserName");
  }
  handleIncomingMessage(): Promise<void> {
    return unsupported("handleIncomingMessage");
  }
  get history(): HistoryApi {
    return unsupported("history");
  }
  processAction(): Promise<void> {
    return unsupported("processAction");
  }
  processAgentSessionStopped(): void {
    return unsupported("processAgentSessionStopped");
  }
  processAgentSessionTitleChanged(): void {
    return unsupported("processAgentSessionTitleChanged");
  }
  processAppContextChanged(): void {
    return unsupported("processAppContextChanged");
  }
  processAppHomeOpened(): void {
    return unsupported("processAppHomeOpened");
  }
  processAssistantContextChanged(): void {
    return unsupported("processAssistantContextChanged");
  }
  processAssistantThreadStarted(): void {
    return unsupported("processAssistantThreadStarted");
  }
  processMemberJoinedChannel(): void {
    return unsupported("processMemberJoinedChannel");
  }
  processMessageDeleted(): Promise<void> {
    return unsupported("processMessageDeleted");
  }
  processMessageUpdated(): Promise<void> {
    return unsupported("processMessageUpdated");
  }
  processModalClose(): void {
    return unsupported("processModalClose");
  }
  processModalSubmit(): Promise<undefined> {
    return unsupported("processModalSubmit");
  }
  processOptionsLoad(): Promise<undefined> {
    return unsupported("processOptionsLoad");
  }
  processReaction(): void {
    return unsupported("processReaction");
  }
  processSlashCommand(): void {
    return unsupported("processSlashCommand");
  }
  get transcripts(): TranscriptsApi {
    return unsupported("transcripts");
  }
}
