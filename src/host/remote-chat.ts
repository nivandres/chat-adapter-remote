import type {
  Adapter,
  ChatInstance,
  EmojiValue,
  Logger,
  Message,
  WebhookOptions,
} from "chat";
import { ConsoleLogger } from "chat";

import { serializeMessage, type AttachmentPolicy } from "../rpc/message-wire";
import { EVENT_MESSAGE_KEYS } from "../rpc/methods";
import { createRpcClient, type RpcClient } from "../rpc/transport";
import { createBridgingLogger, type LogLevel } from "./logger-bridge";
import type { FetchLike } from "../types";

/** Where a failure happened, so callers can route it without parsing messages. */
export type HostErrorPhase = "initialize" | "forward" | "dispatch" | "shutdown";

export interface HostErrorContext {
  phase: HostErrorPhase;
  threadId?: string;
}

export type HostErrorHandler = (
  error: unknown,
  context: HostErrorContext,
) => void;

export interface RemoteChatOptions {
  consumerUrl: string;
  secret: string;
  timeoutMs?: number;
  logger?: Logger;
  fetch?: FetchLike;
  onError?: HostErrorHandler;
  /** Answers `getUserName()`, which cannot be asked of the consumer synchronously. */
  userName?: string;
  /** Builds the per-message attachment policy. */
  attachments?: () => AttachmentPolicy;
  /** Lines below this level stay on the host instead of crossing the wire. Default "info". */
  logForwardLevel?: LogLevel;
  /** Inbound messages forwarded at once. Default 8. */
  maxConcurrentForwards?: number;
  /** Messages allowed to queue behind those. Default 1000; past it, forwarding fails rather than growing. */
  maxQueuedForwards?: number;
}

/**
 * Caps concurrent tasks so a burst cannot open one request per message, and
 * caps the queue behind them so a history sync cannot park an unbounded number
 * of pending promises. Arrivals queue whenever anyone is already waiting, so a
 * latecomer cannot overtake the backlog.
 */
function createLimiter(limit: number, maxQueued: number) {
  const waiting: Array<() => void> = [];
  let active = 0;

  return async function run<T>(task: () => Promise<T>): Promise<T> {
    if (active >= limit || waiting.length > 0) {
      if (waiting.length >= maxQueued) {
        throw new Error(
          `chat-adapter-remote: ${maxQueued} messages already waiting to be forwarded`,
        );
      }
      await new Promise<void>((resolve) => waiting.push(resolve));
    }
    active++;
    try {
      return await task();
    } finally {
      active--;
      waiting.shift()?.();
    }
  };
}

type EventPayload = Record<string, unknown>;

/** Live objects the consumer rebuilds for itself, so they never cross the wire. */
const LOCAL_EVENT_KEYS = ["adapter", "thread", "channel"] as const;

class RemoteChat {
  readonly logger: Logger;
  private readonly rpc: RpcClient;
  private readonly warned = new Set<string>();
  private readonly limit: <T>(task: () => Promise<T>) => Promise<T>;

  constructor(private readonly options: RemoteChatOptions) {
    this.logger =
      options.logger ?? new ConsoleLogger("info", "chat-adapter-remote");
    this.limit = createLimiter(
      options.maxConcurrentForwards ?? 8,
      options.maxQueuedForwards ?? 1000,
    );
    this.rpc = createRpcClient({
      url: options.consumerUrl,
      secret: options.secret,
      timeoutMs: options.timeoutMs,
      fetch: options.fetch,
    });
  }

  // Adapters call these unawaited from their event loops, so a rejection here
  // would be unhandled and would take down the process holding the connection.

  private async forward(
    method: string,
    params: unknown[],
    threadId?: string,
  ): Promise<void> {
    try {
      await this.limit(() => this.rpc.request(method, params));
    } catch (error) {
      this.logger.error(`failed to deliver ${method} to consumer`, {
        threadId,
        error,
      });
      this.options.onError?.(error, { phase: "forward", threadId });
    }
  }

  private async resolveMessage(value: unknown): Promise<Message | undefined> {
    if (!value) return undefined;
    return typeof value === "function"
      ? await (value as () => Promise<Message>)()
      : (value as Message);
  }

  private async serializeEvent(event: EventPayload): Promise<EventPayload> {
    const wire: EventPayload = { ...event };
    for (const key of LOCAL_EVENT_KEYS) delete wire[key];

    for (const key of EVENT_MESSAGE_KEYS) {
      const message = await this.resolveMessage(wire[key]);
      if (message)
        wire[key] = await serializeMessage(
          message,
          this.options.attachments?.(),
        );
      else delete wire[key];
    }
    // EmojiValue.toJSON() gives a placeholder, not the name the consumer needs.
    if (wire.emoji) wire.emoji = (wire.emoji as EmojiValue).name;
    return wire;
  }

  private async forwardEvent(
    method: string,
    event: EventPayload,
    contextId?: string,
  ): Promise<void> {
    const threadId = event.threadId as string | undefined;
    try {
      const wire = await this.serializeEvent(event);
      const params = contextId === undefined ? [wire] : [wire, contextId];
      await this.forward(method, params, threadId);
    } catch (error) {
      this.logger.error(`failed to serialize ${method}`, { threadId, error });
      this.options.onError?.(error, { phase: "forward", threadId });
    }
  }

  /**
   * Unlike the fire-and-forget events, the platform is waiting on these, so
   * the consumer's answer is returned. A failure resolves to undefined, which
   * is what Chat treats as "no response" for both of them.
   */
  private async askEvent(
    method: string,
    event: EventPayload,
    contextId?: string,
  ): Promise<unknown> {
    const threadId = event.threadId as string | undefined;
    try {
      const wire = await this.serializeEvent(event);
      const params = contextId === undefined ? [wire] : [wire, contextId];
      // Deliberately outside the forward limiter: the platform times these out
      // after a few seconds, and a burst of inbound messages would otherwise
      // put a modal submit behind the whole backlog. They are paced by a human
      // clicking, so they cannot flood anything on their own.
      return await this.rpc.request(method, params);
    } catch (error) {
      this.logger.error(`failed to deliver ${method} to consumer`, {
        threadId,
        error,
      });
      this.options.onError?.(error, { phase: "forward", threadId });
      return undefined;
    }
  }

  async processMessage(
    adapter: Adapter,
    threadId: string,
    message: Message | (() => Promise<Message>),
    _options?: WebhookOptions,
  ): Promise<void> {
    try {
      const resolved = (await this.resolveMessage(message))!;
      const wire = await serializeMessage(
        resolved,
        this.options.attachments?.(),
        (attachment, error) =>
          this.logger.warn(
            "attachment data unavailable, forwarding metadata only",
            { attachment, error },
          ),
      );
      await this.forward(
        "processMessage",
        [
          threadId,
          wire,
          {
            channelId: adapter.channelIdFromThreadId(threadId),
            isDM: adapter.isDM?.(threadId),
            channelVisibility: adapter.getChannelVisibility?.(threadId),
          },
        ],
        threadId,
      );
    } catch (error) {
      this.logger.error("failed to deliver message to consumer", {
        threadId,
        error,
      });
      this.options.onError?.(error, { phase: "forward", threadId });
    }
  }

  processReaction(event: EventPayload): void {
    void this.forwardEvent("processReaction", event);
  }

  processMessageUpdated(event: EventPayload): Promise<void> {
    return this.forwardEvent("processMessageUpdated", event);
  }

  processMessageDeleted(event: EventPayload): Promise<void> {
    return this.forwardEvent("processMessageDeleted", event);
  }

  processAction(event: EventPayload): Promise<void> {
    return this.forwardEvent("processAction", event);
  }

  processSlashCommand(event: EventPayload): void {
    void this.forwardEvent("processSlashCommand", event);
  }

  processModalClose(event: EventPayload, contextId?: string): void {
    void this.forwardEvent("processModalClose", event, contextId);
  }

  processAgentSessionStopped(event: EventPayload): void {
    void this.forwardEvent("processAgentSessionStopped", event);
  }

  processAgentSessionTitleChanged(event: EventPayload): void {
    void this.forwardEvent("processAgentSessionTitleChanged", event);
  }

  processAppHomeOpened(event: EventPayload): void {
    void this.forwardEvent("processAppHomeOpened", event);
  }

  processAppContextChanged(event: EventPayload): void {
    void this.forwardEvent("processAppContextChanged", event);
  }

  processAssistantThreadStarted(event: EventPayload): void {
    void this.forwardEvent("processAssistantThreadStarted", event);
  }

  processAssistantContextChanged(event: EventPayload): void {
    void this.forwardEvent("processAssistantContextChanged", event);
  }

  processMemberJoinedChannel(event: EventPayload): void {
    void this.forwardEvent("processMemberJoinedChannel", event);
  }

  processModalSubmit(
    event: EventPayload,
    contextId?: string,
  ): Promise<unknown> {
    return this.askEvent("processModalSubmit", event, contextId);
  }

  processOptionsLoad(event: EventPayload): Promise<unknown> {
    return this.askEvent("processOptionsLoad", event);
  }

  abortTurn(threadId: string): Promise<void> {
    return this.forward("abortTurn", [threadId], threadId);
  }

  getUserName(): string {
    return this.options.userName ?? "";
  }

  getLogger(prefix?: string): Logger {
    return createBridgingLogger({
      prefix,
      localLogger: prefix ? this.logger.child(prefix) : this.logger,
      forwardLevel: this.options.logForwardLevel,
      notify: (level, pfx, message, args) =>
        this.rpc.notify("log", [level, pfx, message, args]),
    });
  }

  warnUnsupported(member: string): void {
    if (this.warned.has(member)) return;
    this.warned.add(member);
    this.logger.warn(
      `ChatInstance.${member} is not bridged; the call was ignored`,
    );
  }
}

/**
 * Members outside the bridged surface resolve to a logged no-op rather than
 * throwing. Adapters reach them from inside their own event callbacks, where
 * a throw becomes an unhandled rejection and kills the host process.
 */
export function createRemoteChat(options: RemoteChatOptions): ChatInstance {
  const chat = new RemoteChat(options);
  return new Proxy(chat, {
    get(target, property, receiver) {
      if (property in target) return Reflect.get(target, property, receiver);
      if (typeof property === "symbol") return undefined;
      return () => target.warnUnsupported(property);
    },
  }) as unknown as ChatInstance;
}
