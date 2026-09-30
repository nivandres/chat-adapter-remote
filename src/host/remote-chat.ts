import type {
  Adapter,
  ChatInstance,
  EmojiValue,
  Logger,
  Message,
  StateAdapter,
  WebhookOptions,
} from "chat";
import { ConsoleLogger } from "chat";

import { encode } from "../rpc/codec";
import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  isTransient,
  isUndelivered,
} from "../rpc/errors";
import { serializeMessage, type AttachmentPolicy } from "../rpc/message-wire";
import { EVENT_MESSAGE_KEYS } from "../rpc/methods";
import { createRpcClient, type RpcClient } from "../rpc/transport";
import { createBridgingLogger, type LogLevel } from "./logger-bridge";
import {
  forwardEntry,
  type DroppedForwardHandler,
  type QueuedForward,
} from "./delivery";
import { createLocalState, createRemoteState } from "./state";
import type { FetchLike } from "../types";

export type HostErrorPhase =
  "initialize" | "forward" | "dispatch" | "shutdown" | "adapter";

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
  redeliver?: (entry: QueuedForward) => Promise<void>;
  dropped?: DroppedForwardHandler;
  attachments?: () => AttachmentPolicy;
  state?: StateAdapter;
  /** Lines below this level stay on the host instead of crossing the wire. Default "info". */
  logForwardLevel?: LogLevel;
  /** Inbound messages forwarded at once. Default 8. */
  maxConcurrentForwards?: number;
  /** Messages allowed to queue behind those. Default 1000; past it, forwarding fails rather than growing. */
  maxQueuedForwards?: number;
}

/** Arrivals queue behind any backlog, so a latecomer cannot overtake it. */
class ForwardBacklogError extends Error {}

function createLimiter(limit: number, maxQueued: number) {
  const waiting: Array<() => void> = [];
  let active = 0;

  return async function run<T>(task: () => Promise<T>): Promise<T> {
    if (active >= limit || waiting.length > 0) {
      if (waiting.length >= maxQueued) {
        throw new ForwardBacklogError(
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

type StateStore = Record<string, (...args: unknown[]) => Promise<unknown>>;

function isStateRefused(error: unknown): boolean {
  return (
    error instanceof RemoteAdapterRpcError &&
    error.code === RpcErrorCode.STATE_UNAVAILABLE
  );
}

const LOCAL_EVENT_KEYS = ["adapter", "thread", "channel"] as const;

class RemoteChat {
  readonly logger: Logger;
  private readonly rpc: RpcClient;
  private readonly warned = new Set<string>();
  private state?: StateAdapter;
  private localState?: StateStore;
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

  // Adapters call these unawaited, so a rejection would end the process.

  private async forward(
    method: string,
    params: unknown[],
    threadId?: string,
  ): Promise<void> {
    try {
      await this.limit(() => this.rpc.request(method, params));
    } catch (error) {
      await this.undelivered(method, params, threadId, error);
    }
  }

  /** A timed-out forward may have been handled, so it is never sent twice. */
  private async undelivered(
    method: string,
    params: unknown[],
    threadId: string | undefined,
    error: unknown,
  ): Promise<void> {
    if (isUndelivered(error) && this.options.redeliver) {
      this.logger.warn(`${method} did not reach the consumer, keeping it`, {
        threadId,
        error,
      });
      const wire = (await encode(params)) as unknown[];
      await this.options.redeliver(forwardEntry(method, wire, threadId));
      return;
    }
    this.logger.error(`failed to deliver ${method} to consumer`, {
      threadId,
      error,
    });
    this.options.onError?.(error, { phase: "forward", threadId });
    if (isTransient(error) && !isUndelivered(error)) return;
    const reason =
      error instanceof ForwardBacklogError
        ? "overflow"
        : isUndelivered(error)
          ? "expired"
          : "rejected";
    this.options.dropped?.(
      forwardEntry(method, params, threadId),
      reason,
      error,
    );
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

    const policy = this.options.attachments?.();
    for (const key of EVENT_MESSAGE_KEYS) {
      const message = await this.resolveMessage(wire[key]);
      if (message) wire[key] = await serializeMessage(message, policy);
      else delete wire[key];
    }
    // EmojiValue.toJSON() gives a placeholder, not the name.
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

  private async askEvent(
    method: string,
    event: EventPayload,
    contextId?: string,
  ): Promise<unknown> {
    const threadId = event.threadId as string | undefined;
    try {
      const wire = await this.serializeEvent(event);
      const params = contextId === undefined ? [wire] : [wire, contextId];
      // Outside the forward limiter: the platform times these out in seconds, and a human paces them.
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

  getState(): StateAdapter {
    this.state ??=
      this.options.state ??
      createRemoteState(async (operation, args) => {
        // Refused means our own store, rather than handing the adapter nothing.
        const local = this.localState;
        if (local) return local[operation]!(...args);
        try {
          return await this.rpc.request("state", [operation, args]);
        } catch (error) {
          if (!isStateRefused(error)) throw error;
          this.logger.warn(
            "the consumer does not lend its state; falling back to a local store that is not persisted",
          );
          this.localState = createLocalState() as unknown as StateStore;
          return this.localState[operation]!(...args);
        }
      });
    return this.state;
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

/** Unbridged members are a logged no-op: a throw inside an adapter's callback would end the process. */
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
