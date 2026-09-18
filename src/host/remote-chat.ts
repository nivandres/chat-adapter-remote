import type {
  Adapter,
  ChatInstance,
  Logger,
  Message,
  WebhookOptions,
} from "chat";
import { ConsoleLogger } from "chat";

import { serializeMessage } from "../rpc/message-wire";
import { createRpcClient, type RpcClient } from "../rpc/transport";
import { createBridgingLogger } from "./logger-bridge";
import type { FetchLike } from "../types";

export interface RemoteChatOptions {
  consumerUrl: string;
  secret: string;
  timeoutMs?: number;
  logger?: Logger;
  fetch?: FetchLike;
}

class RemoteChat {
  readonly logger: Logger;
  private readonly rpc: RpcClient;
  private readonly warned = new Set<string>();

  constructor(options: RemoteChatOptions) {
    this.logger =
      options.logger ?? new ConsoleLogger("info", "chat-adapter-remote");
    this.rpc = createRpcClient({
      url: options.consumerUrl,
      secret: options.secret,
      timeoutMs: options.timeoutMs,
      fetch: options.fetch,
    });
  }

  /**
   * Adapters call this unawaited from their own event loops, so it must
   * never reject: a rejection here would surface as an unhandled rejection
   * and take down the process holding the platform connection.
   */
  async processMessage(
    adapter: Adapter,
    threadId: string,
    message: Message | (() => Promise<Message>),
    _options?: WebhookOptions,
  ): Promise<void> {
    try {
      const resolved =
        typeof message === "function" ? await message() : message;
      const wire = await serializeMessage(resolved, (attachment, error) =>
        this.logger.warn(
          "attachment data unavailable, forwarding metadata only",
          { attachment, error },
        ),
      );
      await this.rpc.request("processMessage", [
        threadId,
        wire,
        adapter.channelIdFromThreadId(threadId),
        adapter.isDM?.(threadId),
      ]);
    } catch (error) {
      this.logger.error("failed to deliver message to consumer", {
        threadId,
        error,
      });
    }
  }

  getLogger(prefix?: string): Logger {
    return createBridgingLogger({
      prefix,
      localLogger: prefix ? this.logger.child(prefix) : this.logger,
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
