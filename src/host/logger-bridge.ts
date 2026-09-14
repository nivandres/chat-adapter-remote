import type { Logger } from "chat";

export type LogLevel = "debug" | "info" | "warn" | "error";

export interface BridgingLoggerOptions {
  prefix?: string;
  localLogger: Logger;
  /** Fire-and-forget mirror to the consumer's real logger. Never awaited by callers. */
  notify: (
    level: LogLevel,
    prefix: string,
    message: string,
    args: unknown[],
  ) => Promise<void>;
}

/** A synchronous, void-returning Logger that also best-effort mirrors each line to the consumer as a fire-and-forget notification; failures are swallowed. */
export function createBridgingLogger(options: BridgingLoggerOptions): Logger {
  const prefix = options.prefix ?? "";

  function log(level: LogLevel, message: string, args: unknown[]): void {
    options.localLogger[level](message, ...args);
    options.notify(level, prefix, message, args).catch(() => {});
  }

  return {
    child: (childPrefix: string) =>
      createBridgingLogger({
        prefix: prefix ? `${prefix}:${childPrefix}` : childPrefix,
        localLogger: options.localLogger.child(childPrefix),
        notify: options.notify,
      }),
    debug: (message, ...args) => log("debug", message, args),
    info: (message, ...args) => log("info", message, args),
    warn: (message, ...args) => log("warn", message, args),
    error: (message, ...args) => log("error", message, args),
  };
}
