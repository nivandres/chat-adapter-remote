import type { Logger } from "chat";

export type LogLevel = "debug" | "info" | "warn" | "error";

const RANK: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export interface BridgingLoggerOptions {
  prefix?: string;
  localLogger: Logger;
  notify: (
    level: LogLevel,
    prefix: string,
    message: string,
    args: unknown[],
  ) => void;
  /** Lines below this level stay local instead of crossing the wire. Default "info". */
  forwardLevel?: LogLevel;
}

/** Logs locally and mirrors lines at or above `forwardLevel` to the consumer. */
export function createBridgingLogger(options: BridgingLoggerOptions): Logger {
  const prefix = options.prefix ?? "";
  const threshold = RANK[options.forwardLevel ?? "info"];

  function log(level: LogLevel, message: string, args: unknown[]): void {
    options.localLogger[level](message, ...args);
    if (RANK[level] >= threshold) options.notify(level, prefix, message, args);
  }

  return {
    child: (childPrefix: string) =>
      createBridgingLogger({
        ...options,
        prefix: prefix ? `${prefix}:${childPrefix}` : childPrefix,
        localLogger: options.localLogger.child(childPrefix),
      }),
    debug: (message, ...args) => log("debug", message, args),
    info: (message, ...args) => log("info", message, args),
    warn: (message, ...args) => log("warn", message, args),
    error: (message, ...args) => log("error", message, args),
  };
}
