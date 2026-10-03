export interface QueuedForward {
  id: string;
  method: string;
  params: unknown[];
  threadId?: string;
  firstAttemptAt: number;
  nextAttemptAt: number;
  attempts: number;
}

/** In memory by default; a shared store survives a host restart too. Entries are JSON-safe. */
export interface ForwardQueue {
  push(entry: QueuedForward): Promise<void>;
  /** Removes and returns what is due, oldest first. */
  takeDue(now: number): Promise<QueuedForward[]>;
  /** How many are waiting, for health checks. */
  size?(): Promise<number>;
}

/** `stopped`: held in memory by a host that stopped, so nothing else will retry it. */
export type DropReason = "expired" | "rejected" | "overflow" | "stopped";

let sequence = 0;

export function forwardEntry(
  method: string,
  params: unknown[],
  threadId: string | undefined,
): QueuedForward {
  const now = Date.now();
  return {
    id: `f${++sequence}-${now}`,
    method,
    params,
    threadId,
    firstAttemptAt: now,
    nextAttemptAt: now,
    attempts: 1,
  };
}

export type DroppedForwardHandler = (
  entry: QueuedForward,
  reason: DropReason,
  error: unknown,
) => void;

export function createMemoryForwardQueue(
  onOverflow: (entry: QueuedForward) => void,
  maxEntries = 1000,
): ForwardQueue {
  const entries: QueuedForward[] = [];
  return {
    async push(entry) {
      entries.push(entry);
      while (entries.length > maxEntries) onOverflow(entries.shift()!);
    },
    async takeDue(now) {
      const due = entries.filter((entry) => entry.nextAttemptAt <= now);
      for (const entry of due) entries.splice(entries.indexOf(entry), 1);
      return due.sort((a, b) => a.firstAttemptAt - b.firstAttemptAt);
    },
    async size() {
      return entries.length;
    },
  };
}

export interface ForwardRetryOptions {
  /** Wait before the first retry. Default one minute. */
  intervalMs?: number;
  /** Growth per retry. Default 1, a fixed interval. */
  backoff?: number;
  /** Ceiling for a growing interval. Default one hour. */
  maxIntervalMs?: number;
  /** Give up this long after the first attempt. Default 24 hours. */
  retentionMs?: number;
  /** Give up after this many attempts. Default unlimited within `retentionMs`. */
  maxAttempts?: number;
}

export interface RedeliveryOptions extends ForwardRetryOptions {
  queue: ForwardQueue;
  send: (method: string, params: unknown[]) => Promise<unknown>;
  isUndelivered: (error: unknown) => boolean;
  onDropped: DroppedForwardHandler;
}

export class Redelivery {
  private timer?: ReturnType<typeof setInterval>;
  private draining?: Promise<void>;

  private readonly intervalMs: number;
  private readonly backoff: number;
  private readonly maxIntervalMs: number;
  private readonly retentionMs: number;
  private readonly maxAttempts: number;

  constructor(private readonly options: RedeliveryOptions) {
    this.intervalMs = options.intervalMs ?? 60_000;
    this.backoff = options.backoff ?? 1;
    this.maxIntervalMs = options.maxIntervalMs ?? 3_600_000;
    this.retentionMs = options.retentionMs ?? 86_400_000;
    this.maxAttempts = options.maxAttempts ?? Number.POSITIVE_INFINITY;
  }

  private delayAfter(attempts: number): number {
    return Math.min(
      this.maxIntervalMs,
      this.intervalMs * this.backoff ** (attempts - 1),
    );
  }

  keep(entry: QueuedForward): Promise<void> {
    return this.options.queue.push({
      ...entry,
      nextAttemptAt: entry.firstAttemptAt + this.delayAfter(entry.attempts),
    });
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.drain(), this.intervalMs);
    this.timer.unref?.();
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = undefined;
  }

  drain(): Promise<void> {
    this.draining ??= this.attemptDue().finally(() => {
      this.draining = undefined;
    });
    return this.draining;
  }

  private async attemptDue(): Promise<void> {
    const now = Date.now();
    const due = await this.options.queue.takeDue(now);
    for (const [index, entry] of due.entries()) {
      try {
        await this.options.send(entry.method, entry.params);
      } catch (error) {
        if (!this.options.isUndelivered(error)) {
          this.options.onDropped(entry, "rejected", error);
          continue;
        }
        // Still down: the rest wait for the next pass rather than each probing.
        for (const waiting of due.slice(index)) {
          await this.reschedule(waiting, now, error, waiting === entry);
        }
        return;
      }
    }
  }

  private async reschedule(
    entry: QueuedForward,
    now: number,
    error: unknown,
    attempted: boolean,
  ): Promise<void> {
    const attempts = entry.attempts + (attempted ? 1 : 0);
    if (
      now - entry.firstAttemptAt >= this.retentionMs ||
      attempts >= this.maxAttempts
    ) {
      this.options.onDropped({ ...entry, attempts }, "expired", error);
      return;
    }
    await this.options.queue.push({
      ...entry,
      attempts,
      nextAttemptAt: now + this.delayAfter(attempts),
    });
  }
}
