import type { Lock, StateAdapter } from "chat";

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
  /** Resolves with any entries evicted to make room, reported as `overflow`. */
  push(entry: QueuedForward): Promise<QueuedForward[] | void>;
  /** Removes and returns what is due, oldest first. */
  takeDue(now: number): Promise<QueuedForward[]>;
  /** How many are waiting, for health checks. */
  size?(): Promise<number>;
  /** Forgets one the consumer acknowledged; needed by `journalForwards`. */
  remove?(id: string): Promise<void>;
}

/** `stopped`: held in memory by a host that stopped, so nothing else will retry it. */
export type DropReason = "expired" | "rejected" | "overflow" | "stopped";

export function forwardEntry(
  method: string,
  params: unknown[],
  threadId: string | undefined,
): QueuedForward {
  const now = Date.now();
  return {
    id: crypto.randomUUID(),
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

export function createMemoryForwardQueue(maxEntries = 1000): ForwardQueue {
  const entries: QueuedForward[] = [];
  return {
    async push(entry) {
      entries.push(entry);
      return entries.splice(0, Math.max(0, entries.length - maxEntries));
    },
    async takeDue(now) {
      const due = entries.filter((entry) => entry.nextAttemptAt <= now);
      for (const entry of due) entries.splice(entries.indexOf(entry), 1);
      return due.sort((a, b) => a.firstAttemptAt - b.firstAttemptAt);
    },
    async size() {
      return entries.length;
    },
    async remove(id) {
      const index = entries.findIndex((entry) => entry.id === id);
      if (index >= 0) entries.splice(index, 1);
    },
  };
}

export interface StateForwardQueueOptions {
  /** Prefix of its keys; give each host sharing a store its own. Default "chat-adapter-remote:forwards". */
  key?: string;
  /** Entries kept; past it the oldest are evicted. Default 1000. */
  maxEntries?: number;
  /** How long an entry lasts in the store; keep it above `forwardRetry.retentionMs`. Default 7 days. */
  ttlMs?: number;
}

interface IndexedForward {
  id: string;
  nextAttemptAt: number;
}

const LOCK_TTL_MS = 10_000;
const LOCK_RETRY_MS = 20;

/** A queue in a Chat SDK state adapter, so forwards outlive a restart and replicas share them. */
export function createStateForwardQueue(
  state: StateAdapter,
  options: StateForwardQueueOptions = {},
): ForwardQueue {
  const key = options.key ?? "chat-adapter-remote:forwards";
  const maxEntries = options.maxEntries ?? 1000;
  const ttlMs = options.ttlMs ?? 7 * 86_400_000;
  const entryKey = (id: string) => `${key}:${id}`;
  let connected: Promise<void> | undefined;
  let tail: Promise<unknown> = Promise.resolve();

  async function lock(): Promise<Lock> {
    const deadline = Date.now() + LOCK_TTL_MS;
    for (;;) {
      const held = await state.acquireLock(key, LOCK_TTL_MS);
      if (held) return held;
      if (Date.now() > deadline) {
        throw new Error(`chat-adapter-remote: could not lock ${key}`);
      }
      await new Promise((resolve) => setTimeout(resolve, LOCK_RETRY_MS));
    }
  }

  /** In turn within this process, under the store's lock across replicas. */
  function update<T>(
    change: (index: IndexedForward[]) => [IndexedForward[], T],
  ): Promise<T> {
    const run = tail.then(async () => {
      await (connected ??= state.connect());
      const held = await lock();
      try {
        const [index, result] = change(
          (await state.get<IndexedForward[]>(key)) ?? [],
        );
        await state.set(key, index);
        return result;
      } finally {
        await state.releaseLock(held);
      }
    });
    tail = run.catch(() => undefined);
    return run;
  }

  async function take(index: IndexedForward[]): Promise<QueuedForward[]> {
    const entries = await Promise.all(
      index.map(({ id }) => state.get<QueuedForward>(entryKey(id))),
    );
    await Promise.all(index.map(({ id }) => state.delete(entryKey(id))));
    return entries.filter((entry) => entry !== null);
  }

  return {
    async push(entry) {
      await (connected ??= state.connect());
      // The entry before its index, so a crash between them leaves an orphan the TTL clears, never a lost entry.
      await state.set(entryKey(entry.id), entry, ttlMs);
      const evicted = await update((index) => {
        const next = [
          ...index.filter(({ id }) => id !== entry.id),
          { id: entry.id, nextAttemptAt: entry.nextAttemptAt },
        ];
        const excess = Math.max(0, next.length - maxEntries);
        return [next.slice(excess), next.slice(0, excess)];
      });
      return take(evicted);
    },
    async takeDue(now) {
      const due = await update((index) => [
        index.filter(({ nextAttemptAt }) => nextAttemptAt > now),
        index.filter(({ nextAttemptAt }) => nextAttemptAt <= now),
      ]);
      return (await take(due)).sort(
        (a, b) => a.firstAttemptAt - b.firstAttemptAt,
      );
    },
    async size() {
      await (connected ??= state.connect());
      return ((await state.get<IndexedForward[]>(key)) ?? []).length;
    },
    async remove(id) {
      await update((index) => [index.filter((entry) => entry.id !== id), 0]);
      await state.delete(entryKey(id));
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
  private readonly sending = new Set<string>();

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
    this.sending.delete(entry.id);
    return this.schedule(entry);
  }

  /** Kept while this process sends it, so retries leave it alone and a crash does not lose it. */
  hold(entry: QueuedForward): Promise<void> {
    this.sending.add(entry.id);
    return this.schedule(entry);
  }

  /** Forgets one that was acknowledged or given up on. */
  async release(id: string): Promise<void> {
    this.sending.delete(id);
    await this.options.queue.remove?.(id);
  }

  private schedule(entry: QueuedForward): Promise<void> {
    return this.store({
      ...entry,
      nextAttemptAt: entry.firstAttemptAt + this.delayAfter(entry.attempts),
    });
  }

  private async store(entry: QueuedForward): Promise<void> {
    for (const evicted of (await this.options.queue.push(entry)) ?? []) {
      this.options.onDropped(evicted, "overflow", undefined);
    }
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
    const taken = await this.options.queue.takeDue(now);
    for (const entry of taken) {
      if (this.sending.has(entry.id)) await this.store(entry);
    }
    const due = taken.filter((entry) => !this.sending.has(entry.id));
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
    await this.store({
      ...entry,
      attempts,
      nextAttemptAt: now + this.delayAfter(attempts),
    });
  }
}
