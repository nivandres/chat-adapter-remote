/** Cheap to run again and possibly large, so never remembered. */
const READS = new Set([
  "__handshake",
  "fetchMessages",
  "fetchThread",
  "fetchMessage",
  "fetchChannelInfo",
  "fetchChannelMessages",
  "fetchSubject",
  "listThreads",
  "getUser",
  "fetchAttachment",
  "rehydrateAttachment",
]);
/** How long a finished call is remembered, well past any consumer's retries. */
const REMEMBER_MS = 120_000;
const MAX_REMEMBERED = 10_000;
const SWEEP_EVERY_MS = 10_000;

interface Entry {
  result: Promise<unknown>;
  settledAt?: number;
}

/** Answers a retried call with the first run's outcome, rather than running it twice. */
export class CallLedger {
  private readonly calls = new Map<string, Entry>();
  private sweptAt = 0;

  /** Numeric ids are per-process counters from consumers before 0.8.3, so only string ids name a call. */
  run(
    id: string | number,
    method: string,
    call: () => Promise<unknown>,
  ): Promise<unknown> {
    if (typeof id !== "string" || READS.has(method)) return call();
    this.sweep();
    const key = `${method} ${id}`;
    const known = this.calls.get(key);
    if (known) return known.result;

    const entry: Entry = { result: call() };
    const settle = () => {
      entry.settledAt = Date.now();
    };
    entry.result.then(settle, settle);
    this.calls.set(key, entry);
    if (this.calls.size > MAX_REMEMBERED) {
      this.calls.delete(this.calls.keys().next().value!);
    }
    return entry.result;
  }

  clear(): void {
    this.calls.clear();
  }

  /** Periodic rather than per call, which would walk every entry each time. */
  private sweep(): void {
    const now = Date.now();
    if (now - this.sweptAt < SWEEP_EVERY_MS) return;
    this.sweptAt = now;
    for (const [key, entry] of this.calls) {
      if (
        entry.settledAt !== undefined &&
        now - entry.settledAt > REMEMBER_MS
      ) {
        this.calls.delete(key);
      }
    }
  }
}
