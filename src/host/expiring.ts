/**
 * Holds values the consumer reaches by id — a stream being written to, the
 * bytes behind an attachment — and drops the ones it stops asking about.
 *
 * The sweep runs on use and, while anything is held, on a timer that is
 * unref'd so it never keeps the process alive.
 */
export class ExpiringMap<T> {
  private readonly entries = new Map<string, { value: T; expiresAt: number }>();
  private sweeper?: ReturnType<typeof setInterval>;
  private sequence = 0;

  constructor(
    private readonly ttlMs: number,
    private readonly onExpire: (value: T) => void = () => {},
  ) {}

  get size(): number {
    return this.entries.size;
  }

  add(prefix: string, value: T): string {
    this.sweep();
    const id = `${prefix}${++this.sequence}`;
    this.entries.set(id, { value, expiresAt: Date.now() + this.ttlMs });
    this.watch();
    return id;
  }

  /** Sweeps first, so an expired id reads as missing rather than stale. */
  get(id: string): T | undefined {
    this.sweep();
    return this.entries.get(id)?.value;
  }

  touch(id: string): void {
    const entry = this.entries.get(id);
    if (entry) entry.expiresAt = Date.now() + this.ttlMs;
  }

  delete(id: string): void {
    this.entries.delete(id);
  }

  clear(): void {
    for (const entry of this.entries.values()) this.onExpire(entry.value);
    this.entries.clear();
    this.unwatch();
  }

  private sweep(): void {
    const now = Date.now();
    for (const [id, entry] of this.entries) {
      if (entry.expiresAt > now) continue;
      this.entries.delete(id);
      this.onExpire(entry.value);
    }
    if (this.entries.size === 0) this.unwatch();
  }

  private watch(): void {
    if (this.sweeper) return;
    this.sweeper = setInterval(() => this.sweep(), this.ttlMs);
    this.sweeper.unref?.();
  }

  private unwatch(): void {
    if (!this.sweeper) return;
    clearInterval(this.sweeper);
    this.sweeper = undefined;
  }
}
