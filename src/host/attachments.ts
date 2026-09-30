import type { AttachmentBytes } from "../rpc/message-wire";
import { ExpiringMap } from "./expiring";

/** `"auto"` inlines while the body budget allows. */
export type InlineAttachments = boolean | "auto";

const BASE64_RATIO = 4 / 3;
/** Below what serverless runtimes accept as a request body. */
export const DEFAULT_ATTACHMENT_BUDGET = 4_000_000;
const BUDGET_SHARE = 0.8;

/** Holds the adapter's own way of reading an attachment; knows nothing about which adapter. */
export class AttachmentRegistry {
  private readonly held: ExpiringMap<() => Promise<AttachmentBytes>>;

  constructor(ttlMs: number) {
    this.held = new ExpiringMap(ttlMs);
  }

  hold(read: () => Promise<AttachmentBytes>): string {
    return this.held.add("a", read);
  }

  /** Kept until it expires: `fetchData` may be called more than once. */
  read(id: string): Promise<AttachmentBytes> | undefined {
    const read = this.held.get(id);
    if (!read) return undefined;
    this.held.touch(id);
    return read();
  }

  clear(): void {
    this.held.clear();
  }
}

export class AttachmentBudget {
  private remaining: number;

  constructor(
    private readonly mode: InlineAttachments,
    maxBodyBytes: number,
  ) {
    this.remaining = maxBodyBytes * BUDGET_SHARE;
  }

  allows(size: number | undefined): boolean {
    if (this.mode !== "auto") return this.mode;
    if (size === undefined) return true;
    return Math.ceil(size * BASE64_RATIO) <= this.remaining;
  }

  take(size: number): void {
    this.remaining -= Math.ceil(size * BASE64_RATIO);
  }
}
