import type { AttachmentBytes } from "../rpc/message-wire";
import { ExpiringMap } from "./expiring";

/**
 * Whether attachment bytes travel inside the message or stay on the host to be
 * fetched by id.
 *
 * `"auto"` inlines while the message still fits the body budget, so small
 * media stays in one round trip and large media cannot produce a body the
 * consumer's runtime would reject.
 */
export type InlineAttachments = boolean | "auto";

const BASE64_RATIO = 4 / 3;
/** The rest of the message, the envelope and the signature share the body too. */
const BUDGET_SHARE = 0.8;

/**
 * Keeps whatever the adapter already knew how to do — its `data`, or the
 * `fetchData` closure it built — so the consumer can ask for the bytes later.
 * Nothing here knows which adapter produced them.
 */
export class AttachmentRegistry {
  private readonly held: ExpiringMap<() => Promise<AttachmentBytes>>;

  constructor(ttlMs: number) {
    this.held = new ExpiringMap(ttlMs);
  }

  hold(read: () => Promise<AttachmentBytes>): string {
    return this.held.add("a", read);
  }

  /** Single use: the consumer rebuilds its own closure around what it receives. */
  read(id: string): Promise<AttachmentBytes> | undefined {
    const read = this.held.get(id);
    if (!read) return undefined;
    this.held.delete(id);
    return read();
  }

  clear(): void {
    this.held.clear();
  }
}

/** How much of one message body is still available for inlined bytes. */
export class AttachmentBudget {
  private remaining: number;

  constructor(
    private readonly mode: InlineAttachments,
    maxBodyBytes: number,
  ) {
    this.remaining = maxBodyBytes * BUDGET_SHARE;
  }

  /** `size` is what the platform reported, if it reported anything. */
  allows(size: number | undefined): boolean {
    if (this.mode !== "auto") return this.mode;
    if (size === undefined) return true;
    return Math.ceil(size * BASE64_RATIO) <= this.remaining;
  }

  take(size: number): void {
    this.remaining -= Math.ceil(size * BASE64_RATIO);
  }
}
