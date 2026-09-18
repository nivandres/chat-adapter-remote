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
/** What `"auto"` weighs against when no body limit was set: runtimes cap request bodies around here. */
export const DEFAULT_ATTACHMENT_BUDGET = 4_000_000;
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

  /** Kept until it expires: `fetchData` carries no promise of being called once. */
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
