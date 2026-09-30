import type { Attachment, Message } from "chat";
import { Message as MessageClass } from "chat";

import type { AttachmentBudget, AttachmentRegistry } from "../host/attachments";
import { decode, encode } from "./codec";

type SerializedMessage = Parameters<typeof MessageClass.fromJSON>[0];

export const ATTACHMENT_REF = "__charAttachment";

export type AttachmentBytes = Buffer;

export interface AttachmentPolicy {
  budget: AttachmentBudget;
  registry: AttachmentRegistry;
  /** The adapter can rebuild it from `fetchMetadata`, so nothing needs holding here. */
  rehydratable: boolean;
}

export type WireAttachment = SerializedMessage["attachments"][number] & {
  data?: AttachmentBytes;
  [ATTACHMENT_REF]?: string;
};

function read(
  attachment: Attachment,
): (() => Promise<AttachmentBytes>) | undefined {
  if (attachment.data) {
    const { data } = attachment;
    return async () =>
      Buffer.isBuffer(data) ? data : Buffer.from(await data.arrayBuffer());
  }
  if (typeof attachment.fetchData !== "function") return undefined;
  const fetchData = attachment.fetchData.bind(attachment);
  return async () => {
    const raw = await fetchData();
    return Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
  };
}

/** `toJSON()` drops `data`/`fetchData`, so bytes are inlined when they fit or held behind an id. */
export async function serializeMessage(
  message: Message,
  policy?: AttachmentPolicy,
  onAttachmentError?: (attachment: Attachment, error: unknown) => void,
): Promise<unknown> {
  const serialized = message.toJSON();
  const attachments: WireAttachment[] = [];

  for (const [index, attachment] of message.attachments.entries()) {
    const base = serialized.attachments[index]! as WireAttachment;
    const resolve = read(attachment);
    if (!resolve) {
      attachments.push(base);
      continue;
    }

    const defer = (read: () => Promise<AttachmentBytes>) => {
      if (policy!.rehydratable && attachment.fetchMetadata) {
        attachments.push(base);
        return;
      }
      attachments.push({
        ...base,
        [ATTACHMENT_REF]: policy!.registry.hold(read),
      });
    };

    // Deciding on the reported size means an oversized attachment is never downloaded here.
    if (policy && !policy.budget.allows(attachment.size)) {
      defer(resolve);
      continue;
    }

    try {
      const data = await resolve();
      if (policy && !policy.budget.allows(data.byteLength)) {
        defer(async () => data);
        continue;
      }
      policy?.budget.take(data.byteLength);
      attachments.push({ ...base, data });
    } catch (error) {
      onAttachmentError?.(attachment, error);
      attachments.push(base);
    }
  }

  return encode({ ...serialized, attachments });
}

export type AttachmentResolver = (
  wired: WireAttachment,
) => (() => Promise<AttachmentBytes>) | undefined;

export function deserializeMessage(
  wire: unknown,
  resolve?: AttachmentResolver,
): Message {
  const decoded = decode(wire) as SerializedMessage & {
    attachments: WireAttachment[];
  };
  const message = MessageClass.fromJSON(decoded);

  message.attachments = message.attachments.map((attachment, index) => {
    const wired = decoded.attachments?.[index];
    // The SDK reads `fetchData`: `toAiMessages` drops an image that only has `data`.
    const data = wired?.data;
    if (data) return { ...attachment, data, fetchData: async () => data };

    const fetchData = wired && resolve ? resolve(wired) : undefined;
    return fetchData ? { ...attachment, fetchData } : attachment;
  });

  return message;
}
