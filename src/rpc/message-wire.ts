import type { Attachment, Message } from "chat";
import { Message as MessageClass } from "chat";

import type { AttachmentBudget, AttachmentRegistry } from "../host/attachments";
import { decode, encode } from "./codec";

type SerializedMessage = Parameters<typeof MessageClass.fromJSON>[0];

/** Tag the consumer reads to know it has to fetch the bytes rather than decode them. */
export const ATTACHMENT_REF = "__charAttachment";

/** Normalised on the way out, so everything downstream handles one shape. */
export type AttachmentBytes = Buffer;

export interface AttachmentPolicy {
  budget: AttachmentBudget;
  registry: AttachmentRegistry;
}

type WireAttachment = SerializedMessage["attachments"][number] & {
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

/**
 * `Message.toJSON()` omits attachment `data`/`fetchData`, since a closure
 * cannot survive JSON, so the bytes have to be carried deliberately: inlined
 * when they fit, otherwise left here behind an id the consumer can fetch.
 * An attachment the adapter gave no way to read is forwarded as metadata.
 */
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

    // Answering by reference on the reported size alone means an oversized
    // attachment is never downloaded here at all.
    if (policy && !policy.budget.allows(attachment.size)) {
      attachments.push({
        ...base,
        [ATTACHMENT_REF]: policy.registry.hold(resolve),
      });
      continue;
    }

    try {
      const data = await resolve();
      if (policy && !policy.budget.allows(data.byteLength)) {
        const held = async () => data;
        attachments.push({
          ...base,
          [ATTACHMENT_REF]: policy.registry.hold(held),
        });
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

/** Fetches the bytes the host kept back, given the id it sent instead. */
export type AttachmentResolver = (id: string) => Promise<AttachmentBytes>;

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
    const held = wired?.[ATTACHMENT_REF];
    // `fetchData` is what the SDK reads: `toAiMessages` drops an image that
    // only carries `data`, and rehydration keys off its absence.
    if (held && resolve) {
      return { ...attachment, fetchData: () => resolve(held) };
    }
    const data = wired?.data;
    return data
      ? { ...attachment, data, fetchData: async () => data }
      : attachment;
  });

  return message;
}
