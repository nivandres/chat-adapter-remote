import type { Attachment, Message } from "chat";
import { Message as MessageClass } from "chat";

import { decode, encode } from "./codec";

type SerializedMessage = Parameters<typeof MessageClass.fromJSON>[0];

/**
 * `Message.toJSON()` omits attachment `data`/`fetchData`, since a closure
 * cannot survive JSON. This resolves `fetchData` to a real Buffer first and
 * merges it back onto the serialized attachment. A failed resolve forwards
 * that attachment as metadata-only rather than failing the whole message.
 */
export async function serializeMessage(
  message: Message,
  onAttachmentError?: (attachment: Attachment, error: unknown) => void,
): Promise<unknown> {
  const serialized = message.toJSON();
  const attachments = await Promise.all(
    message.attachments.map(async (attachment, index) => {
      const base = serialized.attachments[index]!;
      if (attachment.data) return { ...base, data: attachment.data };
      if (typeof attachment.fetchData !== "function") return base;
      try {
        return { ...base, data: await attachment.fetchData() };
      } catch (error) {
        onAttachmentError?.(attachment, error);
        return base;
      }
    }),
  );
  return encode({ ...serialized, attachments });
}

export function deserializeMessage(wire: unknown): Message {
  const decoded = decode(wire) as SerializedMessage & {
    attachments: Array<{ data?: Buffer }>;
  };
  const message = MessageClass.fromJSON(decoded);
  message.attachments = message.attachments.map((attachment, index) => {
    const data = decoded.attachments?.[index]?.data;
    // `fetchData` is what the SDK reads: `toAiMessages` drops an image that
    // only carries `data`, and rehydration keys off its absence.
    return data
      ? { ...attachment, data, fetchData: async () => data }
      : attachment;
  });
  return message;
}
