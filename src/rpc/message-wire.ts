import type { Attachment, Message } from "chat";
import { Message as MessageClass } from "chat";

import { decodeBuffers, encodeBuffers } from "./buffers";
import type { WireMessageSchema } from "./methods";
import type { z } from "zod";

type WireMessage = z.infer<typeof WireMessageSchema>;

/**
 * `Message.toJSON()` omits attachment `data`/`fetchData` since a closure
 * can't survive JSON, so this resolves any `fetchData` to a real Buffer
 * first and merges it back onto the serialized attachment before encoding.
 * A failed `fetchData()` forwards that one attachment as metadata-only
 * instead of failing the whole message.
 */
export async function serializeMessageForWire(
  message: Message,
  onFetchDataError?: (attachment: Attachment, error: unknown) => void,
): Promise<unknown> {
  const serialized = message.toJSON();
  const attachmentsWithData = await Promise.all(
    message.attachments.map(async (attachment, index) => {
      const base = serialized.attachments[index]!;
      if (attachment.data) return { ...base, data: attachment.data };
      if (typeof attachment.fetchData === "function") {
        try {
          const data = await attachment.fetchData();
          return { ...base, data };
        } catch (error) {
          onFetchDataError?.(attachment, error);
          return base;
        }
      }
      return base;
    }),
  );
  return encodeBuffers({ ...serialized, attachments: attachmentsWithData });
}

/** Inverse of {@link serializeMessageForWire}: decodes buffers, reconstructs via `Message.fromJSON`, then patches attachment data back on. */
export function deserializeMessageFromWire(wire: unknown): Message {
  const decoded = decodeBuffers(wire) as WireMessage & {
    attachments: Array<{ data?: Buffer }>;
  };
  const message = MessageClass.fromJSON(
    decoded as Parameters<typeof MessageClass.fromJSON>[0],
  );
  message.attachments = message.attachments.map((attachment, index) => {
    const data = decoded.attachments[index]?.data;
    return data ? { ...attachment, data } : attachment;
  });
  return message;
}
