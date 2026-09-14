import { z } from "zod";

/**
 * A closed allowlist of method names, each with its own strict argument
 * tuple. The dispatcher must never index the real object with a raw string
 * from the wire — only a method that parses against one of these two unions
 * is dispatched. `channelIdFromThreadId` is intentionally absent: Chat SDK
 * core calls it synchronously, so it's answered locally instead — see
 * RemoteAdapter.channelIdFromThreadId and RemoteChat.processMessage.
 */

const ThreadId = z.string();
const MessageId = z.string();
const Id = z.union([z.string(), z.number()]);

export const FetchOptionsSchema = z.strictObject({
  cursor: z.string().optional(),
  direction: z.enum(["forward", "backward"]).optional(),
  limit: z.number().int().positive().optional(),
});

export const TypingOptionsSchema = z.strictObject({
  initiatorUserId: z.string().optional(),
});

const BufferWireSchema = z.strictObject({
  __chatAdapterRemoteBuffer: z.literal(true),
  base64: z.string(),
});

const AttachmentWireSchema = z.strictObject({
  type: z.enum(["image", "file", "video", "audio"]),
  url: z.string().optional(),
  name: z.string().optional(),
  mimeType: z.string().optional(),
  size: z.number().optional(),
  width: z.number().optional(),
  height: z.number().optional(),
  fetchMetadata: z.record(z.string(), z.string()).optional(),
  data: BufferWireSchema.optional(),
});

const FileUploadWireSchema = z.strictObject({
  filename: z.string(),
  mimeType: z.string().optional(),
  data: BufferWireSchema,
});

/** Mirrors AdapterPostableMessage's five variants (CardElement collapses into the opaque `card` passthrough). */
export const AdapterPostableMessageSchema: z.ZodType<unknown> = z.union([
  z.string(),
  z.strictObject({
    raw: z.string(),
    attachments: z.array(AttachmentWireSchema).optional(),
    files: z.array(FileUploadWireSchema).optional(),
  }),
  z.strictObject({
    markdown: z.string(),
    attachments: z.array(AttachmentWireSchema).optional(),
    files: z.array(FileUploadWireSchema).optional(),
  }),
  z.strictObject({
    ast: z.unknown(),
    attachments: z.array(AttachmentWireSchema).optional(),
    files: z.array(FileUploadWireSchema).optional(),
  }),
  z.strictObject({
    card: z.unknown(),
    fallbackText: z.string().optional(),
    files: z.array(FileUploadWireSchema).optional(),
  }),
]);

/** Mirrors SerializedMessage (Message.toJSON()'s output) plus attachment binary data re-attached at the wire boundary. */
export const WireMessageSchema = z.strictObject({
  _type: z.literal("chat:Message"),
  id: z.string(),
  threadId: z.string(),
  text: z.string(),
  formatted: z.unknown(),
  raw: z.unknown(),
  author: z.strictObject({
    userId: z.string(),
    userName: z.string(),
    fullName: z.string(),
    email: z.string().optional(),
    isBot: z.union([z.boolean(), z.literal("unknown")]),
    isMe: z.boolean(),
    isSystem: z.boolean().optional(),
  }),
  metadata: z.strictObject({
    dateSent: z.string(),
    edited: z.boolean(),
    editedAt: z.string().optional(),
  }),
  attachments: z.array(AttachmentWireSchema),
  links: z
    .array(
      z.strictObject({
        url: z.string(),
        title: z.string().optional(),
        description: z.string().optional(),
        imageUrl: z.string().optional(),
        siteName: z.string().optional(),
      }),
    )
    .optional(),
  isMention: z.boolean().optional(),
  userKey: z.string().optional(),
});

/** Outbound: consumer -> host, dispatched into the real Adapter. */
export const OUTBOUND_CALLS = z.discriminatedUnion("method", [
  z.strictObject({
    method: z.literal("__handshake"),
    id: Id,
    params: z.tuple([]),
  }),
  z.strictObject({
    method: z.literal("postMessage"),
    id: Id,
    params: z.tuple([ThreadId, AdapterPostableMessageSchema]),
  }),
  z.strictObject({
    method: z.literal("editMessage"),
    id: Id,
    params: z.tuple([ThreadId, MessageId, AdapterPostableMessageSchema]),
  }),
  z.strictObject({
    method: z.literal("deleteMessage"),
    id: Id,
    params: z.tuple([ThreadId, MessageId]),
  }),
  z.strictObject({
    method: z.literal("addReaction"),
    id: Id,
    params: z.tuple([ThreadId, MessageId, z.string()]),
  }),
  z.strictObject({
    method: z.literal("removeReaction"),
    id: Id,
    params: z.tuple([ThreadId, MessageId, z.string()]),
  }),
  // `.nullish()`, not `.optional()`: JSON.stringify turns an omitted array
  // element into `null`, not `undefined`.
  z.strictObject({
    method: z.literal("fetchMessages"),
    id: Id,
    params: z.tuple([ThreadId, FetchOptionsSchema.nullish()]),
  }),
  z.strictObject({
    method: z.literal("fetchThread"),
    id: Id,
    params: z.tuple([ThreadId]),
  }),
  z.strictObject({
    method: z.literal("startTyping"),
    id: Id,
    params: z.tuple([
      ThreadId,
      z.string().nullish(),
      TypingOptionsSchema.nullish(),
    ]),
  }),
  z.strictObject({
    method: z.literal("disconnect"),
    id: Id,
    params: z.tuple([]),
  }),
]);
export type OutboundCall = z.infer<typeof OUTBOUND_CALLS>;
export type OutboundMethod = OutboundCall["method"];

/** Inbound: host -> consumer, dispatched into the real Chat instance. */
export const INBOUND_CALLS = z.discriminatedUnion("method", [
  z.strictObject({
    method: z.literal("processMessage"),
    id: Id,
    params: z.tuple([
      ThreadId,
      WireMessageSchema,
      z.string() /* channelId, computed host-side */,
    ]),
  }),
  z.strictObject({
    method: z.literal("log"),
    id: z.undefined().optional(),
    params: z.tuple([
      z.enum(["debug", "info", "warn", "error"]),
      z.string(),
      z.string(),
      z.array(z.unknown()),
    ]),
  }),
]);
export type InboundCall = z.infer<typeof INBOUND_CALLS>;
export type InboundMethod = InboundCall["method"];
