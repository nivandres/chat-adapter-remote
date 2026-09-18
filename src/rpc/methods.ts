import { z } from "zod";

export const PROTOCOL_VERSION = 1;

const ThreadId = z.string();
const MessageId = z.string();
const Id = z.union([z.string(), z.number()]);

const FetchOptionsSchema = z.looseObject({
  cursor: z.string().optional(),
  direction: z.enum(["forward", "backward"]).optional(),
  limit: z.number().int().positive().optional(),
});

const TypingOptionsSchema = z.looseObject({
  initiatorUserId: z.string().optional(),
});

/**
 * Payload shapes are deliberately loose. They mirror types owned by `chat`,
 * which may gain fields in any release; rejecting an unknown field would
 * drop real messages. The method allowlist below is the security boundary.
 */
const PostableSchema = z.union([z.string(), z.looseObject({})]);

export const WireMessageSchema = z.looseObject({
  _type: z.literal("chat:Message"),
  id: z.string(),
  threadId: z.string(),
});

export const HandshakeSchema = z.looseObject({
  protocolVersion: z.number().int(),
  name: z.string(),
  userName: z.string(),
  botUserId: z.string().optional(),
  isDM: z.boolean().optional(),
  lockScope: z.enum(["thread", "channel"]).optional(),
  persistThreadHistory: z.boolean().optional(),
  supportsTurnCancellation: z.boolean().optional(),
});

export type Handshake = z.infer<typeof HandshakeSchema>;

/** Consumer -> host, dispatched into the real adapter. */
export const OUTBOUND_CALLS = z.discriminatedUnion("method", [
  z.strictObject({
    method: z.literal("__handshake"),
    id: Id,
    params: z.tuple([]),
  }),
  z.strictObject({
    method: z.literal("postMessage"),
    id: Id,
    params: z.tuple([ThreadId, PostableSchema]),
  }),
  z.strictObject({
    method: z.literal("editMessage"),
    id: Id,
    params: z.tuple([ThreadId, MessageId, PostableSchema]),
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
  // `.nullish()`, not `.optional()`: JSON.stringify turns an omitted array element into `null`.
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

/** Host -> consumer, dispatched into the real Chat instance. */
export const INBOUND_CALLS = z.discriminatedUnion("method", [
  z.strictObject({
    method: z.literal("processMessage"),
    id: Id,
    params: z.tuple([
      ThreadId,
      WireMessageSchema,
      z.string(),
      z.boolean().nullish(),
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
