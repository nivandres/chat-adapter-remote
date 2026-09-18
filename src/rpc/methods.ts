import { z } from "zod";

export const PROTOCOL_VERSION = 2;

const ThreadId = z.string();
const MessageId = z.string();
const ChannelId = z.string();
const Id = z.union([z.string(), z.number()]);

/** Optional `Adapter` members the host reports and the consumer removes when unsupported. Synchronous members travel as per-message facts instead. */
export const OPTIONAL_CAPABILITIES = [
  "disconnect",
  "reply",
  "endTyping",
  "markAsRead",
  "listThreads",
  "getUser",
  "postObject",
  "editObject",
  "stream",
  "scheduleMessage",
  "rehydrateAttachment",
  "openDM",
  "openModal",
  "postEphemeral",
  "postChannelMessage",
  "fetchMessage",
  "fetchChannelInfo",
  "fetchChannelMessages",
  "fetchSubject",
  "onThreadSubscribe",
] as const;

export type OptionalCapability = (typeof OPTIONAL_CAPABILITIES)[number];

// Payloads stay loose: they mirror `chat`-owned types that gain fields between
// releases, and the method allowlist is the security boundary, not the shape.

const Loose = z.object({}).passthrough();
const PostableSchema = z.union([z.string(), Loose]);
const StreamChunkSchema = z.union([z.string(), Loose]);

export const WireMessageSchema = z
  .object({
    _type: z.literal("chat:Message"),
    id: z.string(),
    threadId: z.string(),
  })
  .passthrough();

/** Facts the host derives from the real adapter's synchronous members. */
export const ThreadFactsSchema = z
  .object({
    channelId: z.string(),
    isDM: z.boolean().optional(),
    channelVisibility: z.string().optional(),
  })
  .passthrough();

export const HandshakeSchema = z
  .object({
    protocolVersion: z.number().int(),
    name: z.string(),
    userName: z.string(),
    botUserId: z.string().optional(),
    lockScope: z.enum(["thread", "channel"]).optional(),
    persistThreadHistory: z.boolean().optional(),
    supportsTurnCancellation: z.boolean().optional(),
    capabilities: z.array(z.string()).optional(),
  })
  .passthrough();

/** Consumer -> host, dispatched into the real adapter. */
export const OUTBOUND_CALLS = z.discriminatedUnion("method", [
  z
    .object({
      method: z.literal("__handshake"),
      id: Id,
      params: z.tuple([]),
    })
    .strict(),
  z
    .object({
      method: z.literal("postMessage"),
      id: Id,
      params: z.tuple([ThreadId, PostableSchema]),
    })
    .strict(),
  z
    .object({
      method: z.literal("editMessage"),
      id: Id,
      params: z.tuple([ThreadId, MessageId, PostableSchema]),
    })
    .strict(),
  z
    .object({
      method: z.literal("deleteMessage"),
      id: Id,
      params: z.tuple([ThreadId, MessageId]),
    })
    .strict(),
  z
    .object({
      method: z.literal("addReaction"),
      id: Id,
      params: z.tuple([ThreadId, MessageId, z.string()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("removeReaction"),
      id: Id,
      params: z.tuple([ThreadId, MessageId, z.string()]),
    })
    .strict(),
  // `.nullish()`, not `.optional()`: JSON.stringify turns an omitted array element into `null`.
  z
    .object({
      method: z.literal("fetchMessages"),
      id: Id,
      params: z.tuple([ThreadId, Loose.nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("fetchThread"),
      id: Id,
      params: z.tuple([ThreadId]),
    })
    .strict(),
  z
    .object({
      method: z.literal("startTyping"),
      id: Id,
      params: z.tuple([ThreadId, z.string().nullish(), Loose.nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("disconnect"),
      id: Id,
      params: z.tuple([]),
    })
    .strict(),
  z
    .object({
      method: z.literal("reply"),
      id: Id,
      params: z.tuple([ThreadId, MessageId, PostableSchema]),
    })
    .strict(),
  z
    .object({
      method: z.literal("endTyping"),
      id: Id,
      params: z.tuple([ThreadId, z.string().nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("markAsRead"),
      id: Id,
      params: z.tuple([ThreadId, MessageId, WireMessageSchema.nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("listThreads"),
      id: Id,
      params: z.tuple([ChannelId, Loose.nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("getUser"),
      id: Id,
      params: z.tuple([z.string()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("postObject"),
      id: Id,
      params: z.tuple([ThreadId, z.string(), z.unknown()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("editObject"),
      id: Id,
      params: z.tuple([ThreadId, MessageId, z.string(), z.unknown()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("openDM"),
      id: Id,
      params: z.tuple([z.string()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("openModal"),
      id: Id,
      params: z.tuple([z.string(), z.unknown(), z.string().nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("postEphemeral"),
      id: Id,
      params: z.tuple([ThreadId, z.string(), PostableSchema]),
    })
    .strict(),
  z
    .object({
      method: z.literal("postChannelMessage"),
      id: Id,
      params: z.tuple([ChannelId, PostableSchema]),
    })
    .strict(),
  z
    .object({
      method: z.literal("fetchMessage"),
      id: Id,
      params: z.tuple([ThreadId, MessageId]),
    })
    .strict(),
  z
    .object({
      method: z.literal("fetchChannelInfo"),
      id: Id,
      params: z.tuple([ChannelId]),
    })
    .strict(),
  z
    .object({
      method: z.literal("fetchChannelMessages"),
      id: Id,
      params: z.tuple([ChannelId, Loose.nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("fetchSubject"),
      id: Id,
      params: z.tuple([z.unknown()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("onThreadSubscribe"),
      id: Id,
      params: z.tuple([ThreadId]),
    })
    .strict(),
  z
    .object({
      method: z.literal("streamStart"),
      id: Id,
      params: z.tuple([ThreadId, Loose.nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("streamPush"),
      id: Id,
      params: z.tuple([z.string(), z.array(StreamChunkSchema)]),
    })
    .strict(),
  z
    .object({
      method: z.literal("streamEnd"),
      id: Id,
      params: z.tuple([z.string(), z.boolean().nullish()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("scheduleMessage"),
      id: Id,
      params: z.tuple([ThreadId, PostableSchema, Loose]),
    })
    .strict(),
  z
    .object({
      method: z.literal("cancelScheduledMessage"),
      id: Id,
      params: z.tuple([z.string()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("rehydrateAttachment"),
      id: Id,
      params: z.tuple([Loose]),
    })
    .strict(),
]);

const ThreadEvent = z.object({ threadId: z.string() }).passthrough();

/** Host -> consumer, dispatched into the real Chat instance. */
export const INBOUND_CALLS = z.discriminatedUnion("method", [
  z
    .object({
      method: z.literal("processMessage"),
      id: Id,
      params: z.tuple([ThreadId, WireMessageSchema, ThreadFactsSchema]),
    })
    .strict(),
  z
    .object({
      method: z.literal("processReaction"),
      id: Id,
      params: z.tuple([ThreadEvent]),
    })
    .strict(),
  z
    .object({
      method: z.literal("processMessageUpdated"),
      id: Id,
      params: z.tuple([ThreadEvent]),
    })
    .strict(),
  z
    .object({
      method: z.literal("processMessageDeleted"),
      id: Id,
      params: z.tuple([ThreadEvent]),
    })
    .strict(),
  z
    .object({
      method: z.literal("processAction"),
      id: Id,
      params: z.tuple([ThreadEvent]),
    })
    .strict(),
  z
    .object({
      method: z.literal("processSlashCommand"),
      id: Id,
      params: z.tuple([z.object({ channelId: z.string() }).passthrough()]),
    })
    .strict(),
  z
    .object({
      method: z.literal("abortTurn"),
      id: Id,
      params: z.tuple([ThreadId]),
    })
    .strict(),
  z
    .object({
      method: z.literal("log"),
      id: z.undefined().optional(),
      params: z.tuple([
        z.enum(["debug", "info", "warn", "error"]),
        z.string(),
        z.string(),
        z.array(z.unknown()),
      ]),
    })
    .strict(),
]);

/** Event payload keys that can carry a serialized `Message`. */
export const EVENT_MESSAGE_KEYS = ["message", "previousMessage"] as const;
