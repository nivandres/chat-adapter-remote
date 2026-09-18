import type { Adapter } from "chat";

const ADAPTER_MEMBERS = new Set([
  "addReaction",
  "botUserId",
  "channelIdFromThreadId",
  "decodeThreadId",
  "deleteMessage",
  "disconnect",
  "editMessage",
  "editObject",
  "encodeThreadId",
  "endTyping",
  "fetchChannelInfo",
  "fetchChannelMessages",
  "fetchMessage",
  "fetchMessages",
  "fetchSubject",
  "fetchThread",
  "getChannelVisibility",
  "getUser",
  "handleWebhook",
  "initialize",
  "isDM",
  "listThreads",
  "lockScope",
  "markAsRead",
  "name",
  "onThreadSubscribe",
  "openDM",
  "openModal",
  "parseMessage",
  "persistMessageHistory",
  "persistThreadHistory",
  "postChannelMessage",
  "postEphemeral",
  "postMessage",
  "postObject",
  "rehydrateAttachment",
  "removeReaction",
  "renderFormatted",
  "reply",
  "scheduleMessage",
  "startTyping",
  "stream",
  "supportsTurnCancellation",
  "userName",
]);

const NEVER_EXPOSED = new Set(["constructor", "__proto__"]);

/** Reopening or closing the platform connection is the host's business, not the consumer's. */
const LIFECYCLE = new Set([
  "connect",
  "reconnect",
  "close",
  "destroy",
  "login",
  "logout",
  "start",
  "stop",
]);

/** `onSomething` takes a callback, and a callback cannot cross JSON-RPC. */
const CALLBACK_SHAPED = /^on[A-Z]/;

/** Walks descriptors rather than reading properties, so a getter is not invoked by being looked at. */
function callable(adapter: Adapter, name: string): boolean {
  if (NEVER_EXPOSED.has(name)) return false;
  let proto: object | null = adapter;
  while (proto && proto !== Object.prototype) {
    const descriptor = Object.getOwnPropertyDescriptor(proto, name);
    if (descriptor) return typeof descriptor.value === "function";
    proto = Object.getPrototypeOf(proto) as object | null;
  }
  return false;
}

function discover(adapter: Adapter): string[] {
  const found = new Set<string>();
  let proto: object | null = Object.getPrototypeOf(adapter) as object | null;
  while (proto && proto !== Object.prototype) {
    for (const name of Object.getOwnPropertyNames(proto)) {
      if (ADAPTER_MEMBERS.has(name) || name.startsWith("_")) continue;
      if (LIFECYCLE.has(name) || CALLBACK_SHAPED.test(name)) continue;
      if (callable(adapter, name)) found.add(name);
    }
    proto = Object.getPrototypeOf(proto) as object | null;
  }
  return [...found];
}

/**
 * `true` exposes the adapter's own public methods. A `string[]` is the
 * narrower choice: the wire can only ever pick a name the host already
 * decided on, so it never selects what to call, only from what.
 */
export function resolveCustomMethods(
  adapter: Adapter,
  option: string[] | true | undefined,
): string[] {
  if (!option) return [];
  if (option === true) return discover(adapter);
  return option.filter((name) => callable(adapter, name));
}
