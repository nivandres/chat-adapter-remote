/**
 * Prints what the bridge actually carries, derived from the protocol itself so
 * it cannot drift from the code. Run with `npm run support-map`.
 */
import {
  INBOUND_CALLS,
  OPTIONAL_CAPABILITIES,
  OUTBOUND_CALLS,
  PROTOCOL_VERSION,
} from "../src/rpc/methods";

const methodsOf = (union: typeof OUTBOUND_CALLS | typeof INBOUND_CALLS) =>
  union.options
    .map((option) => (option.shape.method as { value: string }).value)
    .filter((name) => !name.startsWith("__"));

/** Synchronous `Adapter` members: their answer cannot come from a round trip. */
const SYNCHRONOUS = {
  encodeThreadId: "sync, unbounded input",
  decodeThreadId: "sync, core never calls it",
  renderFormatted: "sync, unbounded input",
  parseMessage: "sync, unbounded input",
  channelIdFromThreadId: "sync, answered from cached per-message facts",
  isDM: "sync, answered from cached per-message facts",
  getChannelVisibility: "sync, answered from cached per-message facts",
};

/** Outbound calls that exist only to carry a member whose real name differs. */
const INTERNAL = new Set([
  "streamStart",
  "streamPush",
  "streamEnd",
  "cancelScheduledMessage",
]);

const outbound = methodsOf(OUTBOUND_CALLS);
const optional = new Set<string>(OPTIONAL_CAPABILITIES);
const always = outbound.filter((m) => !optional.has(m) && !INTERNAL.has(m));
const gated = outbound.filter((m) => optional.has(m));

const line = (name: string, note = "") => `  ${name.padEnd(32)}${note}`;

console.log(`\nchat-adapter-remote — protocol v${PROTOCOL_VERSION}\n`);

console.log(`OUTBOUND, always bridged (${always.length})`);
console.log("  the adapter must implement these, so they are never gated\n");
for (const m of always.sort()) console.log(line(m));

console.log(`\nOUTBOUND, gated by the handshake (${gated.length})`);
console.log("  carried only when the host adapter implements them\n");
for (const m of gated.sort()) {
  const note =
    m === "stream"
      ? "3-call protocol (start/push/end)"
      : m === "scheduleMessage"
        ? "cancel() stays on the host, reached by id"
        : m === "rehydrateAttachment"
          ? "returns bytes, fetched on demand"
          : "";
  console.log(line(m, note));
}

console.log(`\nOUTBOUND, not bridged (${Object.keys(SYNCHRONOUS).length})`);
for (const [m, why] of Object.entries(SYNCHRONOUS)) console.log(line(m, why));

const inbound = methodsOf(INBOUND_CALLS);
console.log(`\nINBOUND, bridged (${inbound.length})`);
for (const m of inbound.sort()) console.log(line(m));

console.log(`\nINBOUND, not bridged`);
const bridged = new Set(inbound);
const unbridged = [
  ["processModalSubmit", ""],
  ["processModalClose", ""],
  ["processOptionsLoad", ""],
  ["processAgentSessionStopped", ""],
  ["processAgentSessionTitleChanged", ""],
  ["processAppHomeOpened", ""],
  ["processAppContextChanged", ""],
  ["processAssistantThreadStarted", ""],
  ["processAssistantContextChanged", ""],
  ["processMemberJoinedChannel", ""],
  ["getState", "returns a live StateAdapter"],
  ["history", "live API object"],
  ["transcripts", "live API object"],
].filter(([name]) => !bridged.has(name as string));

for (const [name, why] of unbridged) {
  console.log(line(name as string, (why as string) || "logged no-op"));
}
console.log();
