import type { Logger } from "chat";

/** The part of `fetch` this package uses. Narrower than `typeof fetch` so any function can be injected. */
export type FetchLike = (
  input: string,
  init?: RequestInit,
) => Promise<Response>;

export interface RemoteAdapterConfig {
  /** The AdapterHost dispatch endpoint. */
  url: string;
  /** Shared HMAC secret, identical on both sides. */
  secret: string;
  /** Registration name. Defaults to "remote"; should match the key this adapter is registered under. */
  name?: string;
  /** Overrides the bot username learned from the handshake. */
  userName?: string;
  timeoutMs?: number;
  /** Rejects inbound requests whose signed timestamp is older than this. Default 30s. */
  timestampToleranceMs?: number;
  /** Rejects inbound bodies larger than this. Default 5MB. */
  maxBodyBytes?: number;
  logger?: Logger;
  fetch?: FetchLike;
}
