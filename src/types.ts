import type { Logger } from "chat";

import type { ReplayGuard } from "./rpc/security";

/** The part of `fetch` this package uses. Narrower than `typeof fetch` so any function can be injected. */
export type FetchLike = (
  input: string,
  init?: RequestInit,
) => Promise<Response>;

export interface RemoteAdapterErrorContext {
  /** The inbound method that failed. */
  method: string;
}

export type RemoteAdapterErrorHandler = (
  error: unknown,
  context: RemoteAdapterErrorContext,
) => void;

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
  /** Receives failures that would otherwise only reach the logger. */
  onError?: RemoteAdapterErrorHandler;
  /** Rejects an inbound signature that was already accepted. Defaults to a per-process store; supply your own to share one across instances. */
  replayGuard?: ReplayGuard;
  /** Thread facts kept from inbound messages, oldest evicted first. Default 1000. */
  maxCachedThreads?: number;
  logger?: Logger;
  fetch?: FetchLike;
}
