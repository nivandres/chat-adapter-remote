import type { Logger } from "chat";

import type { ReplayGuard } from "./rpc/security";
import type { Secret } from "./rpc/signing";

/** Narrower than `typeof fetch`, so any function can be injected. */
export type FetchLike = (
  input: string,
  init?: RequestInit,
) => Promise<Response>;

export type HostStateAccess = "scoped" | "full" | "off";

/** `type` is free; `connection`, `qr` and `pairing-code` are conventional. */
export interface HostEvent {
  type: string;
  [key: string]: unknown;
}

export interface RemoteAdapterErrorContext {
  method: string;
}

export type RemoteAdapterErrorHandler = (
  error: unknown,
  context: RemoteAdapterErrorContext,
) => void;

export interface RemoteAdapterConfig {
  /** The host's dispatch endpoint. */
  url: string;
  /** Shared HMAC secret, identical on both sides. While rotating, a list: the first signs, any verifies. */
  secret: Secret;
  /** The key it is registered under; thread ids are translated to it. Default "remote". */
  name?: string;
  /** Overrides the bot username learned from the handshake. */
  userName?: string;
  timeoutMs?: number;
  /** Oldest signed timestamp accepted. Default 30s. */
  timestampToleranceMs?: number;
  /** Largest body read. Unlimited by default. */
  maxBodyBytes?: number;
  /** Inbound failures, beyond the logger. */
  onError?: RemoteAdapterErrorHandler;
  /** `"scoped"` (default) lends keyed operations under a prefix, `"full"` the whole store, `"off"` nothing. */
  hostState?: HostStateAccess;
  /** What the host sends with `host.emit()`, such as a QR code. */
  onEvent?: (event: HostEvent) => void;
  /** Per-process by default; share one across instances. */
  replayGuard?: ReplayGuard;
  /** Thread facts kept in memory. Default 1000. */
  maxCachedThreads?: number;
  logger?: Logger;
  fetch?: FetchLike;
}
