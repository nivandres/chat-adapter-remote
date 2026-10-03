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

/** One request between the two sides, for metrics and tracing. */
export interface RequestEvent {
  /** `sent`: this side called the other; `received`: the other side called this one. */
  direction: "sent" | "received";
  method: string;
  ms: number;
  /** Absent when it succeeded. */
  error?: unknown;
}

export type RequestHandler = (event: RequestEvent) => void;

export interface RetryOptions {
  /** Tries per call, the first included. Default 3. */
  maxAttempts?: number;
  /** Wait between tries. Default 1 second. */
  intervalMs?: number;
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
  /** Tries again a call that got no answer, only against a host that answers a retry once. `false` disables. */
  retry?: RetryOptions | false;
  /** Every request sent or received. */
  onRequest?: RequestHandler;
  logger?: Logger;
  fetch?: FetchLike;
}
