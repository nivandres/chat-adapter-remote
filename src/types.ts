import type { Logger } from "chat";

export interface RemoteAdapterConfig {
  /** AdapterHost's dispatch endpoint (its handleRequest(), mounted at any route). */
  url: string;
  /** Shared HMAC secret with the corresponding AdapterHost. */
  secret: string;
  /**
   * Overrides the identity learned from the handshake. Usually left unset —
   * RemoteAdapter.initialize() fetches {name, userName, botUserId} from the
   * real adapter via the internal __handshake call.
   */
  name?: string;
  userName?: string;
  timeoutMs?: number;
  logger?: Logger;
  /** Override the fetch implementation used for outbound RPC calls. Mainly for tests. */
  fetch?: typeof fetch;
}
