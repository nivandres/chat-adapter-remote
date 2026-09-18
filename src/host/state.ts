import type { StateAdapter } from "chat";

import { STATE_OPERATIONS, type StateOperation } from "../rpc/methods";

/**
 * The state the wrapped adapter persists for itself. Adapters call
 * `chat.getState()` synchronously and then await the result, so this has to be
 * an object that exists immediately; only its methods cross the wire.
 *
 * `connect`/`disconnect` stay local: the consumer owns its own store's
 * lifecycle, and a host opening or closing it would be reaching too far.
 */
export function createRemoteState(
  call: (operation: StateOperation, args: unknown[]) => Promise<unknown>,
): StateAdapter {
  const state: Record<string, unknown> = {
    connect: async () => undefined,
    disconnect: async () => undefined,
  };
  for (const operation of STATE_OPERATIONS) {
    state[operation] = (...args: unknown[]) => call(operation, args);
  }
  return state as unknown as StateAdapter;
}
