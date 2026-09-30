import type { StateAdapter } from "chat";

import { STATE_OPERATIONS, type StateOperation } from "../rpc/methods";

/** Not persisted: only better than handing the adapter nothing. */
export function createLocalState(): StateAdapter {
  const values = new Map<string, { value: unknown; expiresAt?: number }>();
  const live = (key: string) => {
    const entry = values.get(key);
    if (!entry) return undefined;
    if (entry.expiresAt !== undefined && entry.expiresAt <= Date.now()) {
      values.delete(key);
      return undefined;
    }
    return entry;
  };
  const put = (key: string, value: unknown, ttlMs?: number) => {
    values.set(key, {
      value,
      expiresAt: ttlMs === undefined ? undefined : Date.now() + ttlMs,
    });
  };

  return createRemoteState(async (operation, args) => {
    const key = String(args[0]);
    switch (operation) {
      case "get":
        return live(key)?.value ?? null;
      case "set":
        put(key, args[1], args[2] as number | undefined);
        return undefined;
      case "setIfNotExists":
        if (live(key)) return false;
        put(key, args[1], args[2] as number | undefined);
        return true;
      case "delete":
        values.delete(key);
        return undefined;
      case "getList":
        return (live(key)?.value as unknown[]) ?? [];
      case "appendToList": {
        const list = ((live(key)?.value as unknown[]) ?? []).concat(args[1]);
        put(key, list);
        return undefined;
      }
      default:
        return undefined;
    }
  });
}

/** Adapters call `getState()` synchronously, so this exists at once and only its methods cross. */
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
