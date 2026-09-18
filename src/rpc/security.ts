/** Makes a signature single-use inside its freshness window. May be backed by a shared store, so it is allowed to be async. */
export interface ReplayGuard {
  seen(signature: string, toleranceMs: number): boolean | Promise<boolean>;
}

export interface ReplayGuardOptions {
  /** Bounds memory under a flood of distinct signatures. Default 10000. */
  maxEntries?: number;
}

export function createReplayGuard(
  options: ReplayGuardOptions = {},
): ReplayGuard {
  const maxEntries = options.maxEntries ?? 10_000;
  const accepted = new Map<string, number>();

  return {
    seen(signature, toleranceMs) {
      const now = Date.now();
      // Insertion-ordered, so the first entry still inside the window ends the sweep.
      for (const [key, at] of accepted) {
        if (now - at <= toleranceMs) break;
        accepted.delete(key);
      }
      if (accepted.has(signature)) return true;
      while (accepted.size >= maxEntries)
        accepted.delete(accepted.keys().next().value!);
      accepted.set(signature, now);
      return false;
    },
  };
}
