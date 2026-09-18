import type { Adapter } from "chat";
import { describe, expect, it, vi } from "vitest";

import type { RemoteOf } from "./adapter";
import { bridge, handshake } from "./testing/bridge";

declare class Platform {
  notAsync(count: number): string;
  setPresence(jid: string, state: string): Promise<void>;
  _internal(): Promise<string>;
}

/** Never called: these are assertions for typecheck, not for the runtime. */
function typeAssertions(typed: RemoteOf<Platform & Adapter>) {
  const presence: Promise<void> = typed.setPresence("j", "composing");
  const sync: Promise<string> = typed.notAsync(1);
  // @ts-expect-error underscore-prefixed methods are never exposed
  typed._internal();
  return [presence, sync];
}
void typeAssertions;

describe("custom adapter methods", () => {
  it("reaches a method outside the Adapter interface", async () => {
    const setPresence = vi.fn(
      async (jid: string, state: string) => `${jid}:${state}`,
    );
    const b = bridge({ setPresence } as Partial<Adapter>, {
      customMethods: ["setPresence"],
    });
    await handshake(b);

    const remote = b.remote as unknown as {
      setPresence(jid: string, state: string): Promise<string>;
    };
    expect(await remote.setPresence("u1", "composing")).toBe("u1:composing");
    expect(setPresence).toHaveBeenCalledWith("u1", "composing");
  });

  it("carries dates through the codec like any other call", async () => {
    const b = bridge(
      {
        sendPoll: vi.fn(async () => ({
          at: new Date("2030-01-01T00:00:00.000Z"),
        })),
      } as Partial<Adapter>,
      { customMethods: ["sendPoll"] },
    );
    await handshake(b);

    const remote = b.remote as unknown as {
      sendPoll(): Promise<{ at: Date }>;
    };
    expect((await remote.sendPoll()).at).toEqual(
      new Date("2030-01-01T00:00:00.000Z"),
    );
  });

  it("discovers public methods and skips internals when told to expose everything", async () => {
    let getterRead = false;
    class Platform {
      get trap() {
        getterRead = true;
        return "read";
      }
      async setPresence(state: string) {
        return state;
      }
      async _internal() {
        return "secret";
      }
      notAsync() {
        return "sync";
      }
    }
    const adapter = bridge({}).adapter;
    Object.setPrototypeOf(Platform.prototype, Object.getPrototypeOf(adapter));
    Object.setPrototypeOf(adapter, Platform.prototype);

    const { resolveCustomMethods } = await import("./host/custom-methods");
    const exposed = resolveCustomMethods(adapter, true);

    expect(exposed).toContain("setPresence");
    expect(exposed).not.toContain("_internal");
    expect(exposed).not.toContain("constructor");
    expect(exposed).not.toContain("postMessage");
    expect(exposed).not.toContain("trap");
    expect(getterRead).toBe(false);
    // Every exposed method becomes async over the wire, so a synchronous one
    // is exposed too rather than being singled out.
    expect(exposed).toContain("notAsync");
  });

  it("exposes nothing unless the host opted in", async () => {
    const b = bridge({ setPresence: vi.fn() } as Partial<Adapter>);
    await handshake(b);

    expect(
      (b.remote as unknown as { setPresence?: unknown }).setPresence,
    ).toBeUndefined();
  });

  it("refuses a method the host did not expose", async () => {
    const b = bridge({ setPresence: vi.fn() } as Partial<Adapter>, {
      customMethods: ["setPresence"],
    });
    await handshake(b);
    const rpc = Reflect.get(b.remote, "rpc") as {
      request: (method: string, params: unknown) => Promise<unknown>;
    };

    await expect(rpc.request("custom", ["constructor", []])).rejects.toThrow(
      /not exposed/,
    );
    await expect(rpc.request("custom", ["disconnect", []])).rejects.toThrow(
      /not exposed/,
    );
  });

  it("never lets an exposed name replace an interface member", async () => {
    const b = bridge({} as Partial<Adapter>, {
      customMethods: ["postMessage"],
    });
    await handshake(b);

    // Still the bridged implementation, not a custom passthrough.
    await b.remote.postMessage("mock:general:1", "hi");
    expect(b.adapter.postMessage).toHaveBeenCalled();
  });
});
