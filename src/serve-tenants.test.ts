import { createMockAdapter } from "@chat-adapter/tests";
import type { Adapter } from "chat";
import { describe, expect, it, vi } from "vitest";

import { createRemoteAdapter } from "./adapter";
import { serveAdapters, serveTenants, type AdapterHosts } from "./host";
import { CONSUMER_URL } from "./testing/bridge";

const SECRET = "t".repeat(64);

function entry(adapter: Adapter) {
  return { adapter, secret: SECRET, consumerUrl: CONSUMER_URL, fetch: vi.fn() };
}

function post(hosts: AdapterHosts, name: string, text: string) {
  return createRemoteAdapter({
    url: `https://host.test/rpc/${name}`,
    secret: SECRET,
    fetch: (input, init) => hosts.fetch(new Request(input, init)),
  }).postMessage("remote:x", text);
}

describe("adding and removing adapters at runtime", () => {
  it("serves an adapter added after start, and stops it once removed", async () => {
    const hosts = serveAdapters({});
    const adapter = createMockAdapter("whatsapp");

    await hosts.add("late", entry(adapter));
    await post(hosts, "late", "hello");
    await hosts.remove("late");

    expect(adapter.postMessage).toHaveBeenCalledWith("whatsapp:x", "hello");
    expect(adapter.disconnect).toHaveBeenCalled();
    expect(hosts.get("late")).toBeUndefined();
  });

  it("stops what served a name before replacing it", async () => {
    const hosts = serveAdapters({});
    const before = createMockAdapter("whatsapp");
    const after = createMockAdapter("whatsapp");

    await hosts.add("ch", entry(before));
    await hosts.add("ch", entry(after));
    await post(hosts, "ch", "hello");

    expect(before.disconnect).toHaveBeenCalled();
    expect(before.postMessage).not.toHaveBeenCalled();
    expect(after.postMessage).toHaveBeenCalledOnce();
  });

  it("leaves no orphan running when the same name is added concurrently", async () => {
    const hosts = serveAdapters({});
    const adapters = [1, 2, 3].map(() => createMockAdapter("whatsapp"));

    await Promise.all(
      adapters.map((adapter) => hosts.add("ch", entry(adapter))),
    );

    expect(adapters[0]!.disconnect).toHaveBeenCalled();
    expect(adapters[1]!.disconnect).toHaveBeenCalled();
    expect(adapters[2]!.disconnect).not.toHaveBeenCalled();
  });

  it("does not keep an adapter that fails to start", async () => {
    const hosts = serveAdapters({});
    const broken = createMockAdapter("whatsapp", {
      initialize: vi.fn().mockRejectedValue(new Error("logged out")),
    });

    await expect(hosts.add("ch", entry(broken))).rejects.toThrow(/logged out/);
    expect(hosts.get("ch")).toBeUndefined();
  });
});

describe("tenants", () => {
  function records(initial: Record<string, boolean>) {
    const active = { ...initial };
    const adapters = new Map<string, Adapter>();
    const load = vi.fn(async (id: string) => {
      if (!active[id]) return undefined;
      const adapter = createMockAdapter("whatsapp");
      adapters.set(id, adapter);
      return entry(adapter);
    });
    return { active, adapters, load, list: () => Object.keys(active) };
  }

  it("loads each record once however often start is awaited", async () => {
    const source = records({ a: true });
    const tenants = serveTenants(source);
    await tenants.start();
    await tenants.start();

    expect(source.load).toHaveBeenCalledOnce();
  });

  it("serves the active records on start", async () => {
    const source = records({ a: true, b: false });
    const tenants = serveTenants(source);
    await tenants.start();

    await post(tenants, "a", "hi");

    expect(source.adapters.get("a")!.postMessage).toHaveBeenCalled();
    expect(tenants.get("b")).toBeUndefined();
  });

  it("reloads a tenant from its record, and removes one whose record is gone", async () => {
    const source = records({ a: true });
    const tenants = serveTenants(source);
    await tenants.start();
    const first = source.adapters.get("a")!;

    await tenants.load("a");
    expect(first.disconnect).toHaveBeenCalled();
    expect(tenants.get("a")).toBeDefined();

    source.active.a = false;
    await tenants.load("a");
    expect(tenants.get("a")).toBeUndefined();
  });

  it("never loads a tenant for a route nobody added", async () => {
    const source = records({});
    const tenants = serveTenants(source);
    await tenants.start();

    const response = await tenants.fetch(
      new Request("https://host.test/rpc/guess", {
        method: "POST",
        body: "{}",
      }),
    );

    expect(response.status).toBe(404);
    expect(source.load).not.toHaveBeenCalled();
  });

  it("reports a tenant that cannot load and serves the rest", async () => {
    const onError = vi.fn();
    const source = records({ good: true, bad: true });
    const load = source.load;
    source.load = vi.fn(async (id: string) => {
      if (id === "bad") throw new Error("database down");
      return load(id);
    });
    const tenants = serveTenants({ ...source, onError });
    await tenants.start();

    expect(onError).toHaveBeenCalledWith(expect.any(Error), "bad");
    expect(tenants.get("good")).toBeDefined();
  });

  it("reports a failing list instead of rejecting unhandled", async () => {
    const onError = vi.fn();
    serveTenants({
      list: () => Promise.reject(new Error("database down")),
      load: () => undefined,
      onError,
    });
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(onError).toHaveBeenCalledWith(expect.any(Error));
  });
});
