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

  it("holds a request that arrives mid-reload and serves it with the new adapter", async () => {
    const hosts = serveAdapters({});
    await hosts.add("ch", entry(createMockAdapter("whatsapp")));
    let finish!: () => void;
    const next = createMockAdapter("whatsapp", {
      initialize: vi.fn(
        () => new Promise<void>((resolve) => (finish = resolve)),
      ),
    });

    const reloading = hosts.add("ch", entry(next));
    const sent = post(hosts, "ch", "during reload");
    await new Promise((resolve) => setTimeout(resolve, 10));
    finish();
    await reloading;

    await expect(sent).resolves.toBeDefined();
    expect(next.postMessage).toHaveBeenCalledWith(
      "whatsapp:x",
      "during reload",
    );
  });

  it("answers a probe per tenant, and 404 for one not served", async () => {
    const hosts = serveAdapters({});
    await hosts.add("ch", entry(createMockAdapter("whatsapp")));
    const probe = (name: string) =>
      hosts.fetch(new Request(`https://host.test/rpc/${name}`));

    expect((await probe("ch")).status).toBe(200);
    expect((await probe("other")).status).toBe(404);
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

describe("a consumer facing a host that changes", () => {
  function counted(hosts: AdapterHosts) {
    const handshakes = { count: 0 };
    const remote = createRemoteAdapter({
      url: "https://host.test/rpc/ch",
      secret: SECRET,
      fetch: (input, init) => {
        if (String(init?.body).includes("__handshake")) handshakes.count++;
        return hosts.fetch(new Request(input, init));
      },
    });
    return { remote, handshakes };
  }

  it("handshakes again once the host is reloaded with another setup", async () => {
    const hosts = serveAdapters({});
    await hosts.add("ch", {
      ...entry(createMockAdapter("whatsapp")),
      stream: { mode: "off" },
    });
    const { remote, handshakes } = counted(hosts);
    await remote.postMessage("remote:x", "before");
    expect(remote.stream).toBeUndefined();

    await hosts.add("ch", {
      ...entry(createMockAdapter("whatsapp")),
      stream: { mode: "buffer" },
    });
    await remote.postMessage("remote:x", "noticed");
    await remote.postMessage("remote:x", "after");

    expect(handshakes.count).toBe(2);
    expect(remote.stream).toBeTypeOf("function");
  });

  it("does not handshake again across a restart with the same setup, or replicas alike", async () => {
    const hosts = serveAdapters({});
    await hosts.add("ch", entry(createMockAdapter("whatsapp")));
    const { remote, handshakes } = counted(hosts);
    await remote.postMessage("remote:x", "one");

    await hosts.add("ch", entry(createMockAdapter("whatsapp")));
    await remote.postMessage("remote:x", "two");
    await remote.postMessage("remote:x", "three");

    expect(handshakes.count).toBe(1);
  });
});

describe("rotating the secret", () => {
  it("accepts every listed secret and signs with the first", async () => {
    const hosts = serveAdapters({});
    const adapter = createMockAdapter("whatsapp");
    await hosts.add("ch", { ...entry(adapter), secret: ["new", "old"] });
    const consumer = (secret: string | string[]) =>
      createRemoteAdapter({
        url: "https://host.test/rpc/ch",
        secret,
        fetch: (input, init) => hosts.fetch(new Request(input, init)),
      });

    await consumer("old").postMessage("remote:x", "still on the old one");
    await consumer(["new", "old"]).postMessage("remote:x", "already rotated");
    await expect(
      consumer("other").postMessage("remote:x", "never valid"),
    ).rejects.toMatchObject({ code: -32000 });

    expect(adapter.postMessage).toHaveBeenCalledTimes(2);
  });
});
