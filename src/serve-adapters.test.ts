import { createMockAdapter } from "@chat-adapter/tests";
import { describe, expect, it, vi } from "vitest";

import { createRemoteAdapter } from "./adapter";
import { serveAdapters } from "./host";
import { RpcErrorCode } from "./rpc/errors";
import { CONSUMER_URL } from "./testing/bridge";

const SECRET_A = "a".repeat(64);
const SECRET_B = "b".repeat(64);

function twoChannels() {
  const first = createMockAdapter("whatsapp");
  const second = createMockAdapter("whatsapp");
  const hosts = serveAdapters({
    "ch-1": {
      adapter: first,
      secret: SECRET_A,
      consumerUrl: CONSUMER_URL,
      fetch: vi.fn(),
    },
    "ch-2": {
      adapter: second,
      secret: SECRET_B,
      consumerUrl: CONSUMER_URL,
      fetch: vi.fn(),
    },
  });
  const consumer = (path: string, secret: string) =>
    createRemoteAdapter({
      url: `https://host.test/rpc/${path}`,
      secret,
      fetch: (input, init) => hosts.fetch(new Request(input, init)),
    });
  return { first, second, hosts, consumer };
}

describe("several adapters per host", () => {
  it("routes each channel to its own adapter by path", async () => {
    const { first, second, hosts, consumer } = twoChannels();
    await hosts.start();

    await consumer("ch-1", SECRET_A).postMessage("remote:x", "to one");
    await consumer("ch-2", SECRET_B).postMessage("remote:x", "to two");

    expect(first.postMessage).toHaveBeenCalledWith("whatsapp:x", "to one");
    expect(second.postMessage).toHaveBeenCalledWith("whatsapp:x", "to two");
    expect(first.postMessage).toHaveBeenCalledOnce();
    expect(second.postMessage).toHaveBeenCalledOnce();
  });

  it("does not let one channel's secret reach another channel", async () => {
    const { second, hosts, consumer } = twoChannels();
    await hosts.start();

    await expect(
      consumer("ch-2", SECRET_A).postMessage("remote:x", "intruder"),
    ).rejects.toMatchObject({ code: RpcErrorCode.UNAUTHORIZED });
    expect(second.postMessage).not.toHaveBeenCalled();
  });

  it("answers an unknown channel with a clear error", async () => {
    const { hosts } = twoChannels();

    const response = await hosts.fetch(
      new Request("https://host.test/rpc/nope", { method: "POST", body: "{}" }),
    );

    expect(response.status).toBe(404);
    expect((await response.json()).error.message).toMatch(/"nope"/);
  });

  it("keeps the other channels running when one cannot connect", async () => {
    const onError = vi.fn();
    const healthy = createMockAdapter("whatsapp");
    const hosts = serveAdapters({
      broken: {
        adapter: createMockAdapter("whatsapp", {
          initialize: vi.fn().mockRejectedValue(new Error("logged out")),
        }),
        secret: SECRET_A,
        consumerUrl: CONSUMER_URL,
        fetch: vi.fn(),
        onError,
      },
      healthy: {
        adapter: healthy,
        secret: SECRET_B,
        consumerUrl: CONSUMER_URL,
        fetch: vi.fn(),
      },
    });

    await expect(hosts.start()).resolves.toBeUndefined();

    expect(onError).toHaveBeenCalledWith(expect.any(Error), {
      phase: "initialize",
    });
    expect(healthy.initialize).toHaveBeenCalled();
  });
});

describe("host events", () => {
  it("delivers what the host emits to the consumer's onEvent", async () => {
    const { createMemoryState } = await import("@chat-adapter/state-memory");
    const { Chat } = await import("chat");
    const { serveAdapter } = await import("./host");
    const { HOST_URL, SECRET } = await import("./testing/bridge");
    const events: unknown[] = [];

    let chat!: InstanceType<typeof Chat>;
    const host = serveAdapter(createMockAdapter("mock"), {
      secret: SECRET,
      consumerUrl: CONSUMER_URL,
      fetch: async (input, init) =>
        chat.webhooks.mock!(new Request(input, init), {}),
    });
    const remote = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      name: "mock",
      onEvent: (event) => events.push(event),
      fetch: (input, init) => host.handleRequest(new Request(input, init)),
    });
    chat = new Chat({
      userName: "bot",
      adapters: { mock: remote },
      state: createMemoryState(),
    });
    await chat.initialize();
    await host.ready;

    host.emit({
      type: "qr",
      code: "2@abc",
      at: new Date("2030-01-01T00:00:00.000Z"),
    });
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(events).toEqual([
      { type: "qr", code: "2@abc", at: new Date("2030-01-01T00:00:00.000Z") },
    ]);
  });

  it("never lets a failing onEvent reach the host", async () => {
    const { SECRET } = await import("./testing/bridge");
    const { sign } = await import("./rpc/signing");
    const onError = vi.fn();
    const remote = createRemoteAdapter({
      url: "https://host.test/rpc",
      secret: SECRET,
      onError,
      onEvent: () => {
        throw new Error("panel exploded");
      },
      fetch: vi.fn(),
    });
    const body = JSON.stringify({
      jsonrpc: "2.0",
      method: "hostEvent",
      params: [{ type: "connection", state: "open" }],
    });
    const timestamp = String(Date.now());

    const response = await remote.handleWebhook(
      new Request(CONSUMER_URL, {
        method: "POST",
        body,
        headers: {
          "x-chat-adapter-remote-signature": sign(
            body,
            timestamp,
            "nonce",
            SECRET,
          ),
          "x-chat-adapter-remote-nonce": "nonce",
          "x-chat-adapter-remote-timestamp": timestamp,
        },
      }),
    );

    expect(response.status).toBe(204);
    expect(onError).toHaveBeenCalledWith(expect.any(Error), {
      method: "hostEvent",
    });
  });
});
