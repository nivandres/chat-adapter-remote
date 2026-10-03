import { createMockAdapter } from "@chat-adapter/tests";
import type { ChatInstance } from "chat";
import { describe, expect, it, vi } from "vitest";

import { createAdapterHost, serveAdapter } from "../host";
import { sign } from "../rpc/signing";
import {
  CONSUMER_URL,
  SECRET,
  bridge,
  deferred,
  handshake,
  message,
} from "../testing/bridge";

const options = {
  secret: SECRET,
  consumerUrl: CONSUMER_URL,
  fetch: vi.fn(),
};

describe("host lifecycle", () => {
  it("connects during construction, once, however often start() is called", async () => {
    const adapter = createMockAdapter("mock");
    const host = serveAdapter(adapter, options);

    await Promise.all([host.ready, host.start(), host.start()]);

    expect(adapter.initialize).toHaveBeenCalledOnce();
  });

  it("stays stopped when built with createAdapterHost", async () => {
    const adapter = createMockAdapter("mock");
    const host = createAdapterHost(adapter, { ...options, autoStart: true });

    expect(adapter.initialize).not.toHaveBeenCalled();
    await host.start();
    expect(adapter.initialize).toHaveBeenCalledOnce();
  });

  it("reports a failed connection through start() and onError", async () => {
    const onError = vi.fn();
    const host = serveAdapter(
      createMockAdapter("mock", {
        initialize: vi.fn().mockRejectedValue(new Error("auth expired")),
      }),
      { ...options, onError },
    );

    await expect(host.start()).rejects.toThrow("auth expired");
    expect(onError).toHaveBeenCalledWith(expect.any(Error), {
      phase: "initialize",
    });
  });

  it("calls onReady once connected", async () => {
    const onReady = vi.fn();
    await serveAdapter(createMockAdapter("mock"), { ...options, onReady })
      .ready;
    expect(onReady).toHaveBeenCalledOnce();
  });

  it("disconnects on stop() and refuses further dispatch", async () => {
    const adapter = createMockAdapter("mock");
    const host = serveAdapter(adapter, options);
    await host.ready;

    await host.stop();

    expect(adapter.disconnect).toHaveBeenCalled();
    const body = JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "postMessage",
      params: ["t", "hi"],
    });
    const timestamp = String(Date.now());
    const response = await host.handleRequest(
      new Request("https://host.test/rpc", {
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

    expect((await response.json()).error.message).toMatch(/stopped/i);
  });

  it("reports a failing disconnect through onError", async () => {
    const onError = vi.fn();
    const host = serveAdapter(
      createMockAdapter("mock", {
        disconnect: vi.fn().mockRejectedValue(new Error("socket stuck")),
      }),
      { ...options, onError },
    );
    await host.ready;

    await expect(host.stop()).rejects.toThrow("socket stuck");
    expect(onError).toHaveBeenCalledWith(expect.any(Error), {
      phase: "shutdown",
    });
  });
});

describe("platform webhooks", () => {
  it("waits for startup before handing a delivery to the adapter", async () => {
    const order: string[] = [];
    const adapter = createMockAdapter("mock", {
      initialize: vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        order.push("initialized");
      }),
      handleWebhook: vi.fn(async () => {
        order.push("webhook");
        return new Response("ok");
      }),
    });
    const host = createAdapterHost(adapter, options);
    const request = new Request("https://platform.test/events", {
      method: "POST",
    });
    const waitUntil = vi.fn();

    const response = await host.handleWebhook(request, { waitUntil });

    expect(order).toEqual(["initialized", "webhook"]);
    expect(await response.text()).toBe("ok");
    expect(adapter.handleWebhook).toHaveBeenCalledWith(request, { waitUntil });
  });
});

describe("log forwarding", () => {
  it("carries host log lines to the consumer's logger", async () => {
    const b = bridge();
    await handshake(b);
    const logged = deferred<string>();
    vi.spyOn(b.chat, "getLogger").mockReturnValue({
      debug: vi.fn(),
      info: vi.fn(),
      warn: (text: string) => logged.resolve(text),
      error: vi.fn(),
      child: vi.fn(),
    } as never);

    (b.hostChat() as ChatInstance).getLogger("baileys").warn("reconnecting");

    expect(await logged.promise).toBe("reconnecting");
  });
});

describe("health", () => {
  const probe = async (fetch: (request: Request) => Promise<Response>) => {
    const response = await fetch(
      new Request("https://host.test/rpc", { method: "GET" }),
    );
    return { status: response.status, body: await response.json() };
  };

  it("answers probes with the host's state, unsigned", async () => {
    const host = createAdapterHost(createMockAdapter("mock"), options);
    expect(await probe(host.fetch)).toMatchObject({
      status: 503,
      body: { status: "idle" },
    });

    await host.start();
    expect(await probe(host.fetch)).toEqual({
      status: 200,
      body: { status: "ready", openStreams: 0, queuedForwards: 0 },
    });

    await host.stop();
    expect(await probe(host.fetch)).toMatchObject({
      status: 503,
      body: { status: "stopped" },
    });
  });

  it("reports an adapter that failed to start", async () => {
    const host = createAdapterHost(
      createMockAdapter("mock", {
        initialize: vi.fn().mockRejectedValue(new Error("logged out")),
      }),
      options,
    );
    await host.start().catch(() => undefined);

    expect(await probe(host.fetch)).toMatchObject({
      status: 503,
      body: { status: "failed" },
    });
  });

  it("counts the forwards waiting for the consumer", async () => {
    let chat!: ChatInstance;
    const adapter = createMockAdapter("mock", {
      initialize: vi.fn(
        async (instance: ChatInstance) => void (chat = instance),
      ),
    });
    const host = serveAdapter(adapter, {
      ...options,
      fetch: vi.fn().mockRejectedValue(new TypeError("fetch failed")),
    });
    await host.ready;

    await chat.processMessage(adapter, "mock:c:1", message("while down"));

    expect(await host.health()).toMatchObject({ queuedForwards: 1 });
  });
});

describe("request hooks", () => {
  it("reports each request on both sides, with its method and duration", async () => {
    const hostSide: unknown[] = [];
    const consumerSide: unknown[] = [];
    const b = bridge(
      {},
      { onRequest: (event) => hostSide.push(event) },
      { onRequest: (event) => consumerSide.push(event) },
    );
    await handshake(b);

    await b.remote.postMessage("mock:c:1", "hi");

    const call = { method: "postMessage", ms: expect.any(Number) };
    expect(consumerSide).toContainEqual({ direction: "sent", ...call });
    expect(hostSide).toContainEqual({ direction: "received", ...call });
  });

  it("includes the error of a failed request", async () => {
    const events: Array<{ method: string; error?: unknown }> = [];
    const b = bridge(
      { postMessage: vi.fn().mockRejectedValue(new Error("rejected")) },
      { onRequest: (event) => events.push(event) },
    );
    await handshake(b);

    await expect(b.remote.postMessage("mock:c:1", "hi")).rejects.toThrow();

    expect(
      events.find((event) => event.method === "postMessage")?.error,
    ).toBeInstanceOf(Error);
  });

  it("never lets a failing hook fail the request", async () => {
    const b = bridge(
      {},
      {
        onRequest: () => {
          throw new Error("metrics down");
        },
      },
      {
        onRequest: () => {
          throw new Error("metrics down");
        },
      },
    );
    await handshake(b);

    await expect(b.remote.postMessage("mock:c:1", "hi")).resolves.toBeDefined();
  });
});
