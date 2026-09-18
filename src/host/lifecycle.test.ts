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
          "x-chat-adapter-remote-signature": sign(body, timestamp, SECRET),
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
