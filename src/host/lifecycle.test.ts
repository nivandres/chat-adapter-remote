import { createMockAdapter } from "@chat-adapter/tests";
import type { Adapter, ChatInstance } from "chat";
import { describe, expect, it, vi } from "vitest";

import { serveAdapter } from "../host";

const options = {
  secret: "s",
  consumerUrl: "https://consumer.test/inbound",
  fetch: vi.fn(),
};

describe("host lifecycle", () => {
  it("initializes during construction by default", async () => {
    const adapter = createMockAdapter("mock");
    const host = serveAdapter(adapter, options);

    await host.ready;
    expect(adapter.initialize).toHaveBeenCalledOnce();
  });

  it("defers initialization until start() when autoStart is false", async () => {
    const adapter = createMockAdapter("mock");
    const host = serveAdapter(adapter, { ...options, autoStart: false });

    expect(adapter.initialize).not.toHaveBeenCalled();
    await host.start();
    expect(adapter.initialize).toHaveBeenCalledOnce();
  });

  it("only initializes once however often start() is called", async () => {
    const adapter = createMockAdapter("mock");
    const host = serveAdapter(adapter, { ...options, autoStart: false });

    await Promise.all([host.start(), host.start(), host.ready]);
    expect(adapter.initialize).toHaveBeenCalledOnce();
  });

  it("surfaces an initialization failure through start() and onError", async () => {
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

  it("calls onReady once the adapter is initialized", async () => {
    const onReady = vi.fn();
    await serveAdapter(createMockAdapter("mock"), { ...options, onReady })
      .ready;
    expect(onReady).toHaveBeenCalledOnce();
  });

  it("disconnects the adapter on stop() and refuses further dispatch", async () => {
    const adapter = createMockAdapter("mock");
    const host = serveAdapter(adapter, options);
    await host.ready;

    await host.stop();

    expect(adapter.disconnect).toHaveBeenCalled();
    const { sign } = await import("../rpc/signing");
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
          "x-chat-adapter-remote-signature": sign(body, timestamp, "s"),
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
    let chatInstance: ChatInstance | undefined;
    const adapter = createMockAdapter("mock", {
      initialize: vi.fn(async (instance: ChatInstance) => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        chatInstance = instance;
        order.push("initialized");
      }),
      handleWebhook: vi.fn(async () => {
        order.push("webhook");
        return new Response("ok");
      }),
    });

    const host = serveAdapter(adapter, { ...options, autoStart: false });
    const response = await host.handlePlatformWebhook(
      new Request("https://platform.test/events", { method: "POST" }),
    );

    expect(order).toEqual(["initialized", "webhook"]);
    expect(await response.text()).toBe("ok");
    expect(chatInstance).toBeDefined();
  });

  it("passes webhook options through to the adapter", async () => {
    const adapter: Adapter = createMockAdapter("mock");
    const host = serveAdapter(adapter, options);
    const waitUntil = vi.fn();
    const request = new Request("https://platform.test/events", {
      method: "POST",
    });

    await host.handlePlatformWebhook(request, { waitUntil });

    expect(adapter.handleWebhook).toHaveBeenCalledWith(request, { waitUntil });
  });
});
