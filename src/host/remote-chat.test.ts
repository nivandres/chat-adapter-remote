import { describe, expect, it, vi } from "vitest";

import { RemoteChat, RemoteChatUnsupportedMethodError } from "./remote-chat";

const SECRET = "test-secret";
const CONSUMER_URL = "https://consumer.test/inbound";

describe("RemoteChat unsupported methods", () => {
  const chat = new RemoteChat({
    consumerUrl: CONSUMER_URL,
    secret: SECRET,
    fetch: vi.fn(),
  });

  it("throws RemoteChatUnsupportedMethodError for everything except processMessage/getLogger", () => {
    expect(() => chat.abortTurn("t1")).toThrow(
      RemoteChatUnsupportedMethodError,
    );
    expect(() => chat.getState()).toThrow(RemoteChatUnsupportedMethodError);
    expect(() => chat.getUserName()).toThrow(RemoteChatUnsupportedMethodError);
    expect(() => chat.handleIncomingMessage()).toThrow(
      RemoteChatUnsupportedMethodError,
    );
    expect(() => chat.history).toThrow(RemoteChatUnsupportedMethodError);
    expect(() => chat.processAction()).toThrow(
      RemoteChatUnsupportedMethodError,
    );
    expect(() => chat.processReaction()).toThrow(
      RemoteChatUnsupportedMethodError,
    );
    expect(() => chat.processSlashCommand()).toThrow(
      RemoteChatUnsupportedMethodError,
    );
    expect(() => chat.transcripts).toThrow(RemoteChatUnsupportedMethodError);
  });

  it("error message names the specific unsupported method", () => {
    try {
      chat.getState();
      expect.unreachable();
    } catch (error) {
      expect((error as Error).message).toContain("getState");
    }
  });
});

describe("RemoteChat.getLogger", () => {
  it("logs locally and best-effort mirrors the line to the consumer via a fire-and-forget notification", async () => {
    const notified: unknown[] = [];
    const fetchImpl = vi.fn(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        notified.push(JSON.parse(String(init?.body)));
        return new Response(null, { status: 204 });
      },
    ) as unknown as typeof fetch;

    const localLogger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      child: vi.fn().mockReturnThis(),
    };
    const chat = new RemoteChat({
      consumerUrl: CONSUMER_URL,
      secret: SECRET,
      fetch: fetchImpl,
      logger: localLogger,
    });

    const logger = chat.getLogger("prefix");
    logger.info("hello", { extra: true });
    expect(localLogger.info).toHaveBeenCalledWith("hello", { extra: true });

    // notify() is fire-and-forget — give the microtask queue a turn.
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(notified).toHaveLength(1);
    expect(notified[0]).toMatchObject({
      method: "log",
      params: ["info", "prefix", "hello", [{ extra: true }]],
    });
  });

  it("a failed notify() never throws or rejects the caller", async () => {
    const failingFetch = vi
      .fn()
      .mockRejectedValue(new Error("network down")) as unknown as typeof fetch;
    const localLogger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      child: vi.fn().mockReturnThis(),
    };
    const chat = new RemoteChat({
      consumerUrl: CONSUMER_URL,
      secret: SECRET,
      fetch: failingFetch,
      logger: localLogger,
    });

    expect(() => chat.getLogger().error("boom")).not.toThrow();
    expect(localLogger.error).toHaveBeenCalledWith("boom");
  });
});
