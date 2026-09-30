import {
  AdapterError,
  AdapterRateLimitError,
  AuthenticationError,
  PermissionError,
  ResourceNotFoundError,
} from "@chat-adapter/shared";
import { Message, parseMarkdown } from "chat";
import { describe, expect, it, vi } from "vitest";

import { decode, encode } from "./codec";
import { RpcErrorCode, deserializeError, serializeError } from "./errors";
import { deserializeMessage, serializeMessage } from "./message-wire";
import { createReplayGuard } from "./security";
import { isTimestampFresh, sign, verify } from "./signing";
import { createRpcClient } from "./transport";

function message(overrides: Record<string, unknown> = {}): Message {
  return new Message({
    id: "m1",
    threadId: "mock:general:1",
    text: "hello",
    formatted: parseMarkdown("hello"),
    raw: {},
    author: {
      userId: "u1",
      userName: "alice",
      fullName: "Alice",
      isBot: false,
      isMe: false,
    },
    metadata: { dateSent: new Date("2024-01-01T00:00:00.000Z"), edited: false },
    attachments: [],
    ...overrides,
  });
}

describe("signing", () => {
  const timestamp = String(Date.now());

  it("verifies a body it signed and rejects anything else", () => {
    const signature = sign("payload", timestamp, "n1", "secret");

    expect(verify("payload", timestamp, "n1", signature, "secret")).toBe(true);
    expect(verify("tampered", timestamp, "n1", signature, "secret")).toBe(
      false,
    );
    expect(verify("payload", timestamp, "n2", signature, "secret")).toBe(false);
    expect(verify("payload", timestamp, "n1", signature, "other")).toBe(false);
    expect(verify("payload", timestamp, "n1", "short", "secret")).toBe(false);
  });

  it("accepts timestamps inside the window only", () => {
    expect(isTimestampFresh("1000", 30_000, 31_000)).toBe(true);
    expect(isTimestampFresh("1000", 30_000, 31_001)).toBe(false);
    expect(isTimestampFresh("not-a-number", 30_000)).toBe(false);
  });

  it("accepts a signature once inside its window", async () => {
    const guard = createReplayGuard();

    expect(await guard.seen("sig", 30_000)).toBe(false);
    expect(await guard.seen("sig", 30_000)).toBe(true);
  });
});

describe("codec", () => {
  it("round-trips the values JSON alone would flatten", async () => {
    const value = {
      when: new Date("2024-01-01T00:00:00.000Z"),
      bytes: Buffer.from("hello"),
      nested: [{ also: Buffer.from("world") }],
    };

    const wire = JSON.parse(JSON.stringify(await encode(value)));
    const restored = decode(wire) as typeof value;

    expect(restored.when).toBeInstanceOf(Date);
    expect(restored.when).toEqual(value.when);
    expect(restored.bytes.toString()).toBe("hello");
    expect(restored.nested[0]!.also.toString()).toBe("world");
    expect(decode(restored)).toEqual(restored);
  });

  it("allows shared references but not cycles", async () => {
    const shared = { id: 1 };
    await expect(encode({ a: shared, b: shared })).resolves.toBeDefined();

    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    await expect(encode(cyclic)).rejects.toThrow(/circular/);
  });
});

describe("message wire", () => {
  it("preserves replyTo and dates", async () => {
    const original = message({ replyTo: message({ id: "m0", text: "first" }) });

    const restored = deserializeMessage(await serializeMessage(original));

    expect(restored.text).toBe("hello");
    expect(restored.replyTo?.text).toBe("first");
    expect(restored.metadata.dateSent).toEqual(original.metadata.dateSent);
  });

  it("resolves attachment bytes, and forwards metadata when that fails", async () => {
    const resolved = await serializeMessage(
      message({
        attachments: [
          {
            type: "file",
            name: "ok.txt",
            fetchData: async () => Buffer.from("bytes"),
          },
        ],
      }),
    );
    expect(deserializeMessage(resolved).attachments[0]!.data?.toString()).toBe(
      "bytes",
    );

    const onError = vi.fn();
    const failed = await serializeMessage(
      message({
        attachments: [
          {
            type: "file",
            name: "gone.txt",
            fetchData: async () => {
              throw new Error("404");
            },
          },
        ],
      }),
      undefined,
      onError,
    );
    // The SDK reads attachments through fetchData, so a delivered attachment
    // has to expose it or `toAiMessages` silently drops the image.
    const rebuilt = deserializeMessage(resolved).attachments[0]!;
    expect(typeof rebuilt.fetchData).toBe("function");
    expect((await rebuilt.fetchData!()).toString()).toBe("bytes");

    expect(deserializeMessage(failed).attachments[0]!.data).toBeUndefined();
    expect(deserializeMessage(failed).attachments[0]!.name).toBe("gone.txt");
    expect(onError).toHaveBeenCalled();
  });
});

describe("errors", () => {
  it("rebuilds adapter error classes with the fields callers branch on", () => {
    const cases = [
      new AdapterRateLimitError("mock", 42),
      new AuthenticationError("mock", "bad token"),
      new ResourceNotFoundError("mock", "channel", "C1"),
      new PermissionError("mock", "post", "chat:write"),
      new AdapterError("boom", "mock", "CUSTOM"),
    ];

    for (const original of cases) {
      const restored = deserializeError(serializeError(original));
      expect(restored).toBeInstanceOf(original.constructor);
      expect(restored.message).toBe(original.message);
    }

    const rate = deserializeError(
      serializeError(new AdapterRateLimitError("mock", 42)),
    ) as AdapterRateLimitError;
    expect(rate.retryAfter).toBe(42);
  });

  it("does not leak the message of an unrecognized error", () => {
    const wire = serializeError(new Error("internal hostname db-01.internal"));

    expect(wire.code).toBe(RpcErrorCode.INTERNAL_ERROR);
    expect(wire.message).not.toContain("db-01");
  });
});

describe("transport", () => {
  it("refuses a response larger than the body limit", async () => {
    const client = createRpcClient({
      url: "https://host.test/rpc",
      secret: "s",
      maxBodyBytes: 1024,
      fetch: async (_url, init) =>
        Response.json({
          jsonrpc: "2.0",
          id: JSON.parse(String(init?.body)).id,
          result: "x".repeat(4096),
        }),
    });

    await expect(client.request("fetchAttachment", ["a1"])).rejects.toThrow(
      /larger than/,
    );
  });

  it("matches responses to their own request and never throws from notify", async () => {
    const client = createRpcClient({
      url: "https://host.test/rpc",
      secret: "s",
      fetch: async (_url, init) =>
        Response.json({
          jsonrpc: "2.0",
          id: JSON.parse(String(init?.body)).id,
          result: { ok: true },
        }),
    });

    expect(await client.request("postMessage", [])).toEqual({ ok: true });

    const mismatched = createRpcClient({
      url: "https://host.test/rpc",
      secret: "s",
      fetch: async () =>
        Response.json({ jsonrpc: "2.0", id: 999, result: null }),
    });
    await expect(mismatched.request("postMessage", [])).rejects.toThrow(
      /did not match/,
    );

    expect(() =>
      mismatched.notify("log", ["info", "", "hi", []]),
    ).not.toThrow();
  });
});

describe("host bookkeeping", () => {
  it("expires what the consumer stopped asking for", async () => {
    vi.useFakeTimers();
    try {
      const { ExpiringMap } = await import("../host/expiring");
      const expired: string[] = [];
      const map = new ExpiringMap<string>(1000, (value) => expired.push(value));
      const id = map.add("x", "held");

      expect(map.get(id)).toBe("held");
      vi.setSystemTime(Date.now() + 1500);
      await vi.advanceTimersByTimeAsync(1100);

      expect(expired).toEqual(["held"]);
      expect(map.get(id)).toBeUndefined();
      expect(map.size).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps the forwarding threshold across child loggers", async () => {
    const { createBridgingLogger } = await import("../host/logger-bridge");
    const sent: string[] = [];
    const local = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      child: vi.fn(function (this: unknown) {
        return this;
      }),
    };
    const logger = createBridgingLogger({
      localLogger: local as never,
      forwardLevel: "warn",
      notify: (_level, _prefix, text) => sent.push(text),
    });

    // Baileys logs through a child, so the bridge has to survive one.
    logger.child("baileys").warn("reconnecting");
    logger.child("baileys").debug("noise");

    expect(sent).toEqual(["reconnecting"]);
  });
});
