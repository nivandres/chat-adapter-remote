import { describe, expect, it, vi } from "vitest";

import {
  createRemoteAdapter,
  RemoteAdapter,
  RemoteAdapterUnsupportedSyncMethodError,
} from "./adapter";
import { sign } from "./rpc/signing";

const SECRET = "test-secret";
const URL_ = "https://host.test/rpc";

function jsonRpcFetch(
  handler: (body: { method: string; params: unknown[] }) => unknown,
) {
  return vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    const parsed = JSON.parse(String(init?.body));
    return Response.json({
      jsonrpc: "2.0",
      id: parsed.id,
      result: handler(parsed),
    });
  }) as unknown as typeof fetch;
}

describe("RemoteAdapter outbound methods", () => {
  it("editMessage forwards threadId/messageId/message and returns the RawMessage", async () => {
    const fetchImpl = jsonRpcFetch(({ method, params }) => {
      expect(method).toBe("editMessage");
      expect(params).toEqual(["t1", "m1", "updated"]);
      return { id: "m1", threadId: "t1", raw: {} };
    });
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
      fetch: fetchImpl,
    });
    const result = await adapter.editMessage("t1", "m1", "updated");
    expect(result).toEqual({ id: "m1", threadId: "t1", raw: {} });
  });

  it("deleteMessage forwards threadId/messageId", async () => {
    const fetchImpl = jsonRpcFetch(({ method, params }) => {
      expect(method).toBe("deleteMessage");
      expect(params).toEqual(["t1", "m1"]);
      return null;
    });
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
      fetch: fetchImpl,
    });
    await expect(adapter.deleteMessage("t1", "m1")).resolves.toBeUndefined();
  });

  it("addReaction/removeReaction normalize an EmojiValue to its .name before sending", async () => {
    const fetchImpl = jsonRpcFetch(({ params }) => {
      expect(params[2]).toBe("thumbsup");
      return null;
    });
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
      fetch: fetchImpl,
    });
    const emoji = {
      name: "thumbsup",
      toString: () => "👍",
      toJSON: () => "{{emoji:thumbsup}}",
    };
    await adapter.addReaction("t1", "m1", emoji);
    await adapter.removeReaction("t1", "m1", emoji);
  });

  it("addReaction passes a plain string through unchanged", async () => {
    const fetchImpl = jsonRpcFetch(({ params }) => {
      expect(params[2]).toBe("fire");
      return null;
    });
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
      fetch: fetchImpl,
    });
    await adapter.addReaction("t1", "m1", "fire");
  });

  it("fetchThread forwards and returns ThreadInfo", async () => {
    const fetchImpl = jsonRpcFetch(() => ({
      id: "t1",
      channelId: "c1",
      metadata: {},
    }));
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
      fetch: fetchImpl,
    });
    await expect(adapter.fetchThread("t1")).resolves.toEqual({
      id: "t1",
      channelId: "c1",
      metadata: {},
    });
  });

  it("startTyping forwards status and options", async () => {
    const fetchImpl = jsonRpcFetch(({ params }) => {
      expect(params).toEqual(["t1", "on", { initiatorUserId: "u1" }]);
      return null;
    });
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
      fetch: fetchImpl,
    });
    await adapter.startTyping("t1", "on", { initiatorUserId: "u1" });
  });

  it("disconnect forwards with no params", async () => {
    const fetchImpl = jsonRpcFetch(({ method, params }) => {
      expect(method).toBe("disconnect");
      expect(params).toEqual([]);
      return null;
    });
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
      fetch: fetchImpl,
    });
    await adapter.disconnect();
  });

  it("initialize performs the handshake and adopts the real adapter's identity when not overridden", async () => {
    const fetchImpl = jsonRpcFetch(({ method }) => {
      expect(method).toBe("__handshake");
      return { name: "chatwoot", userName: "chatwoot-bot", botUserId: "42" };
    });
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      fetch: fetchImpl,
    });
    await adapter.initialize({} as never);
    expect(adapter.name).toBe("chatwoot");
    expect(adapter.userName).toBe("chatwoot-bot");
    expect(adapter.botUserId).toBe("42");
  });

  it("initialize keeps explicitly configured name/userName instead of the handshake's", async () => {
    const fetchImpl = jsonRpcFetch(() => ({
      name: "chatwoot",
      userName: "chatwoot-bot",
      botUserId: "42",
    }));
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "custom",
      userName: "custom-bot",
      fetch: fetchImpl,
    });
    await adapter.initialize({} as never);
    expect(adapter.name).toBe("custom");
    expect(adapter.userName).toBe("custom-bot");
  });
});

describe("RemoteAdapter unsupported synchronous methods", () => {
  const adapter = new RemoteAdapter({
    url: URL_,
    secret: SECRET,
    name: "mock",
  });

  it("encodeThreadId/decodeThreadId/renderFormatted throw a typed, documented error", () => {
    expect(() => adapter.encodeThreadId()).toThrow(
      RemoteAdapterUnsupportedSyncMethodError,
    );
    expect(() => adapter.decodeThreadId()).toThrow(
      RemoteAdapterUnsupportedSyncMethodError,
    );
    expect(() => adapter.renderFormatted({} as never)).toThrow(
      RemoteAdapterUnsupportedSyncMethodError,
    );
  });

  it("parseMessage throws — messages only ever arrive pre-parsed via handleWebhook", () => {
    expect(() => adapter.parseMessage()).toThrow(/never called/);
  });
});

describe("RemoteAdapter.handleWebhook security", () => {
  async function signedRequest(secret: string, body: unknown) {
    const text = JSON.stringify(body);
    const timestamp = String(Date.now());
    return new Request("https://consumer.test/inbound", {
      method: "POST",
      body: text,
      headers: {
        "x-chat-adapter-remote-signature": sign(text, timestamp, secret),
        "x-chat-adapter-remote-timestamp": timestamp,
      },
    });
  }

  it("rejects a request signed with the wrong secret", async () => {
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
    });
    const request = await signedRequest("wrong-secret", {
      jsonrpc: "2.0",
      id: 1,
      method: "log",
      params: ["info", "", "hi", []],
    });
    const response = await adapter.handleWebhook(request);
    expect(response.status).toBe(401);
  });

  it("acknowledges but ignores an unrecognized inbound method rather than throwing", async () => {
    const adapter = createRemoteAdapter({
      url: URL_,
      secret: SECRET,
      name: "mock",
    });
    const request = await signedRequest(SECRET, {
      jsonrpc: "2.0",
      id: 1,
      method: "processAction",
      params: [],
    });
    const response = await adapter.handleWebhook(request);
    expect(response.status).toBe(200);
  });
});
