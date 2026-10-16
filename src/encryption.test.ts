import { createMockAdapter } from "@chat-adapter/tests";
import type { ChatInstance } from "chat";
import { describe, expect, it, vi } from "vitest";

import { createRemoteAdapter } from "./adapter";
import { serveAdapter } from "./host";
import { RpcErrorCode } from "./rpc/errors";
import { seal, unseal } from "./rpc/sealing";
import { CONSUMER_URL, HOST_URL, SECRET, message } from "./testing/bridge";
import type { FetchLike } from "./types";

const THREAD = "mock:general:1";

/** A host and a consumer over a loopback that records every body that crosses it. */
function wired(host: { encrypt?: boolean }, consumer: { encrypt?: boolean }) {
  const wire: string[] = [];
  let chat!: ChatInstance;
  const adapter = createMockAdapter("mock", {
    initialize: vi.fn(async (instance: ChatInstance) => void (chat = instance)),
  });
  const remote = createRemoteAdapter({
    url: HOST_URL,
    secret: SECRET,
    name: "mock",
    ...consumer,
    fetch: async (input, init) => {
      wire.push(String(init?.body));
      const response = await served.handleRequest(new Request(input, init));
      wire.push(await response.clone().text());
      return response;
    },
  });
  const toConsumer: FetchLike = async (input, init) => {
    wire.push(String(init?.body));
    const response = await remote.handleWebhook(new Request(input, init));
    wire.push(await response.clone().text());
    return response;
  };
  const served = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch: toConsumer,
    ...host,
  });
  return { adapter, remote, served, wire, chat: () => chat };
}

describe("sealing", () => {
  it("opens only what was sealed for the same context, under one of the secrets", () => {
    const sealed = seal("hello", SECRET, "request.n1");

    expect(unseal(sealed, SECRET, "request.n1")).toBe("hello");
    expect(unseal(sealed, ["new", SECRET], "request.n1")).toBe("hello");
    expect(unseal(sealed, SECRET, "response.n1")).toBeNull();
    expect(unseal(sealed, "other", "request.n1")).toBeNull();
    const tampered = JSON.parse(sealed) as { sealed: string };
    tampered.sealed = `${tampered.sealed.slice(0, -4)}AAAA`;
    expect(unseal(JSON.stringify(tampered), SECRET, "request.n1")).toBeNull();
  });
});

describe("encrypted mode", () => {
  it("carries calls and their answers both ways with nothing readable on the wire", async () => {
    const b = wired({ encrypt: true }, { encrypt: true });
    await b.served.ready;
    const received = new Promise<string>((resolve) =>
      b.remote.initialize({
        getLogger: () => ({ child: () => ({}) }) as never,
        getState: () => undefined,
        processMessage: (
          _adapter: unknown,
          _thread: string,
          incoming: { text: string },
        ) => resolve(incoming.text),
      } as never),
    );

    await b.remote.postMessage(THREAD, "the secret plan");
    await b
      .chat()
      .processMessage(b.adapter, THREAD, message("a private reply"));

    expect(await received).toBe("a private reply");
    expect(b.adapter.postMessage).toHaveBeenCalledWith(
      THREAD,
      "the secret plan",
    );
    for (const body of b.wire) {
      expect(body).not.toMatch(
        /secret plan|private reply|postMessage|processMessage/,
      );
    }
  });

  it("says so when only one side encrypts", async () => {
    const consumerOnly = wired({}, { encrypt: true });
    await consumerOnly.served.ready;
    await expect(consumerOnly.remote.postMessage(THREAD, "x")).rejects.toThrow(
      /encrypt on both sides/,
    );

    const hostOnly = wired({ encrypt: true }, {});
    await hostOnly.served.ready;
    await expect(hostOnly.remote.postMessage(THREAD, "x")).rejects.toThrow(
      /encrypt on both sides/,
    );
  });

  it("believes no result that was not sealed for its own request", async () => {
    let swapped: string | undefined;
    const forged = createRemoteAdapter({
      url: HOST_URL,
      secret: SECRET,
      encrypt: true,
      fetch: async (_input, init = {}) => {
        const nonce = (init.headers as Record<string, string>)[
          "x-chat-adapter-remote-nonce"
        ];
        const { id } = JSON.parse(
          unseal(String(init.body), SECRET, `request.${nonce}`)!,
        ) as { id: string };
        const answer = JSON.stringify({
          jsonrpc: "2.0",
          id,
          result: { id: "forged" },
        });
        // A plain answer, then one sealed for another request.
        const body = swapped ?? answer;
        swapped = seal(answer, SECRET, "response.someone-else");
        return new Response(body);
      },
    });

    for (let attempt = 0; attempt < 2; attempt++) {
      await expect(forged.postMessage(THREAD, "x")).rejects.toMatchObject({
        code: RpcErrorCode.UNAUTHORIZED,
      });
    }
  });
});
