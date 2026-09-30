import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import {
  Chat,
  Message,
  type Adapter,
  type ChatInstance,
  type Thread,
} from "chat";
import { describe, expect, it, vi } from "vitest";

import { createRemoteAdapter } from "./adapter";
import { serveAdapter } from "./host";
import {
  CONSUMER_URL,
  HOST_URL,
  SECRET,
  deferred,
  message,
} from "./testing/bridge";
import { translateIds } from "./thread-ids";
import type { FetchLike } from "./types";

/** A host adapter called "whatsapp", registered on the consumer as "remote". */
async function renamed(overrides: Partial<Adapter> = {}) {
  let hostChat!: ChatInstance;
  const adapter = createMockAdapter("whatsapp", {
    initialize: vi.fn(
      async (instance: ChatInstance) => void (hostChat = instance),
    ),
    ...overrides,
  });
  let chat!: Chat;
  const host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch: async (input, init) =>
      chat.webhooks.remote!(new Request(input, init), {}),
  });
  const toHost: FetchLike = (input, init) =>
    host.handleRequest(new Request(input, init));
  const remote = createRemoteAdapter({
    url: HOST_URL,
    secret: SECRET,
    fetch: toHost,
  });
  chat = new Chat({
    userName: "bot",
    adapters: { remote },
    state: createMemoryState(),
  });
  await chat.initialize();
  await host.ready;
  return { adapter, chat, remote, hostChat: () => hostChat };
}

describe("thread id prefixes", () => {
  it("hands Chat ids under the key it was registered with", async () => {
    const r = await renamed();
    const received = deferred<{ thread: Thread; message: Message }>();
    r.chat.onNewMention(async (thread, m) =>
      received.resolve({ thread, message: m }),
    );

    await r
      .hostChat()
      .processMessage(
        r.adapter,
        "whatsapp:general:1",
        message("@whatsapp-bot hi", { threadId: "whatsapp:general:1" }),
      );

    const { thread, message: got } = await received.promise;
    expect(thread.id).toBe("remote:general:1");
    expect(got.threadId).toBe("remote:general:1");
  });

  it("sends the host its own ids back", async () => {
    const r = await renamed();
    const done = deferred<void>();
    r.chat.onNewMention(async (thread) => {
      await thread.post("reply");
      done.resolve();
    });

    await r
      .hostChat()
      .processMessage(
        r.adapter,
        "whatsapp:general:1",
        message("@whatsapp-bot hi", { threadId: "whatsapp:general:1" }),
      );
    await done.promise;

    expect(r.adapter.postMessage).toHaveBeenCalledWith(
      "whatsapp:general:1",
      "reply",
    );
  });

  it("routes a thread opened by id straight to the host", async () => {
    const r = await renamed();

    await r.chat.thread("remote:general:1").post("proactive");

    expect(r.adapter.postMessage).toHaveBeenCalledWith(
      "whatsapp:general:1",
      "proactive",
    );
  });

  it("translates the thread id openDM answers with", async () => {
    const r = await renamed({ openDM: vi.fn(async () => "whatsapp:D42") });

    expect(await r.remote.openDM!("u1")).toBe("remote:D42");
  });

  it("never rewrites message content that happens to start with the prefix", async () => {
    const r = await renamed();

    await r.remote.postMessage(
      "remote:general:1",
      "whatsapp: is how I reach you",
    );

    expect(r.adapter.postMessage).toHaveBeenCalledWith(
      "whatsapp:general:1",
      "whatsapp: is how I reach you",
    );
  });

  it("leaves raw platform data and formatted text alone", () => {
    const translated = translateIds(
      {
        threadId: "whatsapp:1",
        raw: { threadId: "whatsapp:1" },
        formatted: { id: "whatsapp:node" },
        text: "whatsapp:1",
      },
      "whatsapp",
      "remote",
    );

    expect(translated).toEqual({
      threadId: "remote:1",
      raw: { threadId: "whatsapp:1" },
      formatted: { id: "whatsapp:node" },
      text: "whatsapp:1",
    });
  });
});
