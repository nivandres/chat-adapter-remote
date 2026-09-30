import { createMemoryState } from "@chat-adapter/state-memory";
import { createMockAdapter } from "@chat-adapter/tests";
import {
  Chat,
  Message,
  parseMarkdown,
  type Adapter,
  type ChatInstance,
} from "chat";
import { vi } from "vitest";

import { createRemoteAdapter, type RemoteAdapter } from "../adapter";
import {
  serveAdapter,
  type AdapterHost,
  type ServeAdapterOptions,
} from "../host";
import type { FetchLike, RemoteAdapterConfig } from "../types";

export const SECRET =
  "8f2a1c9e4b7d0a6538e1c4f9b2d7a05c3e6f81b4d9a2c705e8f3b6d1a4c7e092";
export const HOST_URL = "https://host.test/rpc";
export const CONSUMER_URL = "https://consumer.test/inbound";

export function message(
  text: string,
  overrides: Partial<ConstructorParameters<typeof Message>[0]> = {},
): Message {
  return new Message({
    id: `m-${Math.random()}`,
    threadId: "mock:general:1",
    text,
    formatted: parseMarkdown(text),
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

export function deferred<T>() {
  let resolve!: (value: T) => void;
  return { promise: new Promise<T>((r) => (resolve = r)), resolve };
}

export interface Bridge {
  host: AdapterHost;
  remote: RemoteAdapter;
  chat: Chat;
  adapter: Adapter;
  hostChat: () => ChatInstance;
}

/** A consumer and a host wired to each other through an in-memory `fetch`, exercising the real HTTP request/response path. */
export function bridge(
  overrides: Partial<Adapter> = {},
  hostOptions: Partial<ServeAdapterOptions> = {},
  consumerOptions: Partial<RemoteAdapterConfig> = {},
): Bridge {
  let captured: ChatInstance | undefined;
  const adapter = createMockAdapter("mock", {
    initialize: vi.fn(async (chatInstance: ChatInstance) => {
      captured = chatInstance;
    }),
    ...overrides,
  });

  let host: AdapterHost;
  let chat: Chat;
  const loopback: FetchLike = async (input, init) => {
    const request = new Request(input, init);
    return new URL(request.url).hostname === "host.test"
      ? host.handleRequest(request)
      : chat.webhooks.mock!(request, {});
  };

  host = serveAdapter(adapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
    fetch: loopback,
    ...hostOptions,
  });
  const remote = createRemoteAdapter({
    url: HOST_URL,
    secret: SECRET,
    name: "mock",
    fetch: loopback,
    ...consumerOptions,
  });
  chat = new Chat({
    userName: "mock-bot",
    adapters: { mock: remote },
    state: createMemoryState(),
  });

  return {
    host,
    remote,
    chat,
    adapter,
    hostChat: () => {
      if (!captured) throw new Error("adapter was not initialized");
      return captured;
    },
  };
}

/** Chat initializes its adapters lazily, so a delivery attempt is what triggers the handshake. */
export async function handshake(b: Bridge): Promise<void> {
  await b.host.ready;
  await b.chat.webhooks.mock!(new Request(CONSUMER_URL, { method: "POST" }));
}
