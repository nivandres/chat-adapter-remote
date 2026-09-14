import { Message, parseMarkdown } from "chat";
import { describe, expect, it, vi } from "vitest";

import {
  deserializeMessageFromWire,
  serializeMessageForWire,
} from "./message-wire";

function baseMessage(attachments: Message["attachments"] = []): Message {
  return new Message({
    id: "m1",
    threadId: "t1",
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
    attachments,
  });
}

describe("serializeMessageForWire / deserializeMessageFromWire", () => {
  it("round-trips a message with an inline Buffer attachment", async () => {
    const message = baseMessage([
      { type: "file", name: "a.txt", data: Buffer.from("payload") },
    ]);
    const wire = await serializeMessageForWire(message);
    const restored = deserializeMessageFromWire(wire);
    expect(restored.text).toBe("hello");
    expect(Buffer.isBuffer(restored.attachments[0]!.data)).toBe(true);
    expect((restored.attachments[0]!.data as Buffer).toString()).toBe(
      "payload",
    );
  });

  it("eagerly resolves fetchData into a real Buffer before serialization", async () => {
    const fetchData = vi.fn().mockResolvedValue(Buffer.from("fetched"));
    const message = baseMessage([{ type: "file", name: "b.txt", fetchData }]);
    const wire = await serializeMessageForWire(message);
    expect(fetchData).toHaveBeenCalledOnce();
    const restored = deserializeMessageFromWire(wire);
    expect((restored.attachments[0]!.data as Buffer).toString()).toBe(
      "fetched",
    );
  });

  it("forwards an attachment as metadata-only when fetchData throws, without failing the whole message", async () => {
    const fetchData = vi.fn().mockRejectedValue(new Error("broken media URL"));
    const onFetchDataError = vi.fn();
    const message = baseMessage([{ type: "file", name: "c.txt", fetchData }]);
    const wire = await serializeMessageForWire(message, onFetchDataError);
    expect(onFetchDataError).toHaveBeenCalledOnce();
    const restored = deserializeMessageFromWire(wire);
    expect(restored.text).toBe("hello");
    expect(restored.attachments[0]!.data).toBeUndefined();
    expect(restored.attachments[0]!.name).toBe("c.txt");
  });

  it("leaves an attachment with neither data nor fetchData as metadata-only", async () => {
    const message = baseMessage([
      { type: "image", url: "https://example.com/x.png" },
    ]);
    const wire = await serializeMessageForWire(message);
    const restored = deserializeMessageFromWire(wire);
    expect(restored.attachments[0]!.url).toBe("https://example.com/x.png");
    expect(restored.attachments[0]!.data).toBeUndefined();
  });

  it("preserves message dates through the round trip", async () => {
    const message = baseMessage();
    const wire = await serializeMessageForWire(message);
    const restored = deserializeMessageFromWire(wire);
    expect(restored.metadata.dateSent).toBeInstanceOf(Date);
    expect(restored.metadata.dateSent.toISOString()).toBe(
      "2024-01-01T00:00:00.000Z",
    );
  });
});
