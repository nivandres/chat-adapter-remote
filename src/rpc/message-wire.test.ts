import { Message, parseMarkdown } from "chat";
import { describe, expect, it, vi } from "vitest";

import { deserializeMessage, serializeMessage } from "./message-wire";

function message(
  text: string,
  overrides: Partial<ConstructorParameters<typeof Message>[0]> = {},
): Message {
  return new Message({
    id: "m1",
    threadId: "mock:c:1",
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

describe("message wire", () => {
  it("preserves replyTo, the most common real message shape", async () => {
    const original = message("this is a reply", {
      replyTo: message("the original"),
    });
    const restored = deserializeMessage(await serializeMessage(original));
    expect(restored.text).toBe("this is a reply");
    expect(restored.replyTo?.text).toBe("the original");
  });

  it("preserves dates as Date instances", async () => {
    const restored = deserializeMessage(await serializeMessage(message("hi")));
    expect(restored.metadata.dateSent).toBeInstanceOf(Date);
    expect(restored.metadata.dateSent.toISOString()).toBe(
      "2024-01-01T00:00:00.000Z",
    );
  });

  it("resolves fetchData into bytes before sending", async () => {
    const fetchData = vi.fn().mockResolvedValue(Buffer.from("fetched"));
    const restored = deserializeMessage(
      await serializeMessage(
        message("see attached", {
          attachments: [{ type: "file", name: "a.txt", fetchData }],
        }),
      ),
    );
    expect(fetchData).toHaveBeenCalledOnce();
    expect((restored.attachments[0]!.data as Buffer).toString()).toBe(
      "fetched",
    );
  });

  it("forwards an attachment as metadata-only when fetchData fails", async () => {
    const onError = vi.fn();
    const fetchData = vi.fn().mockRejectedValue(new Error("gone"));
    const restored = deserializeMessage(
      await serializeMessage(
        message("x", {
          attachments: [{ type: "file", name: "a.txt", fetchData }],
        }),
        onError,
      ),
    );
    expect(onError).toHaveBeenCalledOnce();
    expect(restored.attachments[0]!.name).toBe("a.txt");
    expect(restored.attachments[0]!.data).toBeUndefined();
  });

  it("carries fields this package does not know about", async () => {
    const original = message("hi");
    const wire = (await serializeMessage(original)) as Record<string, unknown>;
    wire.somethingChatAddedLater = "value";
    expect(() => deserializeMessage(wire)).not.toThrow();
  });
});
