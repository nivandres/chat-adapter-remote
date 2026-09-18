import {
  Message,
  emoji,
  getEmoji,
  type Attachment,
  type ReactionEvent,
} from "chat";
import { describe, expect, it, vi } from "vitest";

import { bridge, deferred, handshake, message } from "./testing/bridge";

const THREAD = "mock:general:1";
const user = {
  userId: "u1",
  userName: "alice",
  fullName: "Alice",
  isBot: false,
  isMe: false,
};

describe("inbound events", () => {
  it("delivers a reaction with its emoji singleton and message rebuilt", async () => {
    const b = bridge();
    await handshake(b);
    const received = deferred<ReactionEvent>();
    b.chat.onReaction(async (event) => received.resolve(event));
    const reacted = message("react to me");

    b.hostChat().processReaction({
      added: true,
      emoji: getEmoji("thumbs_up"),
      rawEmoji: "+1",
      message: reacted,
      messageId: reacted.id,
      raw: {},
      threadId: THREAD,
      user,
    } as never);

    const event = await received.promise;
    expect(event.added).toBe(true);
    // Identity, not equality: Chat compares reaction emoji with ===.
    expect(event.emoji).toBe(emoji.thumbs_up);
    expect(event.message).toBeInstanceOf(Message);
    expect(event.message!.text).toBe("react to me");
    expect(event.user.userName).toBe("alice");
  });

  it("delivers an edit with both versions, and a delete with its date", async () => {
    const b = bridge();
    await handshake(b);
    const edited = deferred<{ text: string; previous?: string }>();
    const deleted = deferred<{ messageId: string; deletedAt?: Date }>();
    b.chat.onMessageUpdated(async (_thread, updated, previous) =>
      edited.resolve({ text: updated.text, previous: previous?.text }),
    );
    b.chat.onMessageDeleted(async (event) =>
      deleted.resolve({
        messageId: event.messageId,
        deletedAt: event.deletedAt,
      }),
    );

    await b.hostChat().processMessageUpdated({
      adapter: b.adapter,
      threadId: THREAD,
      message: message("the new text"),
      previousMessage: message("the old text"),
    });
    await b.hostChat().processMessageDeleted({
      adapter: b.adapter,
      threadId: THREAD,
      channelId: "mock:general",
      messageId: "m-gone",
      deletedAt: new Date("2024-03-03T00:00:00.000Z"),
      raw: {},
    });

    expect(await edited.promise).toEqual({
      text: "the new text",
      previous: "the old text",
    });
    expect(await deleted.promise).toEqual({
      messageId: "m-gone",
      deletedAt: new Date("2024-03-03T00:00:00.000Z"),
    });
  });

  it("delivers a button click and a slash command", async () => {
    const b = bridge();
    await handshake(b);
    const clicked = deferred<string | undefined>();
    const commanded = deferred<string>();
    b.chat.onAction("approve", async (event) => clicked.resolve(event.value));
    b.chat.onSlashCommand("/help", async (event) =>
      commanded.resolve(event.text),
    );

    await b.hostChat().processAction(
      {
        adapter: b.adapter,
        actionId: "approve",
        messageId: "m1",
        threadId: THREAD,
        raw: {},
        user,
        value: "42",
      } as never,
      undefined,
    );
    b.hostChat().processSlashCommand(
      {
        adapter: b.adapter,
        channelId: "mock:general",
        command: "/help",
        text: "topics",
        raw: {},
        user,
      } as never,
      undefined,
    );

    expect(await clicked.promise).toBe("42");
    expect(await commanded.promise).toBe("topics");
  });

  it("forwards abortTurn and answers getUserName locally", async () => {
    const b = bridge();
    await handshake(b);
    const abortTurn = vi.spyOn(b.chat, "abortTurn").mockResolvedValue();

    await b.hostChat().abortTurn(THREAD);

    expect(abortTurn).toHaveBeenCalledWith(THREAD);
    expect(b.hostChat().getUserName()).toBe(b.adapter.userName);
  });
});

describe("attachments", () => {
  const png = Buffer.alloc(64, 7);

  function mediaBridge(
    size: number | undefined,
    bytes: Buffer,
    hostOptions = {},
  ) {
    const b = bridge({}, hostOptions);
    const attach = () =>
      message("@mock-bot look", {
        attachments: [
          {
            type: "image",
            name: "a.png",
            mimeType: "image/png",
            size,
            fetchData: async () => bytes,
          },
        ],
      });
    return { b, attach };
  }

  it("inlines small media and still exposes fetchData", async () => {
    const { b, attach } = mediaBridge(png.length, png);
    await handshake(b);
    const received = deferred<Message>();
    b.chat.onNewMention(async (_thread, m) => received.resolve(m));

    await b.hostChat().processMessage(b.adapter, THREAD, attach());

    const [attachment] = (await received.promise).attachments;
    expect(attachment!.data).toEqual(png);
    expect(await attachment!.fetchData!()).toEqual(png);
  });

  it("keeps oversized media on the host and fetches it on demand", async () => {
    const big = Buffer.alloc(4096, 3);
    const { b, attach } = mediaBridge(big.length, big, { maxBodyBytes: 1000 });
    await handshake(b);
    const received = deferred<Message>();
    b.chat.onNewMention(async (_thread, m) => received.resolve(m));

    await b.hostChat().processMessage(b.adapter, THREAD, attach());

    const [attachment] = (await received.promise).attachments;
    // Not in the message body: the whole point is that it never inflated it.
    expect(attachment!.data).toBeUndefined();
    expect(attachment!.name).toBe("a.png");
    expect(await attachment!.fetchData!()).toEqual(big);
  });

  it("never downloads oversized media on the host when the size is known", async () => {
    const big = Buffer.alloc(4096, 3);
    let downloads = 0;
    const b = bridge({}, { maxBodyBytes: 1000 });
    await handshake(b);
    const received = deferred<Message>();
    b.chat.onNewMention(async (_thread, m) => received.resolve(m));

    await b.hostChat().processMessage(
      b.adapter,
      THREAD,
      message("@mock-bot look", {
        attachments: [
          {
            type: "image",
            size: big.length,
            fetchData: async () => {
              downloads++;
              return big;
            },
          },
        ],
      }),
    );

    const [attachment] = (await received.promise).attachments;
    expect(downloads).toBe(0);
    expect(await attachment!.fetchData!()).toEqual(big);
    expect(downloads).toBe(1);
  });

  it("leaves an adapter that can rehydrate to do it itself", async () => {
    const big = Buffer.alloc(4096, 9);
    let held = 0;
    const b = bridge(
      {
        rehydrateAttachment: vi.fn((attachment) => ({
          ...attachment,
          fetchData: async () =>
            Buffer.alloc(4096, Number(attachment.fetchMetadata!.fill)),
        })),
      },
      { maxBodyBytes: 1000 },
    );
    await handshake(b);
    const registry = Reflect.get(b.host, "attachments") as {
      hold: (read: unknown) => string;
    };
    const hold = registry.hold.bind(registry);
    registry.hold = (read) => {
      held++;
      return hold(read);
    };
    const received = deferred<Message>();
    b.chat.onNewMention(async (_thread, m) => received.resolve(m));

    await b.hostChat().processMessage(
      b.adapter,
      THREAD,
      message("@mock-bot look", {
        attachments: [
          {
            type: "image",
            size: big.length,
            fetchMetadata: { fill: "9" },
            fetchData: async () => big,
          },
        ],
      }),
    );

    const [attachment] = (await received.promise).attachments;
    // Nothing kept on our side: no id, no TTL, the adapter rebuilds it.
    expect(held).toBe(0);
    expect(await attachment!.fetchData!()).toEqual(big);
    expect(b.adapter.rehydrateAttachment).toHaveBeenCalled();
  });

  it("honours an explicit inlineAttachments setting", async () => {
    const { b, attach } = mediaBridge(png.length, png, {
      inlineAttachments: false,
    });
    await handshake(b);
    const received = deferred<Message>();
    b.chat.onNewMention(async (_thread, m) => received.resolve(m));

    await b.hostChat().processMessage(b.adapter, THREAD, attach());

    const [attachment] = (await received.promise).attachments;
    expect(attachment!.data).toBeUndefined();
    expect(await attachment!.fetchData!()).toEqual(png);
  });

  it("forwards metadata untouched when the adapter offers no way to read it", async () => {
    const b = bridge();
    await handshake(b);
    const received = deferred<Message>();
    b.chat.onNewMention(async (_thread, m) => received.resolve(m));

    await b.hostChat().processMessage(
      b.adapter,
      THREAD,
      message("@mock-bot look", {
        attachments: [
          { type: "file", name: "linked.pdf", url: "https://x.test/a.pdf" },
        ],
      }),
    );

    const [attachment] = (await received.promise).attachments;
    expect(attachment!.url).toBe("https://x.test/a.pdf");
    expect(attachment!.fetchData).toBeUndefined();
  });
});

describe("events that carry an answer back", () => {
  it("returns the consumer's modal response to the host", async () => {
    const b = bridge();
    await handshake(b);
    b.chat.onModalSubmit("report", async () => ({
      action: "update" as const,
      modal: {
        type: "modal" as const,
        callbackId: "report",
        title: "Done",
        children: [],
      },
    }));

    const answer = await b.hostChat().processModalSubmit(
      {
        adapter: b.adapter,
        callbackId: "report",
        values: { field: "value" },
        raw: {},
        threadId: THREAD,
        user,
      } as never,
      "ctx-1",
    );

    expect(answer).toMatchObject({ action: "update" });
  });

  it("returns select options to the host", async () => {
    const b = bridge();
    await handshake(b);
    b.chat.onOptionsLoad("pick", async () => [
      { label: "One", value: "1" },
      { label: "Two", value: "2" },
    ]);

    const answer = (await b.hostChat().processOptionsLoad({
      adapter: b.adapter,
      actionId: "pick",
      query: "o",
      raw: {},
      user,
    } as never)) as { options?: unknown[] } | unknown[];

    expect(JSON.stringify(answer)).toContain("One");
  });

  it("resolves to undefined rather than throwing when the consumer is unreachable", async () => {
    const { createRemoteChat } = await import("./host/remote-chat");
    const chat = createRemoteChat({
      consumerUrl: "https://consumer.test/inbound",
      secret: "x".repeat(32),
      fetch: async () => {
        throw new Error("connection refused");
      },
    });

    await expect(
      chat.processModalSubmit({ callbackId: "x" } as never, undefined),
    ).resolves.toBeUndefined();
  });

  it("delivers the fire-and-forget platform events", async () => {
    const b = bridge();
    await handshake(b);
    const joined = deferred<string>();
    b.chat.onMemberJoinedChannel(async (event) => joined.resolve(event.userId));

    b.hostChat().processMemberJoinedChannel({
      adapter: b.adapter,
      channelId: "mock:general",
      userId: "u9",
    } as never);

    expect(await joined.promise).toBe("u9");
  });
});

describe("methods that keep a live object on the host", () => {
  it("schedules a message and cancels it through the host", async () => {
    const cancel = vi.fn(async () => undefined);
    let next = 0;
    const b = bridge({
      scheduleMessage: vi.fn(async (_threadId, _message, options) => ({
        scheduledMessageId: `sched-${++next}`,
        channelId: "mock:general",
        postAt: options.postAt,
        raw: { ok: true },
        cancel,
      })),
    });
    await handshake(b);
    const postAt = new Date("2030-01-01T00:00:00.000Z");

    const scheduled = await b.remote.scheduleMessage!(THREAD, "later", {
      postAt,
    });
    expect(scheduled.postAt).toEqual(postAt);

    await scheduled.cancel();
    expect(cancel).toHaveBeenCalledOnce();
    await expect(scheduled.cancel()).rejects.toThrow(/unknown or already due/);
  });

  it("fetches attachment bytes through the host when rehydrated", async () => {
    const b = bridge({
      rehydrateAttachment: vi.fn((attachment: Attachment) => ({
        ...attachment,
        fetchData: async () =>
          Buffer.from(`bytes for ${attachment.fetchMetadata!.mediaId}`),
      })),
    });
    await handshake(b);
    const stored: Attachment = {
      type: "image",
      name: "photo.jpg",
      fetchMetadata: { mediaId: "wa-123" },
    };

    const rehydrated = b.remote.rehydrateAttachment!(stored);
    const data = await rehydrated.fetchData!();

    expect((data as Buffer).toString()).toBe("bytes for wa-123");
    expect(rehydrated.name).toBe("photo.jpg");
  });

  it("keeps bytes it already holds instead of asking the host", async () => {
    const b = bridge({
      rehydrateAttachment: vi.fn((attachment: Attachment) => attachment),
    });
    await handshake(b);
    const data = Buffer.from("already here");

    const rehydrated = b.remote.rehydrateAttachment!({
      type: "image",
      data,
    });

    expect(rehydrated.data).toBe(data);
    expect(b.adapter.rehydrateAttachment).not.toHaveBeenCalled();
  });
});
