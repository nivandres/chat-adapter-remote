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
