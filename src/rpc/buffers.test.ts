import { describe, expect, it } from "vitest";

import { decodeBuffers, encodeBuffers } from "./buffers";

describe("encodeBuffers/decodeBuffers", () => {
  it("round-trips a Buffer inside a nested object", async () => {
    const original = {
      attachments: [{ name: "a.png", data: Buffer.from("hello") }],
    };
    const encoded = await encodeBuffers(original);
    expect(JSON.parse(JSON.stringify(encoded))).toEqual(encoded); // fully JSON-safe
    const decoded = decodeBuffers(encoded) as typeof original;
    expect(Buffer.isBuffer(decoded.attachments[0]!.data)).toBe(true);
    expect((decoded.attachments[0]!.data as Buffer).toString()).toBe("hello");
  });

  it("round-trips an ArrayBuffer", async () => {
    const bytes = new Uint8Array([1, 2, 3]).buffer;
    const encoded = await encodeBuffers({ data: bytes });
    const decoded = decodeBuffers(encoded) as { data: Buffer };
    expect(Buffer.from(decoded.data)).toEqual(Buffer.from(bytes));
  });

  it("leaves primitives, arrays, and plain objects untouched", async () => {
    const original = { a: 1, b: "text", c: [1, 2, { d: true }], e: null };
    const encoded = await encodeBuffers(original);
    expect(decodeBuffers(encoded)).toEqual(original);
  });

  it("throws on a circular reference instead of hanging", async () => {
    const circular: Record<string, unknown> = { a: 1 };
    circular.self = circular;
    await expect(encodeBuffers(circular)).rejects.toThrow(/circular/i);
  });

  it("decodeBuffers is idempotent on an already-decoded Buffer", async () => {
    const original = { attachments: [{ data: Buffer.from("hello") }] };
    const encoded = await encodeBuffers(original);
    const decodedOnce = decodeBuffers(encoded) as typeof original;
    const decodedTwice = decodeBuffers(decodedOnce) as typeof original;
    expect(Buffer.isBuffer(decodedTwice.attachments[0]!.data)).toBe(true);
    expect(decodedTwice.attachments[0]!.data.toString()).toBe("hello");
  });
});
