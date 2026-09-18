import { describe, expect, it } from "vitest";

import { decode, encode } from "./codec";

describe("codec", () => {
  it("round-trips a Date instead of flattening it to {}", async () => {
    const original = {
      metadata: { dateSent: new Date("2024-01-01T00:00:00.000Z") },
    };
    const restored = decode(await encode(original)) as typeof original;
    expect(restored.metadata.dateSent).toBeInstanceOf(Date);
    expect(restored.metadata.dateSent.toISOString()).toBe(
      "2024-01-01T00:00:00.000Z",
    );
  });

  it("round-trips a Buffer nested in an object", async () => {
    const restored = decode(await encode({ data: Buffer.from("payload") })) as {
      data: Buffer;
    };
    expect(Buffer.isBuffer(restored.data)).toBe(true);
    expect(restored.data.toString()).toBe("payload");
  });

  it("produces JSON-safe output", async () => {
    const encoded = await encode({ at: new Date(), data: Buffer.from("x") });
    expect(JSON.parse(JSON.stringify(encoded))).toEqual(encoded);
  });

  it("accepts the same object referenced twice as siblings", async () => {
    const shared = { id: 1 };
    await expect(encode({ a: shared, b: shared })).resolves.toEqual({
      a: { id: 1 },
      b: { id: 1 },
    });
    await expect(encode([shared, shared])).resolves.toEqual([
      { id: 1 },
      { id: 1 },
    ]);
  });

  it("still rejects a genuine cycle", async () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    await expect(encode(cyclic)).rejects.toThrow(/circular/i);
  });

  it("decodes idempotently", async () => {
    const once = decode(
      await encode({ data: Buffer.from("x"), at: new Date(5) }),
    );
    const twice = decode(once) as { data: Buffer; at: Date };
    expect(Buffer.isBuffer(twice.data)).toBe(true);
    expect(twice.at).toBeInstanceOf(Date);
  });
});
