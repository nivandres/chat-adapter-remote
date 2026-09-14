const BUFFER_TAG = "__chatAdapterRemoteBuffer";

interface WireBuffer {
  [BUFFER_TAG]: true;
  base64: string;
}

function isWireBuffer(value: unknown): value is WireBuffer {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as Record<string, unknown>)[BUFFER_TAG] === true
  );
}

async function toBuffer(value: Buffer | ArrayBuffer | Blob): Promise<Buffer> {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof ArrayBuffer) return Buffer.from(value);
  return Buffer.from(await value.arrayBuffer());
}

/** Deep-walks arrays and plain objects, replacing any Buffer/ArrayBuffer/Blob with a JSON-safe wire representation. */
export async function encodeBuffers(
  value: unknown,
  seen = new WeakSet<object>(),
): Promise<unknown> {
  if (value == null) return value;
  if (
    Buffer.isBuffer(value) ||
    value instanceof ArrayBuffer ||
    value instanceof Blob
  ) {
    const buffer = await toBuffer(value);
    return {
      [BUFFER_TAG]: true,
      base64: buffer.toString("base64"),
    } satisfies WireBuffer;
  }
  if (Array.isArray(value)) {
    if (seen.has(value))
      throw new Error(
        "chat-adapter-remote: circular reference cannot be sent over RPC",
      );
    seen.add(value);
    return Promise.all(value.map((item) => encodeBuffers(item, seen)));
  }
  if (typeof value === "object") {
    if (seen.has(value))
      throw new Error(
        "chat-adapter-remote: circular reference cannot be sent over RPC",
      );
    seen.add(value);
    const entries = await Promise.all(
      Object.entries(value as Record<string, unknown>).map(
        async ([key, item]) => [key, await encodeBuffers(item, seen)] as const,
      ),
    );
    return Object.fromEntries(entries);
  }
  return value;
}

/**
 * Inverse of {@link encodeBuffers}. Must stay idempotent: a value that's
 * already a real Buffer passes through untouched instead of falling into
 * the object branch, which would otherwise enumerate its byte indices via
 * `Object.entries` and corrupt it into a plain `{0: ..., 1: ...}` object.
 */
export function decodeBuffers(value: unknown): unknown {
  if (value == null) return value;
  if (Buffer.isBuffer(value)) return value;
  if (isWireBuffer(value)) return Buffer.from(value.base64, "base64");
  if (Array.isArray(value)) return value.map((item) => decodeBuffers(item));
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [
        key,
        decodeBuffers(item),
      ]),
    );
  }
  return value;
}
