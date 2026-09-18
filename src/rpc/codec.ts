const BUFFER_TAG = "__charBuffer";
const DATE_TAG = "__charDate";

interface WireBuffer {
  [BUFFER_TAG]: string;
}

interface WireDate {
  [DATE_TAG]: string;
}

function isWireBuffer(value: object): value is WireBuffer {
  return typeof (value as WireBuffer)[BUFFER_TAG] === "string";
}

function isWireDate(value: object): value is WireDate {
  return typeof (value as WireDate)[DATE_TAG] === "string";
}

async function toBuffer(value: Buffer | ArrayBuffer | Blob): Promise<Buffer> {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof ArrayBuffer) return Buffer.from(value);
  return Buffer.from(await value.arrayBuffer());
}

/**
 * Replaces Buffer/ArrayBuffer/Blob and Date with JSON-safe wire forms.
 * `ancestors` tracks the current path, not every value seen, so a value
 * referenced twice as siblings is not mistaken for a cycle.
 */
export async function encode(
  value: unknown,
  ancestors = new Set<object>(),
): Promise<unknown> {
  if (value === null || typeof value !== "object") return value;
  if (value instanceof Date) return { [DATE_TAG]: value.toISOString() };
  if (
    Buffer.isBuffer(value) ||
    value instanceof ArrayBuffer ||
    value instanceof Blob
  ) {
    return { [BUFFER_TAG]: (await toBuffer(value)).toString("base64") };
  }
  if (ancestors.has(value))
    throw new TypeError(
      "chat-adapter-remote: circular reference cannot be sent over RPC",
    );

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const items = [];
      for (const item of value) items.push(await encode(item, ancestors));
      return items;
    }
    const entries: Array<[string, unknown]> = [];
    for (const [key, item] of Object.entries(value))
      entries.push([key, await encode(item, ancestors)]);
    return Object.fromEntries(entries);
  } finally {
    ancestors.delete(value);
  }
}

/** Inverse of {@link encode}. Idempotent: already-decoded Buffers and Dates pass through untouched. */
export function decode(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Buffer.isBuffer(value) || value instanceof Date) return value;
  if (isWireBuffer(value)) return Buffer.from(value[BUFFER_TAG], "base64");
  if (isWireDate(value)) return new Date(value[DATE_TAG]);
  if (Array.isArray(value)) return value.map(decode);
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, decode(item)]),
  );
}
