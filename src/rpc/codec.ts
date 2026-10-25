const BUFFER_TAG = "__charBuffer";
const DATE_TAG = "__charDate";
const OBJECT_TAG = "__charObject";
const TAGS = [BUFFER_TAG, DATE_TAG, OBJECT_TAG];

interface WireBuffer {
  [BUFFER_TAG]: string;
}

interface WireDate {
  [DATE_TAG]: string;
}

interface WireObject {
  [OBJECT_TAG]: object;
}

function isWireBuffer(value: object): value is WireBuffer {
  return typeof (value as WireBuffer)[BUFFER_TAG] === "string";
}

function isWireDate(value: object): value is WireDate {
  return typeof (value as WireDate)[DATE_TAG] === "string";
}

function isWireObject(value: object): value is WireObject {
  return (value as WireObject)[OBJECT_TAG] instanceof Object;
}

async function toBuffer(value: Buffer | ArrayBuffer | Blob): Promise<Buffer> {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof ArrayBuffer) return Buffer.from(value);
  return Buffer.from(await value.arrayBuffer());
}

/**
 * Replaces Buffer/ArrayBuffer/Blob and Date with JSON-safe wire forms, and
 * wraps an object that would pass for one, so it arrives as it was.
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
    const object = Object.fromEntries(entries);
    return TAGS.some((tag) => tag in object)
      ? { [OBJECT_TAG]: object }
      : object;
  } finally {
    ancestors.delete(value);
  }
}

/** Inverse of {@link encode}, once per crossing: a second pass would read an object it unwrapped as a wire form. */
export function decode(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (Buffer.isBuffer(value) || value instanceof Date) return value;
  if (isWireBuffer(value)) return Buffer.from(value[BUFFER_TAG], "base64");
  if (isWireDate(value)) return new Date(value[DATE_TAG]);
  if (Array.isArray(value)) return value.map(decode);
  return Object.fromEntries(
    Object.entries(isWireObject(value) ? value[OBJECT_TAG] : value).map(
      ([key, item]) => [key, decode(item)],
    ),
  );
}
