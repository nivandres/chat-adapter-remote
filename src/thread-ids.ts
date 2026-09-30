const CONTENT_KEYS = new Set(["raw", "formatted"]);
const ID_KEYS = new Set(["threadId", "channelId", "id"]);

function swap(value: string, from: string, to: string): string {
  return value.startsWith(from) ? to + value.slice(from.length) : value;
}

function walk(value: unknown, from: string, to: string): unknown {
  if (Array.isArray(value)) return value.map((item) => walk(item, from, to));
  if (
    value === null ||
    typeof value !== "object" ||
    Buffer.isBuffer(value) ||
    value instanceof Date
  ) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      key,
      CONTENT_KEYS.has(key)
        ? item
        : ID_KEYS.has(key) && typeof item === "string"
          ? swap(item, from, to)
          : walk(item, from, to),
    ]),
  );
}

/** Chat routes by the id prefix, so the consumer can register under any key. */
export function translateIds(
  value: unknown,
  from: string,
  to: string,
): unknown {
  return from === to ? value : walk(value, `${from}:`, `${to}:`);
}

export function translateParams(
  params: unknown[],
  from: string,
  to: string,
): unknown[] {
  if (from === to) return params;
  const [first, ...rest] = params;
  return [
    typeof first === "string"
      ? swap(first, `${from}:`, `${to}:`)
      : walk(first, `${from}:`, `${to}:`),
    ...rest.map((item) => walk(item, `${from}:`, `${to}:`)),
  ];
}
