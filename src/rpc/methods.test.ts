import { describe, expect, it } from "vitest";

import { INBOUND_CALLS, OUTBOUND_CALLS } from "./methods";

describe("OUTBOUND_CALLS allowlist", () => {
  it("accepts a well-formed postMessage call", () => {
    const result = OUTBOUND_CALLS.safeParse({
      method: "postMessage",
      id: 1,
      params: ["mock:1", "hello"],
    });
    expect(result.success).toBe(true);
  });

  it("rejects a method not in the allowlist", () => {
    const result = OUTBOUND_CALLS.safeParse({
      method: "__proto__",
      id: 1,
      params: [],
    });
    expect(result.success).toBe(false);
  });

  it("rejects a method that exists on Object.prototype but isn't allowlisted", () => {
    const result = OUTBOUND_CALLS.safeParse({
      method: "constructor",
      id: 1,
      params: [],
    });
    expect(result.success).toBe(false);
  });

  it("rejects wrong-arity params", () => {
    const result = OUTBOUND_CALLS.safeParse({
      method: "deleteMessage",
      id: 1,
      params: ["mock:1"],
    });
    expect(result.success).toBe(false);
  });

  it("rejects extra top-level keys (strict object)", () => {
    const result = OUTBOUND_CALLS.safeParse({
      method: "disconnect",
      id: 1,
      params: [],
      extra: "nope",
    });
    expect(result.success).toBe(false);
  });

  it("rejects wrong-typed params", () => {
    const result = OUTBOUND_CALLS.safeParse({
      method: "postMessage",
      id: 1,
      params: [123, "hello"],
    });
    expect(result.success).toBe(false);
  });
});

describe("INBOUND_CALLS allowlist", () => {
  it("accepts a well-formed log notification with no id", () => {
    const result = INBOUND_CALLS.safeParse({
      method: "log",
      params: ["info", "prefix", "message", []],
    });
    expect(result.success).toBe(true);
  });

  it("rejects a method not in the inbound allowlist", () => {
    const result = INBOUND_CALLS.safeParse({
      method: "processAction",
      id: 1,
      params: [],
    });
    expect(result.success).toBe(false);
  });
});
