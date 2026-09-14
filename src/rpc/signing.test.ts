import { describe, expect, it } from "vitest";

import { isTimestampFresh, sign, verify } from "./signing";

describe("sign/verify", () => {
  it("verifies a correctly signed body", () => {
    const body = JSON.stringify({ hello: "world" });
    const timestamp = String(Date.now());
    const signature = sign(body, timestamp, "secret");
    expect(verify(body, timestamp, signature, "secret")).toBe(true);
  });

  it("rejects a tampered body", () => {
    const timestamp = String(Date.now());
    const signature = sign("original", timestamp, "secret");
    expect(verify("tampered", timestamp, signature, "secret")).toBe(false);
  });

  it("rejects a wrong secret", () => {
    const body = "payload";
    const timestamp = String(Date.now());
    const signature = sign(body, timestamp, "secret-a");
    expect(verify(body, timestamp, signature, "secret-b")).toBe(false);
  });

  it("does not throw on a signature of different length than expected", () => {
    const body = "payload";
    const timestamp = String(Date.now());
    expect(() => verify(body, timestamp, "short", "secret")).not.toThrow();
    expect(verify(body, timestamp, "short", "secret")).toBe(false);
  });

  it("does not throw on an empty signature", () => {
    const body = "payload";
    const timestamp = String(Date.now());
    expect(() => verify(body, timestamp, "", "secret")).not.toThrow();
    expect(verify(body, timestamp, "", "secret")).toBe(false);
  });
});

describe("isTimestampFresh", () => {
  it("accepts a timestamp within the window", () => {
    expect(isTimestampFresh(String(1000), 30_000, 1000)).toBe(true);
    expect(isTimestampFresh(String(1000), 30_000, 31_000)).toBe(true);
  });

  it("rejects a timestamp outside the window", () => {
    expect(isTimestampFresh(String(1000), 30_000, 31_001)).toBe(false);
  });

  it("rejects a non-numeric timestamp", () => {
    expect(isTimestampFresh("not-a-number", 30_000)).toBe(false);
  });
});
