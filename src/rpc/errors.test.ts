import {
  AdapterError,
  AdapterRateLimitError,
  AuthenticationError,
  NetworkError,
  PermissionError,
  ResourceNotFoundError,
  ValidationError,
} from "@chat-adapter/shared";
import { describe, expect, it } from "vitest";

import {
  RemoteAdapterRpcError,
  RpcErrorCode,
  deserializeError,
  serializeError,
} from "./errors";

describe("error mapping", () => {
  it("round-trips every mapped adapter error class", () => {
    const cases = [
      new AdapterRateLimitError("mock", 42),
      new AuthenticationError("mock", "bad token"),
      new ResourceNotFoundError("mock", "channel", "C1"),
      new PermissionError("mock", "post", "chat:write"),
      new ValidationError("mock", "too long"),
      new NetworkError("mock", "timeout"),
      new AdapterError("boom", "mock", "CUSTOM"),
    ];

    for (const original of cases) {
      const restored = deserializeError(serializeError(original));
      expect(restored).toBeInstanceOf(original.constructor);
      expect(restored.message).toBe(original.message);
    }
  });

  it("keeps subtype fields that callers branch on", () => {
    const rate = deserializeError(
      serializeError(new AdapterRateLimitError("mock", 42)),
    ) as AdapterRateLimitError;
    expect(rate.retryAfter).toBe(42);

    const missing = deserializeError(
      serializeError(new ResourceNotFoundError("mock", "channel", "C1")),
    ) as ResourceNotFoundError;
    expect(missing.resourceType).toBe("channel");
    expect(missing.resourceId).toBe("C1");

    const denied = deserializeError(
      serializeError(new PermissionError("mock", "post", "chat:write")),
    ) as PermissionError;
    expect(denied.action).toBe("post");
    expect(denied.requiredScope).toBe("chat:write");
  });

  it("does not leak the message of an unrecognized error", () => {
    const wire = serializeError(new Error("internal hostname db-01.internal"));
    expect(wire.code).toBe(RpcErrorCode.INTERNAL_ERROR);
    expect(wire.message).not.toContain("db-01");
  });

  it("represents transport-level failures as RemoteAdapterRpcError", () => {
    expect(
      deserializeError({
        code: RpcErrorCode.METHOD_NOT_FOUND,
        message: "no such method",
      }),
    ).toBeInstanceOf(RemoteAdapterRpcError);
  });
});
