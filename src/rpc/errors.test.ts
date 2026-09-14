import {
  AdapterRateLimitError,
  ResourceNotFoundError,
  ValidationError,
} from "@chat-adapter/shared";
import { describe, expect, it } from "vitest";

import {
  deserializeError,
  RemoteAdapterRpcError,
  RpcErrorCode,
  serializeError,
} from "./errors";

describe("serializeError/deserializeError", () => {
  it("round-trips AdapterRateLimitError with its retryAfter", () => {
    const original = new AdapterRateLimitError("mock", 42);
    const wire = serializeError(original);
    expect(wire.code).toBe(RpcErrorCode.ADAPTER_RATE_LIMITED);
    const restored = deserializeError(wire);
    expect(restored).toBeInstanceOf(AdapterRateLimitError);
    expect((restored as AdapterRateLimitError).retryAfter).toBe(42);
  });

  it("round-trips ResourceNotFoundError with resourceType/resourceId", () => {
    const original = new ResourceNotFoundError("mock", "channel", "C123");
    const restored = deserializeError(serializeError(original));
    expect(restored).toBeInstanceOf(ResourceNotFoundError);
    expect((restored as ResourceNotFoundError).resourceType).toBe("channel");
    expect((restored as ResourceNotFoundError).resourceId).toBe("C123");
  });

  it("maps ValidationError to INVALID_PARAMS", () => {
    const wire = serializeError(new ValidationError("mock", "bad input"));
    expect(wire.code).toBe(RpcErrorCode.INVALID_PARAMS);
    expect(deserializeError(wire)).toBeInstanceOf(ValidationError);
  });

  it("never leaks the original message for an unrecognized error", () => {
    const wire = serializeError(new Error("super secret internal detail"));
    expect(wire.code).toBe(RpcErrorCode.INTERNAL_ERROR);
    expect(wire.message).not.toContain("secret");
  });

  it("reconstructs an unrecognized wire error as RemoteAdapterRpcError", () => {
    const restored = deserializeError({
      code: RpcErrorCode.METHOD_NOT_FOUND,
      message: "no such method",
    });
    expect(restored).toBeInstanceOf(RemoteAdapterRpcError);
    expect((restored as RemoteAdapterRpcError).code).toBe(
      RpcErrorCode.METHOD_NOT_FOUND,
    );
  });
});
