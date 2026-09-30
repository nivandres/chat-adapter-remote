import {
  AdapterError,
  AdapterRateLimitError,
  AuthenticationError,
  NetworkError,
  PermissionError,
  ResourceNotFoundError,
  ValidationError,
} from "@chat-adapter/shared";

export const RpcErrorCode = {
  PARSE_ERROR: -32700,
  INVALID_REQUEST: -32600,
  METHOD_NOT_FOUND: -32601,
  INVALID_PARAMS: -32602,
  INTERNAL_ERROR: -32603,
  UNAUTHORIZED: -32000,
  STALE_TIMESTAMP: -32001,
  METHOD_NOT_IMPLEMENTED: -32002,
  REPLAYED: -32003,
  STREAM_NOT_FOUND: -32004,
  NOT_CANCELLABLE: -32005,
  STATE_UNAVAILABLE: -32006,
  UNAVAILABLE: -32007,
  TIMEOUT: -32008,
  STREAM_DISCARDED: -32009,
  ADAPTER_ERROR: -32010,
  ADAPTER_RATE_LIMITED: -32011,
  ADAPTER_AUTH_FAILED: -32012,
  ADAPTER_NOT_FOUND: -32013,
  ADAPTER_PERMISSION_DENIED: -32014,
  ADAPTER_NETWORK_ERROR: -32015,
} as const;

export type RpcErrorCode = (typeof RpcErrorCode)[keyof typeof RpcErrorCode];

export interface RpcErrorObject {
  code: number;
  message: string;
  data?: unknown;
}

export class RemoteAdapterRpcError extends Error {
  constructor(
    public readonly code: number,
    message: string,
    public readonly data?: unknown,
  ) {
    super(message);
    this.name = "RemoteAdapterRpcError";
  }
}

/** Deliberately not posted: nothing should retry or repost it. */
export class StreamDiscardedError extends RemoteAdapterRpcError {
  constructor(message: string) {
    super(RpcErrorCode.STREAM_DISCARDED, message);
    this.name = "StreamDiscardedError";
  }
}

/** Never handled, so safe to send again. A timeout may have been. */
export function isUndelivered(error: unknown): boolean {
  return (
    error instanceof RemoteAdapterRpcError &&
    error.code === RpcErrorCode.UNAVAILABLE
  );
}

export function isTransient(error: unknown): boolean {
  return (
    error instanceof RemoteAdapterRpcError &&
    (error.code === RpcErrorCode.UNAVAILABLE ||
      error.code === RpcErrorCode.TIMEOUT)
  );
}

/** Unrecognised errors collapse to a generic message, so host internals never leave. */
export function serializeError(error: unknown): RpcErrorObject {
  if (error instanceof AdapterRateLimitError) {
    return {
      code: RpcErrorCode.ADAPTER_RATE_LIMITED,
      message: error.message,
      data: {
        errorClass: "AdapterRateLimitError",
        adapter: error.adapter,
        retryAfter: error.retryAfter,
      },
    };
  }
  if (error instanceof AuthenticationError) {
    return {
      code: RpcErrorCode.ADAPTER_AUTH_FAILED,
      message: error.message,
      data: { errorClass: "AuthenticationError", adapter: error.adapter },
    };
  }
  if (error instanceof ResourceNotFoundError) {
    return {
      code: RpcErrorCode.ADAPTER_NOT_FOUND,
      message: error.message,
      data: {
        errorClass: "ResourceNotFoundError",
        adapter: error.adapter,
        resourceType: error.resourceType,
        resourceId: error.resourceId,
      },
    };
  }
  if (error instanceof PermissionError) {
    return {
      code: RpcErrorCode.ADAPTER_PERMISSION_DENIED,
      message: error.message,
      data: {
        errorClass: "PermissionError",
        adapter: error.adapter,
        action: error.action,
        requiredScope: error.requiredScope,
      },
    };
  }
  if (error instanceof ValidationError) {
    return {
      code: RpcErrorCode.INVALID_PARAMS,
      message: error.message,
      data: { errorClass: "ValidationError", adapter: error.adapter },
    };
  }
  if (error instanceof NetworkError) {
    return {
      code: RpcErrorCode.ADAPTER_NETWORK_ERROR,
      message: error.message,
      data: { errorClass: "NetworkError", adapter: error.adapter },
    };
  }
  if (error instanceof AdapterError) {
    return {
      code: RpcErrorCode.ADAPTER_ERROR,
      message: error.message,
      data: {
        errorClass: "AdapterError",
        adapter: error.adapter,
        originalCode: error.code,
      },
    };
  }
  if (error instanceof RemoteAdapterRpcError) {
    return { code: error.code, message: error.message, data: error.data };
  }
  return {
    code: RpcErrorCode.INTERNAL_ERROR,
    message: "Internal adapter error",
  };
}

export function deserializeError(error: RpcErrorObject): Error {
  const data = (error.data ?? {}) as Record<string, unknown>;
  const errorClass =
    typeof data.errorClass === "string" ? data.errorClass : undefined;
  const adapter = typeof data.adapter === "string" ? data.adapter : "remote";

  switch (errorClass) {
    case "AdapterRateLimitError":
      return new AdapterRateLimitError(
        adapter,
        data.retryAfter as number | undefined,
      );
    case "AuthenticationError":
      return new AuthenticationError(adapter, error.message);
    case "ResourceNotFoundError":
      return new ResourceNotFoundError(
        adapter,
        data.resourceType as string,
        data.resourceId as string | undefined,
      );
    case "PermissionError":
      return new PermissionError(
        adapter,
        data.action as string,
        data.requiredScope as string | undefined,
      );
    case "ValidationError":
      return new ValidationError(adapter, error.message);
    case "NetworkError":
      return new NetworkError(adapter, error.message);
    case "AdapterError":
      return new AdapterError(
        error.message,
        adapter,
        data.originalCode as string | undefined,
      );
    default:
      if (error.code === RpcErrorCode.STREAM_DISCARDED) {
        return new StreamDiscardedError(error.message);
      }
      return new RemoteAdapterRpcError(error.code, error.message, error.data);
  }
}
