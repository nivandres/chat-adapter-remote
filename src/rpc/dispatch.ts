import { JsonRpcRequestSchema } from "./envelope";
import { RpcErrorCode, type RpcErrorObject } from "./errors";
import {
  isTimestampFresh,
  verify,
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
} from "./signing";

export interface DispatchOptions {
  secret: string;
  replayWindowMs?: number;
  maxBodyBytes?: number;
}

export type VerifyAndParseResult =
  | {
      ok: true;
      id: string | number | null | undefined;
      method: string;
      params: unknown;
    }
  | { ok: false; response: Response };

function errorResponse(
  id: string | number | null,
  status: number,
  error: RpcErrorObject,
): Response {
  return Response.json({ jsonrpc: "2.0", id, error }, { status });
}

/**
 * Shared by AdapterHost and RemoteAdapter.handleWebhook: checks body size,
 * signature, replay window, and the generic envelope shape. Stops short of
 * the method-specific allowlist (OUTBOUND_CALLS vs INBOUND_CALLS) — that's
 * the caller's job, since the two directions allow different methods.
 */
export async function verifyAndParse(
  request: Request,
  options: DispatchOptions,
): Promise<VerifyAndParseResult> {
  const maxBodyBytes = options.maxBodyBytes ?? 5_000_000;
  const replayWindowMs = options.replayWindowMs ?? 30_000;

  const contentLength = request.headers.get("content-length");
  if (contentLength && Number(contentLength) > maxBodyBytes) {
    return {
      ok: false,
      response: errorResponse(null, 413, {
        code: RpcErrorCode.INVALID_REQUEST,
        message: "Payload too large",
      }),
    };
  }

  const rawBody = await request.text();
  if (Buffer.byteLength(rawBody, "utf8") > maxBodyBytes) {
    return {
      ok: false,
      response: errorResponse(null, 413, {
        code: RpcErrorCode.INVALID_REQUEST,
        message: "Payload too large",
      }),
    };
  }

  const signature = request.headers.get(SIGNATURE_HEADER);
  const timestamp = request.headers.get(TIMESTAMP_HEADER);
  if (!signature || !timestamp) {
    return {
      ok: false,
      response: errorResponse(null, 401, {
        code: RpcErrorCode.UNAUTHORIZED,
        message: "Missing signature",
      }),
    };
  }
  if (!verify(rawBody, timestamp, signature, options.secret)) {
    return {
      ok: false,
      response: errorResponse(null, 401, {
        code: RpcErrorCode.UNAUTHORIZED,
        message: "Invalid signature",
      }),
    };
  }
  if (!isTimestampFresh(timestamp, replayWindowMs)) {
    return {
      ok: false,
      response: errorResponse(null, 401, {
        code: RpcErrorCode.REPLAY_REJECTED,
        message: "Request timestamp outside the allowed window",
      }),
    };
  }

  let json: unknown;
  try {
    json = JSON.parse(rawBody);
  } catch {
    return {
      ok: false,
      response: errorResponse(null, 400, {
        code: RpcErrorCode.PARSE_ERROR,
        message: "Invalid JSON",
      }),
    };
  }

  const envelope = JsonRpcRequestSchema.safeParse(json);
  if (!envelope.success) {
    return {
      ok: false,
      response: errorResponse(null, 400, {
        code: RpcErrorCode.INVALID_REQUEST,
        message: "Invalid JSON-RPC envelope",
      }),
    };
  }

  return {
    ok: true,
    id: envelope.data.id,
    method: envelope.data.method,
    params: envelope.data.params,
  };
}
