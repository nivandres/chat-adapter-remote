import { JsonRpcRequestSchema } from "./envelope";
import { RpcErrorCode, type RpcErrorObject } from "./errors";
import type { ReplayGuard } from "./security";
import {
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
  isTimestampFresh,
  verify,
} from "./signing";

export interface DispatchOptions {
  secret: string;
  /** Rejects requests whose signed timestamp is older than this. Default 30s. */
  timestampToleranceMs?: number;
  /** Rejects bodies larger than this. Unlimited by default: both ends are trusted. */
  maxBodyBytes?: number;
  /** Rejects a signature that was already accepted inside the freshness window. */
  replayGuard?: ReplayGuard;
}

export type VerifiedRequest =
  | {
      ok: true;
      id: string | number | null | undefined;
      method: string;
      params: unknown;
    }
  | { ok: false; response: Response };

function errorResponse(status: number, error: RpcErrorObject): Response {
  return Response.json({ jsonrpc: "2.0", id: null, error }, { status });
}

/** Reads at most `maxBytes`, returning null past the limit rather than buffering the rest. */
export async function readBody(
  request: Request | Response,
  maxBytes: number,
): Promise<string | null> {
  const declared = request.headers.get("content-length");
  if (declared && Number(declared) > maxBytes) return null;
  if (!request.body) {
    const text = await request.text();
    return Buffer.byteLength(text) > maxBytes ? null : text;
  }

  const reader = request.body.getReader();
  const chunks: Buffer[] = [];
  let total = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel();
      return null;
    }
    chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks).toString("utf8");
}

/**
 * Shared by both directions: body size, signature, timestamp freshness, and
 * the generic envelope. The method-specific allowlist is the caller's, since
 * each direction accepts a different set.
 */
export async function verifyRequest(
  request: Request,
  options: DispatchOptions,
): Promise<VerifiedRequest> {
  const rawBody = await readBody(
    request,
    options.maxBodyBytes ?? Number.POSITIVE_INFINITY,
  );
  if (rawBody === null) {
    return {
      ok: false,
      response: errorResponse(413, {
        code: RpcErrorCode.INVALID_REQUEST,
        message: "Payload too large",
      }),
    };
  }

  const signature = request.headers.get(SIGNATURE_HEADER);
  const timestamp = request.headers.get(TIMESTAMP_HEADER);
  if (
    !signature ||
    !timestamp ||
    !verify(rawBody, timestamp, signature, options.secret)
  ) {
    return {
      ok: false,
      response: errorResponse(401, {
        code: RpcErrorCode.UNAUTHORIZED,
        message: "Invalid signature",
      }),
    };
  }
  const toleranceMs = options.timestampToleranceMs ?? 30_000;
  if (!isTimestampFresh(timestamp, toleranceMs)) {
    return {
      ok: false,
      response: errorResponse(401, {
        code: RpcErrorCode.STALE_TIMESTAMP,
        message: "Timestamp outside tolerance",
      }),
    };
  }
  if (await options.replayGuard?.seen(signature, toleranceMs)) {
    return {
      ok: false,
      response: errorResponse(401, {
        code: RpcErrorCode.REPLAYED,
        message: "Signature already used",
      }),
    };
  }

  let json: unknown;
  try {
    json = JSON.parse(rawBody);
  } catch {
    return {
      ok: false,
      response: errorResponse(400, {
        code: RpcErrorCode.PARSE_ERROR,
        message: "Invalid JSON",
      }),
    };
  }

  const envelope = JsonRpcRequestSchema.safeParse(json);
  if (!envelope.success) {
    return {
      ok: false,
      response: errorResponse(400, {
        code: RpcErrorCode.INVALID_REQUEST,
        message: "Invalid JSON-RPC envelope",
      }),
    };
  }
  const { id, method, params } = envelope.data;
  return { ok: true, id, method, params };
}
