import { JsonRpcRequestSchema } from "./envelope";
import { RpcErrorCode, type RpcErrorObject } from "./errors";
import { unseal } from "./sealing";
import type { ReplayGuard } from "./security";
import {
  NONCE_HEADER,
  SIGNATURE_HEADER,
  TIMESTAMP_HEADER,
  secretsOf,
  type Secret,
  isTimestampFresh,
  verify,
} from "./signing";

const DEFAULT_MAX_BODY_BYTES = 32 * 1024 * 1024;

export interface DispatchOptions {
  /** Shared HMAC secret, identical on both sides. While rotating, a list: the first signs, any verifies. */
  secret: Secret;
  /** Rejects requests whose signed timestamp is older than this. Default 30s. */
  timestampToleranceMs?: number;
  /** Rejects bodies larger than this, read before the signature can be checked. Default 32 MiB. */
  maxBodyBytes?: number;
  /** Rejects a signature that was already accepted inside the freshness window. */
  replayGuard?: ReplayGuard;
  /** Expect sealed bodies, and seal the answers; safe over plain HTTP. Both sides must agree. */
  encrypt?: boolean;
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
  const unauthorized = (): VerifiedRequest => ({
    ok: false,
    response: errorResponse(401, {
      code: RpcErrorCode.UNAUTHORIZED,
      message: "Invalid signature",
    }),
  });
  const signature = request.headers.get(SIGNATURE_HEADER);
  const timestamp = request.headers.get(TIMESTAMP_HEADER);
  const nonce = request.headers.get(NONCE_HEADER);
  // Checked before the body is read, so an unsigned request costs nothing.
  if (!signature || !timestamp || !nonce) return unauthorized();
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
  const rawBody = await readBody(
    request,
    options.maxBodyBytes ?? DEFAULT_MAX_BODY_BYTES,
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
  if (
    !secretsOf(options.secret).some((secret) =>
      verify(rawBody, timestamp, nonce, signature, secret),
    )
  ) {
    return unauthorized();
  }
  // A timestamp may run up to the tolerance ahead, so it stays fresh for twice as long.
  if (await options.replayGuard?.seen(signature, 2 * toleranceMs)) {
    return {
      ok: false,
      response: errorResponse(401, {
        code: RpcErrorCode.REPLAYED,
        message: "Signature already used",
      }),
    };
  }

  const body = options.encrypt
    ? unseal(rawBody, options.secret, `request.${nonce}`)
    : rawBody;
  if (body === null) {
    return {
      ok: false,
      response: errorResponse(400, {
        code: RpcErrorCode.INVALID_REQUEST,
        message: "Expected an encrypted body; set encrypt on both sides",
      }),
    };
  }

  let json: unknown;
  try {
    json = JSON.parse(body);
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
        message:
          json && typeof json === "object" && "sealed" in json
            ? "Got an encrypted body; set encrypt on both sides"
            : "Invalid JSON-RPC envelope",
      }),
    };
  }
  const { id, method, params } = envelope.data;
  return { ok: true, id, method, params };
}
