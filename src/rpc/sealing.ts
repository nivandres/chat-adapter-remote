import crypto from "node:crypto";

import { secretsOf, type Secret } from "./signing";

const IV_BYTES = 12;
const TAG_BYTES = 16;
const keys = new Map<string, Buffer>();

/** AES-256-GCM under a key derived from the shared secret, so `encrypt` needs nothing new to configure. */
function keyOf(secret: string): Buffer {
  let key = keys.get(secret);
  if (!key) {
    key = Buffer.from(
      crypto.hkdfSync(
        "sha256",
        secret,
        "chat-adapter-remote",
        "sealed body",
        32,
      ),
    );
    keys.set(secret, key);
  }
  return key;
}

/** `context` is authenticated but not sent: a response sealed for one request cannot pass for another's. */
export function seal(
  plaintext: string,
  secret: Secret,
  context: string,
): string {
  const iv = crypto.randomBytes(IV_BYTES);
  const cipher = crypto.createCipheriv(
    "aes-256-gcm",
    keyOf(secretsOf(secret)[0]!),
    iv,
  );
  cipher.setAAD(Buffer.from(context));
  const data = Buffer.concat([
    cipher.update(plaintext, "utf8"),
    cipher.final(),
  ]);
  return JSON.stringify({
    sealed: Buffer.concat([iv, cipher.getAuthTag(), data]).toString("base64"),
  });
}

/** Null for anything not sealed under one of the secrets and this context, tampered bodies included. */
export function unseal(
  body: string,
  secret: Secret,
  context: string,
): string | null {
  let sealed: unknown;
  try {
    sealed = (JSON.parse(body) as { sealed?: unknown })?.sealed;
  } catch {
    return null;
  }
  if (typeof sealed !== "string") return null;
  const raw = Buffer.from(sealed, "base64");
  if (raw.length < IV_BYTES + TAG_BYTES) return null;
  for (const candidate of secretsOf(secret)) {
    try {
      const decipher = crypto.createDecipheriv(
        "aes-256-gcm",
        keyOf(candidate),
        raw.subarray(0, IV_BYTES),
      );
      decipher.setAAD(Buffer.from(context));
      decipher.setAuthTag(raw.subarray(IV_BYTES, IV_BYTES + TAG_BYTES));
      return Buffer.concat([
        decipher.update(raw.subarray(IV_BYTES + TAG_BYTES)),
        decipher.final(),
      ]).toString("utf8");
    } catch {
      // Sealed under another secret of the list, or tampered with.
    }
  }
  return null;
}

/** Seals the answer to a verified request; a body-less answer has nothing to hide. */
export async function sealResponse(
  response: Response,
  secret: Secret,
  nonce: string | null,
): Promise<Response> {
  if (!nonce || response.status === 204) return response;
  const sealed = seal(await response.text(), secret, `response.${nonce}`);
  const headers = new Headers(response.headers);
  headers.set("content-type", "application/json");
  headers.delete("content-length");
  return new Response(sealed, { status: response.status, headers });
}
