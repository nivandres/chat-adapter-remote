import crypto from "node:crypto";

export const SIGNATURE_HEADER = "x-chat-adapter-remote-signature";
export const TIMESTAMP_HEADER = "x-chat-adapter-remote-timestamp";
export const NONCE_HEADER = "x-chat-adapter-remote-nonce";

/** The nonce keeps identical calls from separate instances, signed in the same millisecond, from reading as replays. */
export function sign(
  rawBody: string,
  timestamp: string,
  nonce: string,
  secret: string,
): string {
  const message = `${timestamp}.${nonce}.${rawBody}`;
  return (
    "sha256=" +
    crypto.createHmac("sha256", secret).update(message).digest("hex")
  );
}

/** `timingSafeEqual` throws on unequal lengths. */
export function verify(
  rawBody: string,
  timestamp: string,
  nonce: string,
  receivedSignature: string,
  secret: string,
): boolean {
  const expected = sign(rawBody, timestamp, nonce, secret);
  const expectedBuffer = Buffer.from(expected);
  const receivedBuffer = Buffer.from(receivedSignature);
  if (expectedBuffer.length !== receivedBuffer.length) return false;
  return crypto.timingSafeEqual(expectedBuffer, receivedBuffer);
}

export function isTimestampFresh(
  timestamp: string,
  windowMs: number,
  now = Date.now(),
): boolean {
  const parsed = Number(timestamp);
  if (!Number.isFinite(parsed)) return false;
  return Math.abs(now - parsed) <= windowMs;
}
