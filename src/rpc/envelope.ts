import { z } from "zod";

const JsonRpcId = z.union([z.string(), z.number(), z.null()]);

/** Generic envelope shape, validated before the method-specific schema in methods.ts. */
export const JsonRpcRequestSchema = z.strictObject({
  jsonrpc: z.literal("2.0"),
  id: JsonRpcId.optional(),
  method: z.string(),
  params: z.unknown(),
});
export type JsonRpcRequest = z.infer<typeof JsonRpcRequestSchema>;

export const JsonRpcErrorObjectSchema = z.strictObject({
  code: z.number(),
  message: z.string(),
  data: z.unknown().optional(),
});

export const JsonRpcSuccessResponseSchema = z.strictObject({
  jsonrpc: z.literal("2.0"),
  id: JsonRpcId,
  result: z.unknown(),
});

export const JsonRpcErrorResponseSchema = z.strictObject({
  jsonrpc: z.literal("2.0"),
  id: JsonRpcId,
  error: JsonRpcErrorObjectSchema,
});

export const JsonRpcResponseSchema = z.union([
  JsonRpcSuccessResponseSchema,
  JsonRpcErrorResponseSchema,
]);
export type JsonRpcResponse = z.infer<typeof JsonRpcResponseSchema>;

export function isErrorResponse(
  response: JsonRpcResponse,
): response is z.infer<typeof JsonRpcErrorResponseSchema> {
  return "error" in response;
}
