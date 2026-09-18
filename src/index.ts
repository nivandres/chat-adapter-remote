export {
  RemoteAdapter,
  RemoteAdapterUnsupportedSyncMethodError,
  createRemoteAdapter,
  type RemoteOf,
} from "./adapter";
export { RemoteAdapterRpcError, RpcErrorCode } from "./rpc/errors";
export { PROTOCOL_VERSION } from "./rpc/methods";
export { createReplayGuard, type ReplayGuard } from "./rpc/security";
export type { RemoteAdapterConfig } from "./types";
