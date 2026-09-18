export {
  RemoteAdapter,
  RemoteAdapterUnsupportedSyncMethodError,
  createRemoteAdapter,
} from "./adapter";
export { RemoteAdapterRpcError, RpcErrorCode } from "./rpc/errors";
export { PROTOCOL_VERSION } from "./rpc/methods";
export type { RemoteAdapterConfig } from "./types";
