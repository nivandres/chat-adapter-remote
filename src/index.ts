export {
  RemoteAdapter,
  RemoteAdapterUnsupportedSyncMethodError,
  createRemoteAdapter,
  type RemoteOf,
} from "./adapter";
export {
  RemoteAdapterRpcError,
  RpcErrorCode,
  StreamDiscardedError,
} from "./rpc/errors";
export { PROTOCOL_VERSION } from "./rpc/methods";
export { createReplayGuard, type ReplayGuard } from "./rpc/security";
export type {
  HostEvent,
  HostStateAccess,
  RemoteAdapterConfig,
  RequestEvent,
  RequestHandler,
  RetryOptions,
} from "./types";
