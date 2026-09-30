import type { Adapter, WebhookOptions } from "chat";

import { RpcErrorCode } from "../rpc/errors";
import { AdapterHost, type ServeAdapterOptions } from "./adapter-host";

export interface HostedAdapter extends ServeAdapterOptions {
  adapter: Adapter;
}

function notFound(name: string): Response {
  return Response.json(
    {
      jsonrpc: "2.0",
      id: null,
      error: {
        code: RpcErrorCode.METHOD_NOT_FOUND,
        message: `chat-adapter-remote: no adapter is served as "${name}"`,
      },
    },
    { status: 404 },
  );
}

function nameOf(request: Request): string {
  return new URL(request.url).pathname.split("/").filter(Boolean).pop() ?? "";
}

/** Each host verifies with its own secret, so one channel's secret cannot reach another. */
export class AdapterHosts {
  readonly hosts: Readonly<Record<string, AdapterHost>>;

  readonly fetch = (request: Request): Promise<Response> => {
    const host = this.hosts[nameOf(request)];
    return host
      ? host.handleRequest(request)
      : Promise.resolve(notFound(nameOf(request)));
  };

  constructor(entries: Record<string, HostedAdapter>) {
    this.hosts = Object.fromEntries(
      Object.entries(entries).map(([name, { adapter, ...options }]) => [
        name,
        new AdapterHost(adapter, { ...options, autoStart: false }),
      ]),
    );
  }

  /** Unlike Chat's `Promise.all`, one channel failing leaves the others running. */
  async start(): Promise<void> {
    await Promise.allSettled(
      Object.values(this.hosts).map((host) => host.start()),
    );
  }

  async stop(): Promise<void> {
    await Promise.allSettled(
      Object.values(this.hosts).map((host) => host.stop()),
    );
  }

  handleWebhook(
    name: string,
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    const host = this.hosts[name];
    return host
      ? host.handleWebhook(request, options)
      : Promise.resolve(notFound(name));
  }
}

export function serveAdapters(
  entries: Record<string, HostedAdapter>,
): AdapterHosts {
  const hosts = new AdapterHosts(entries);
  void hosts.start();
  return hosts;
}
