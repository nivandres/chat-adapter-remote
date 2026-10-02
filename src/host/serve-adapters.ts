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

function hostFor({ adapter, ...options }: HostedAdapter): AdapterHost {
  return new AdapterHost(adapter, { ...options, autoStart: false });
}

function nameOf(request: Request): string {
  return new URL(request.url).pathname.split("/").filter(Boolean).pop() ?? "";
}

/** Each host verifies with its own secret, so one channel's secret cannot reach another. */
export class AdapterHosts {
  private readonly entries = new Map<string, AdapterHost>();
  private readonly pending = new Map<string, Promise<unknown>>();

  readonly fetch = (request: Request): Promise<Response> => {
    const host = this.get(nameOf(request));
    return host
      ? host.handleRequest(request)
      : Promise.resolve(notFound(nameOf(request)));
  };

  constructor(entries: Record<string, HostedAdapter> = {}) {
    for (const [name, entry] of Object.entries(entries)) {
      this.entries.set(name, hostFor(entry));
    }
  }

  get hosts(): Readonly<Record<string, AdapterHost>> {
    return Object.fromEntries(this.entries);
  }

  get(name: string): AdapterHost | undefined {
    return this.entries.get(name);
  }

  /** Serves `name`, replacing and stopping whatever served it before. */
  add(name: string, entry: HostedAdapter): Promise<AdapterHost> {
    return this.serialize(name, async () => {
      await this.stopHost(name);
      const host = hostFor(entry);
      this.entries.set(name, host);
      try {
        await host.start();
      } catch (error) {
        await this.stopHost(name);
        throw error;
      }
      return host;
    });
  }

  remove(name: string): Promise<void> {
    return this.serialize(name, () => this.stopHost(name));
  }

  /** Unlike Chat's `Promise.all`, one channel failing leaves the others running. */
  async start(): Promise<void> {
    await Promise.allSettled(
      [...this.entries.values()].map((host) => host.start()),
    );
  }

  async stop(): Promise<void> {
    await Promise.allSettled(
      [...this.entries.keys()].map((name) => this.remove(name)),
    );
  }

  handleWebhook(
    name: string,
    request: Request,
    options?: WebhookOptions,
  ): Promise<Response> {
    const host = this.get(name);
    return host
      ? host.handleWebhook(request, options)
      : Promise.resolve(notFound(name));
  }

  private async stopHost(name: string): Promise<void> {
    const host = this.entries.get(name);
    if (!host) return;
    this.entries.delete(name);
    await host.stop();
  }

  /** One change per name at a time, so two concurrent adds cannot leave an orphaned host running. */
  private serialize<T>(name: string, change: () => Promise<T>): Promise<T> {
    const run = (this.pending.get(name) ?? Promise.resolve())
      .catch(() => undefined)
      .then(change);
    this.pending.set(name, run);
    void run
      .catch(() => undefined)
      .finally(() => {
        if (this.pending.get(name) === run) this.pending.delete(name);
      });
    return run;
  }
}

export function serveAdapters(
  entries: Record<string, HostedAdapter>,
): AdapterHosts {
  const hosts = new AdapterHosts(entries);
  void hosts.start();
  return hosts;
}
