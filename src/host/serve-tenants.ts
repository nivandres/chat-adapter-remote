import { AdapterHosts, type HostedAdapter } from "./serve-adapters";

type MaybePromise<T> = T | Promise<T>;

/** Where tenants come from: the library never sees the records, only what `load` builds from one. */
export interface TenantSource {
  /** The tenants to serve on start. */
  list(): MaybePromise<string[]>;
  /** A tenant's adapter and options, or undefined when it should not be served. */
  load(id: string): MaybePromise<HostedAdapter | undefined>;
  /** A tenant that could not be served on start, or `list` itself failing; the others carry on. */
  onError?(error: unknown, id?: string): void;
}

/** Tenants are only ever loaded on request of your own code, never from an unknown route. */
export class AdapterTenants extends AdapterHosts {
  constructor(private readonly source: TenantSource) {
    super();
  }

  private starting?: Promise<void>;

  /** Once: awaiting it again only waits for the first start. */
  override start(): Promise<void> {
    this.starting ??= this.serveListed();
    return this.starting;
  }

  private async serveListed(): Promise<void> {
    const ids = await this.source.list();
    await Promise.all(
      ids.map((id) =>
        this.load(id).catch((error: unknown) =>
          this.source.onError?.(error, id),
        ),
      ),
    );
  }

  /** Serves a tenant from its record, replacing a running one; a missing record removes it. */
  async load(id: string): Promise<void> {
    const entry = await this.source.load(id);
    if (entry) await this.add(id, entry);
    else await this.remove(id);
  }
}

export function serveTenants(source: TenantSource): AdapterTenants {
  const tenants = new AdapterTenants(source);
  void tenants.start().catch((error: unknown) => source.onError?.(error));
  return tenants;
}
