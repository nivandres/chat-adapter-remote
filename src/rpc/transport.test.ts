import { describe, expect, it, vi } from "vitest";

import { createRpcClient } from "./transport";
import type { FetchLike } from "../types";

const options = { url: "https://host.test/rpc", secret: "s" };

function respond(body: unknown): FetchLike {
  return async () => Response.json(body);
}

describe("rpc client", () => {
  it("returns the decoded result", async () => {
    const client = createRpcClient({
      ...options,
      fetch: async (_url, init) =>
        Response.json({
          jsonrpc: "2.0",
          id: JSON.parse(String(init?.body)).id,
          result: { ok: true },
        }),
    });
    await expect(client.request("postMessage", [])).resolves.toEqual({
      ok: true,
    });
  });

  it("rejects a response whose id does not match the request", async () => {
    const client = createRpcClient({
      ...options,
      fetch: respond({ jsonrpc: "2.0", id: 999, result: null }),
    });
    await expect(client.request("postMessage", [])).rejects.toThrow(
      /response id/,
    );
  });

  it("rejects a non-JSON response", async () => {
    const client = createRpcClient({
      ...options,
      fetch: async () => new Response("<html>502</html>", { status: 502 }),
    });
    await expect(client.request("postMessage", [])).rejects.toThrow(/non-JSON/);
  });

  it("numbers requests per client rather than globally", async () => {
    const ids: unknown[] = [];
    const capture: FetchLike = async (_url, init) => {
      const { id } = JSON.parse(String(init?.body));
      ids.push(id);
      return Response.json({ jsonrpc: "2.0", id, result: null });
    };

    await createRpcClient({ ...options, fetch: capture }).request("a", []);
    await createRpcClient({ ...options, fetch: capture }).request("b", []);

    expect(ids).toEqual([1, 1]);
  });

  it("never throws from notify, even when the transport fails", async () => {
    const client = createRpcClient({
      ...options,
      fetch: vi.fn().mockRejectedValue(new Error("down")),
    });
    expect(() => client.notify("log", [])).not.toThrow();
    await new Promise((resolve) => setTimeout(resolve, 5));
  });
});
