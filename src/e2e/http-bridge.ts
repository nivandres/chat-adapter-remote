import http from "node:http";
import type { AddressInfo } from "node:net";

async function toRequest(
  req: http.IncomingMessage,
  baseUrl: string,
): Promise<Request> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (typeof value === "string") headers.set(key, value);
  }
  const method = req.method ?? "GET";
  return new Request(new URL(req.url ?? "/", baseUrl), {
    method,
    headers,
    body:
      method === "GET" || method === "HEAD" ? undefined : Buffer.concat(chunks),
  });
}

export interface RealHttpServer {
  url: string;
  close: () => Promise<void>;
}

/** Starts a real Node server on an ephemeral localhost port, bridged to a Fetch API handler. */
export function startRealServer(
  handler: (request: Request) => Promise<Response>,
): Promise<RealHttpServer> {
  return new Promise((resolve) => {
    let baseUrl = "";
    const server = http.createServer((req, res) => {
      toRequest(req, baseUrl)
        .then(handler)
        .then(async (response) => {
          const body = Buffer.from(await response.arrayBuffer());
          const headers: Record<string, string> = {};
          response.headers.forEach((value, key) => (headers[key] = value));
          res.writeHead(response.status, headers);
          res.end(body);
        })
        .catch((error) => {
          res.writeHead(500);
          res.end(String(error));
        });
    });
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as AddressInfo;
      baseUrl = `http://127.0.0.1:${port}`;
      resolve({
        url: baseUrl,
        close: () => new Promise((done) => server.close(() => done())),
      });
    });
  });
}
