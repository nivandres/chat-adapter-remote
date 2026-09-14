import http from "node:http";
import type { AddressInfo } from "node:net";

async function toFetchRequest(
  req: http.IncomingMessage,
  baseUrl: string,
): Promise<Request> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    if (typeof value === "string") headers.set(key, value);
  }
  const hasBody = !["GET", "HEAD"].includes(req.method ?? "GET");
  return new Request(new URL(req.url ?? "/", baseUrl), {
    method: req.method,
    headers,
    body: hasBody && chunks.length ? Buffer.concat(chunks) : undefined,
  });
}

async function writeFetchResponse(
  response: Response,
  res: http.ServerResponse,
): Promise<void> {
  const body = Buffer.from(await response.arrayBuffer());
  const headers: Record<string, string> = {};
  response.headers.forEach((value, key) => {
    headers[key] = value;
  });
  res.writeHead(response.status, headers);
  res.end(body);
}

export interface RealHttpServer {
  url: string;
  close: () => Promise<void>;
}

/** Starts a real Node `http` server on an ephemeral localhost port, bridged to a Fetch API `(request: Request) => Promise<Response>` handler. */
export function startRealServer(
  handler: (request: Request) => Promise<Response>,
): Promise<RealHttpServer> {
  return new Promise((resolve) => {
    let baseUrl = "";
    const server = http.createServer((req, res) => {
      toFetchRequest(req, baseUrl)
        .then(handler)
        .then((response) => writeFetchResponse(response, res))
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
        close: () => new Promise((res) => server.close(() => res())),
      });
    });
  });
}
