/**
 * Manual verification harness, not part of the automated test suite.
 * Requires a real phone to scan the QR code. See examples/README.md.
 *
 * Run with: bun run examples/baileys-host.ts
 * (leave running, then start examples/baileys-consumer.ts in a second terminal)
 */
import { useMultiFileAuthState } from "baileys";
import { createBaileysAdapter } from "chat-adapter-baileys";
import qrcodeTerminal from "qrcode-terminal";

import { serveAdapter } from "../src/host";

function requireEnv(name: string): string {
  const value = process.env[name];
  if (!value)
    throw new Error(
      `${name} is required — set it before running this example.`,
    );
  return value;
}

const SECRET = requireEnv("CHAT_ADAPTER_REMOTE_SECRET");
const CONSUMER_URL =
  process.env.CONSUMER_URL ?? "http://127.0.0.1:4001/inbound";
const HOST_PORT = Number(process.env.HOST_PORT ?? 4000);

async function main() {
  const { state, saveCreds } = await useMultiFileAuthState(
    "./examples/.baileys-auth",
  );

  const baileysAdapter = createBaileysAdapter({
    adapterName: "whatsapp",
    auth: { state, saveCreds },
    onQR: (qr) => {
      console.log(
        "\nScan this QR code with WhatsApp (Linked devices > Link a device):\n",
      );
      qrcodeTerminal.generate(qr, { small: true });
    },
  });

  // chat-adapter-baileys@2.1.0 defines its own `reply(message, content)`, which collides with the
  // optional `reply?(threadId, messageId, message)` that chat core added to Adapter after 4.24.
  // Not dispatched by this bridge, so it is unreachable at runtime. Remove once upstream aligns.
  // @ts-expect-error -- upstream signature collision, see above
  const host = serveAdapter(baileysAdapter, {
    secret: SECRET,
    consumerUrl: CONSUMER_URL,
  });
  await host.ready.catch((error) => {
    console.error("Baileys adapter failed to initialize:", error);
    process.exit(1);
  });

  const server = Bun.serve({
    port: HOST_PORT,
    hostname: "127.0.0.1",
    fetch: host.fetch,
  });
  console.log(
    `Host listening on http://${server.hostname}:${server.port} — waiting for QR scan / reconnect...`,
  );

  await baileysAdapter.connect();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
