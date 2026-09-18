import { defineConfig } from "tsup";

export default defineConfig({
  entry: { index: "src/index.ts", host: "src/host.ts" },
  format: ["esm"],
  target: "node20",
  platform: "node",
  // tsup strips the `node:` prefix by default, which makes bundlers reach for
  // a browser crypto polyfill.
  removeNodeProtocol: false,
  dts: true,
  clean: true,
});
