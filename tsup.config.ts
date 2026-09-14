import { defineConfig } from "tsup";

export default defineConfig({
  entry: { index: "src/index.ts", host: "src/host.ts" },
  format: ["esm"],
  dts: true,
  clean: true,
  sourcemap: true,
});
