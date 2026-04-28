const esbuild = require("esbuild");

const watch = process.argv.includes("--watch");
const production = process.argv.includes("--production");

const options = {
  entryPoints: ["src/extension.ts"],
  bundle: true,
  outfile: "dist/extension.js",
  platform: "node",
  target: "node18",
  format: "cjs",
  external: ["vscode"],
  minify: production,
  sourcemap: !production,
  logLevel: "info",
};

if (watch) {
  esbuild.context(options).then((ctx) => ctx.watch());
} else {
  esbuild.build(options).catch(() => process.exit(1));
}
