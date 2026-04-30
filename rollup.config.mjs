import typescript from "@rollup/plugin-typescript";

/** @type {import('rollup').RollupOptions} */
const options = {
  input: "src/index.ts",
  external: [
    "@utiliread/http",
    "@utiliread/json",
    "@microsoft/fetch-event-source",
  ],
  plugins: [typescript()],
};

export default [
  Object.assign(
    {
      output: {
        file: "dist/index.mjs",
        sourcemap: true,
        format: "es",
      },
    },
    options,
  ),
  Object.assign(
    {
      output: {
        file: "dist/index.js",
        sourcemap: true,
        format: "cjs",
      },
    },
    options,
  ),
];
