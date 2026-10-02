import { afterEach, expect, test } from "bun:test";
import { parseArgs } from "../src/config.ts";

const keys = [
  "TESTS",
  "COLD_SAMPLES",
  "COLD_IDLE_MS",
  "COLD_WARM_REQUESTS",
] as const;
const saved = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
const argv = [...process.argv];
afterEach(() => {
  process.argv = [...argv];
  for (const key of keys) {
    if (saved[key] === undefined) delete process.env[key];
    else process.env[key] = saved[key];
  }
});

test("cold-start accepts explicit zero idle and env overrides flags", () => {
  process.argv = [
    ...argv.slice(0, 2),
    "--tests=cold-start",
    "--cold-samples=3",
    "--cold-idle-ms=0",
  ];
  for (const key of keys) delete process.env[key];
  process.env.COLD_SAMPLES = "7";
  expect(parseArgs()).toMatchObject({
    tests: ["cold-start"],
    coldSamples: 7,
    coldIdleMs: 0,
    coldWarmRequests: 5,
  });
});

test.each([
  ["COLD_SAMPLES", "0"],
  ["COLD_SAMPLES", "1.5"],
  ["COLD_IDLE_MS", "-1"],
  ["COLD_IDLE_MS", "3600001"],
  ["COLD_WARM_REQUESTS", "0"],
  ["COLD_WARM_REQUESTS", "NaN"],
] as const)("rejects invalid %s=%s", (key, value) => {
  process.env[key] = value;
  expect(parseArgs).toThrow();
});
