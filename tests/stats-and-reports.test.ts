import { describe, expect, spyOn, test } from "bun:test";
import { summarize, txRates } from "../src/stats.ts";
import { summarizeRuns, type RunRecord, type ScenarioResult } from "../src/results.ts";
import { runScenario } from "../src/runner.ts";
import { printBests } from "../src/reporters/console.ts";
import { toCsv } from "../src/reporters/csv.ts";
import { runOptions } from "./helpers.ts";

async function run(success: boolean): Promise<RunRecord> {
  return { run: 1, ...await runScenario(runOptions(success ? {} : { exec: async () => { throw new Error("HTTP 503 unavailable"); } })) };
}

function scenario(backend: string, runs: RunRecord[]): ScenarioResult {
  return {
    mode: "edge", backend, engine: "d1", transactionMode: "none", test: "point-read", workload: "kv",
    scanLimit: null, tenantMode: "single", tenantCount: 1, concurrency: 1, loadModel: "constant",
    durationSec: 0, iterations: 4, timeoutMs: 1000, runs, summary: summarizeRuns(runs),
  };
}

describe("latency and reports", () => {
  test("nearest-rank percentiles preserve input and handle singleton/empty samples", () => {
    const input = [100, 2, 4, 1, 3];
    expect(summarize(input, 1000)).toMatchObject({ n: 5, min: 1, p50: 3, p90: 100, p95: 100, p99: 100, max: 100, mean: 22, ops: 5 });
    expect(input).toEqual([100, 2, 4, 1, 3]);
    expect(summarize([7], 1000)).toMatchObject({ n: 1, p50: 7, p99: 7, stdev: 0 });
    expect(summarize([], 1000)).toMatchObject({ n: 0, ops: 0 });
    expect(summarize([7], 0).ops).toBe(0);
  });

  test("failed runs do not create zero latency or zero replica-rate samples", async () => {
    const good = { ...await run(true), replicaRate: 1 };
    const bad = await run(false);
    const summary = summarizeRuns([bad, good, bad]);
    expect(summary).toMatchObject({ medianP50: 20, medianP95: 20, medianDbP50: 10, medianErrorRate: 1, medianReplicaRate: 1 });
    expect(summarizeRuns([bad])).toMatchObject({ medianP50: null, medianP95: null, medianP99: null, medianDbP50: null, medianErrorRate: 1 });
  });

  test("an all-failed backend cannot win latency and CSV leaves missing latency blank", async () => {
    const bad = scenario("failed-backend", [await run(false)]);
    const good = scenario("healthy-backend", [await run(true)]);
    const output: string[] = [];
    const log = spyOn(console, "log").mockImplementation((line) => { output.push(String(line)); });
    try { printBests([bad, good]); } finally { log.mockRestore(); }
    expect(output.join("\n")).toContain("best p50=healthy-backend best p95=healthy-backend");
    const [header, row] = toCsv([bad]).trimEnd().split("\n");
    const columns = header!.split(",");
    const values = row!.split(",");
    expect(values.length).toBe(columns.length);
    expect(values[columns.indexOf("p50")]).toBe("");
    expect(values[columns.indexOf("errors")]).toBe("4");
    expect(values[columns.indexOf("errorRate")]).toBe("1.0000");
  });

  test("transaction rates remain finite with zero attempts", () => {
    expect(txRates({ attempted: 0, attemptsTotal: 0, committed: 0, successAfterRetry: 0, failedAfterRetries: 0, conflicts: 0, retries: 0, error: null })).toMatchObject({ conflictRate: 0, retryRate: 0, avgRetries: 0 });
  });
});
