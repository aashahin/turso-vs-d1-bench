import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { serializeResult, writeArtifacts } from "../src/artifacts.ts";
import { summarize, dispersion } from "../src/stats.ts";
import { summarizeRuns, type ScenarioResult } from "../src/results.ts";
import { comparisonKey, saturationEvidence } from "../src/reporters/manhali.ts";
import { runScenario } from "../src/runner.ts";
import { runColdStart } from "../src/cold-start.ts";
import { runOptions, sample } from "./helpers.ts";
describe("failure-aware decision reports and artifacts", () => {
  test("serialization suppresses unsupported small-sample p99 and preserves failure counters", () => {
    const json = JSON.parse(
      serializeResult({
        latency: summarize([1, 2, 3], 1000),
        success: 3,
        failed: 7,
        errorRate: 0.7,
      }),
    );
    expect(json.latency.p99).toBeNull();
    expect(json.failed).toBe(7);
    expect(json.latency.p95).toBe(3);
    expect(
      JSON.parse(serializeResult(summarize(Array(1000).fill(2), 1000))).p99,
    ).toBe(2);
  });
  test("exclusive output cannot overwrite history or leave partial companion files", async () => {
    const directory = await mkdtemp(join(tmpdir(), "bench-history-"));
    try {
      const old = join(directory, "history.json"),
        companion = join(directory, "new.csv");
      await writeArtifacts({ [old]: "original" });
      await expect(
        writeArtifacts({ [companion]: "partial", [old]: "overwrite" }),
      ).rejects.toThrow();
      expect(await readFile(old, "utf8")).toBe("original");
      await expect(readFile(companion, "utf8")).rejects.toThrow();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
  test("dispersion and bootstrap are deterministic and single runs do not imply confidence", () => {
    expect(dispersion([1, 2, 3, 4, 5])).toEqual(dispersion([1, 2, 3, 4, 5]));
    expect(dispersion([10])?.medianBootstrap95).toBeNull();
    expect(dispersion([1, 2, 3, 4, 5])).toMatchObject({
      min: 1,
      max: 5,
      median: 3,
    });
  });
  test("all-failed runs contribute to reliability but never invent latency", async () => {
    const good = { run: 1, ...(await runScenario(runOptions())) };
    const bad = {
      run: 2,
      ...(await runScenario(
        runOptions({
          exec: async () => {
            throw new Error("HTTP 503 unavailable");
          },
        }),
      )),
    };
    const summary = summarizeRuns([good, bad, bad]);
    expect(summary.pooledErrorRate).toBeCloseTo(2 / 3);
    expect(summary.medianP50).toBe(20);
    expect(summary.medianP99).toBeNull();
    expect(summary.warnings).toContain("HIGH_ERROR_RATE");
  });
  test("reports never group a shared schema or Drizzle backend with isolated raw execution", () => {
    const base = {
      mode: "edge",
      suite: "latency",
      test: "point-read",
      tenantCount: 10,
      concurrency: 10,
      tenantIsolation: "database-per-tenant",
      accessLayer: "raw",
    } as ScenarioResult;
    expect(comparisonKey(base)).not.toBe(
      comparisonKey({ ...base, tenantIsolation: "shared-schema" }),
    );
    expect(comparisonKey(base)).not.toBe(
      comparisonKey({ ...base, accessLayer: "drizzle" }),
    );
  });
  test("saturation uses measured error/latency evidence and never extrapolates beyond tested points", async () => {
    const run = { run: 1, ...(await runScenario(runOptions())) };
    const row = (rps: number, p95: number, errors: number): ScenarioResult => ({
      mode: "edge",
      suite: "saturation",
      test: "point-read",
      backend: "d1-per-tenant",
      tenantIsolation: "database-per-tenant",
      accessLayer: "raw",
      engine: "d1",
      transactionMode: "none",
      workload: "kv",
      scanLimit: null,
      tenantMode: "single",
      tenantCount: 1,
      concurrency: 100,
      distribution: "uniform",
      loadModel: "arrival-rate",
      offeredRps: rps,
      durationSec: 1,
      iterations: 100,
      timeoutMs: 1000,
      runs: [run],
      summary: {
        ...summarizeRuns([run]),
        medianP95: p95,
        medianRps: rps,
        pooledErrorRate: errors,
      },
    });
    const evidence = saturationEvidence([
      row(10, 10, 0),
      row(25, 50, 0.02),
    ])[0]!;
    expect(evidence.highestTestedStableRps).toBe(10);
    expect(evidence.firstObservedInstability?.offeredRps).toBe(25);
    expect(evidence.firstObservedInstability?.reasons).toContain(
      "error rate >= 1%",
    );
  });
  test("raw SDK error text cannot leak a token through runner error samples", async () => {
    const r = await runScenario(
      runOptions({
        exec: async () => {
          throw new Error("fetch failed https://secret-token@host.invalid");
        },
      }),
    );
    expect(JSON.stringify(r)).not.toContain("secret-token");
  });
});
describe("idle variants remain observational", () => {
  test.each([
    "idle-same-row",
    "idle-different-row",
    "idle-same-tenant",
    "idle-different-tenant",
  ])(
    "%s records row/tenant control plus independent pool evidence",
    async (variant) => {
      const seen: { id: number; tenant: number }[] = [];
      const r = await runColdStart({
        run: 1,
        backend: "turso-per-tenant",
        tenantMode: "distributed",
        tenantCount: 2,
        tenantIsolation: "database-per-tenant",
        samples: 1,
        idleMs: 30000,
        warmRequests: 2,
        timeoutMs: 1000,
        seedRows: 100,
        variant,
        sleep: async () => {},
        ops: () => ({
          test: "point-read",
          method: "GET",
          params: { id: 10, tenant: 1 },
        }),
        exec: async (op) => {
          seen.push({
            id: Number(op.params.id),
            tenant: Number(op.params.tenant),
          });
          return sample({
            pool: {
              poolHit: false,
              connectionReused: false,
              checkoutMs: 0,
              connectionCreateMs: 0,
              events: {
                connectionCreated: 1,
                connectionReused: 0,
                connectionClosed: 0,
                poolHit: 0,
                poolMiss: 1,
                poolEviction: 0,
              },
            },
          });
        },
      });
      expect(r).toMatchObject({
        newConnections: 1,
        newPools: 1,
        firstOnNewIsolate: 0,
        firstLifecycleUnknown: 1,
      });
      expect(seen[0]).toEqual({ id: 10, tenant: 1 });
      expect(seen[1]).toEqual({
        id:
          variant === "idle-different-row" || variant === "idle-same-tenant"
            ? 11
            : 10,
        tenant: variant === "idle-different-tenant" ? 2 : 1,
      });
      expect(r.trials[0]?.poolPresentAtCheckout).toBe(false);
    },
  );
});

test("out-of-order responses cannot replace newer pool peaks and cumulative counters", async () => {
  const { TenantPoolManager } = await import("../worker/src/pool.ts");
  const { DEFAULT_POOL } = await import("../shared/policy.ts");
  const { deferred } = await import("./helpers.ts");
  const pool = new TenantPoolManager(DEFAULT_POOL, () => ({
    async close() {},
  }));
  await pool.use("A", "A", async () => {});
  const older = pool.snapshot();
  await pool.use("B", "B", async () => {});
  const newer = pool.snapshot();
  const late = deferred(),
    second = deferred();
  const running = runScenario(
    runOptions({
      totalOps: 2,
      concurrency: 2,
      exec: async (op) => {
        if (op.params.id === 0) await late.promise;
        else second.resolve();
        return sample({
          poolMetrics: op.params.id === 0 ? older : newer,
          lifecycle: {
            isolateId: "same",
            requestNumber: Number(op.params.id) + 1,
            firstRequest: op.params.id === 0,
          },
        });
      },
    }),
  );
  await second.promise;
  for (let i = 0; i < 12; i++) await Promise.resolve();
  late.resolve();
  const outcome = await running;
  expect(outcome.pool?.workerSnapshots.same).toMatchObject({
    connectionCreated: 2,
    peakConnections: 2,
    snapshotSequence: newer.snapshotSequence,
  });
  await pool.close();
});
