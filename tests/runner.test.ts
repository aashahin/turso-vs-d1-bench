import { afterEach, describe, expect, jest, test } from "bun:test";
import { execOne, runScenario } from "../src/runner.ts";
import { deferred, runOptions, sample } from "./helpers.ts";

afterEach(() => {
  jest.useRealTimers();
});

describe("operation deadlines", () => {
  test("times out an executor that ignores AbortSignal and ignores its late completion", async () => {
    jest.useFakeTimers();
    const late = deferred<ReturnType<typeof sample>>();
    let signal!: AbortSignal;
    const running = execOne(
      async (_op, received) => {
        signal = received;
        return late.promise;
      },
      runOptions().ops(0),
      100,
    );
    jest.advanceTimersByTime(100);
    const outcome = await running;
    expect(signal.aborted).toBe(true);
    expect(outcome).toMatchObject({
      ok: false,
      err: { errorClass: "timeout" },
    });
    late.resolve(sample());
    expect(await running).toBe(outcome);
    expect(jest.getTimerCount()).toBe(0);
  });

  test("observes a late rejection after timing out", async () => {
    jest.useFakeTimers();
    const late = deferred<ReturnType<typeof sample>>();
    const running = execOne(() => late.promise, runOptions().ops(0), 100);
    jest.advanceTimersByTime(100);
    expect(await running).toMatchObject({
      ok: false,
      err: { errorClass: "timeout" },
    });
    late.reject(new Error("late backend failure"));
    await Promise.resolve();
    expect(jest.getTimerCount()).toBe(0);
  });

  test("clears the deadline after an early success or failure", async () => {
    jest.useFakeTimers();
    expect(
      (await execOne(async () => sample(), runOptions().ops(0), 100)).ok,
    ).toBe(true);
    expect(
      await execOne(
        async () => {
          throw new Error("HTTP 429 overloaded");
        },
        runOptions().ops(0),
        100,
      ),
    ).toMatchObject({
      ok: false,
      err: { errorClass: "rate-limit", httpStatus: 429 },
    });
    expect(jest.getTimerCount()).toBe(0);
  });
});

describe("load scheduling and accounting", () => {
  test.each(["constant", "burst"] as const)(
    "%s executes the exact budget and excludes warmup",
    async (loadModel) => {
      const seen: number[] = [];
      const result = await runScenario(
        runOptions({
          loadModel,
          totalOps: 7,
          warmup: 2,
          concurrency: 3,
          exec: async (op) => {
            const index = Number(op.params.id);
            seen.push(index);
            return sample({ e2eMs: index < 2 ? 9000 : 20 });
          },
        }),
      );
      expect(seen.sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8]);
      expect(result.success).toBe(7);
      expect(result.e2e).toMatchObject({ n: 7, p50: 20, max: 20 });
    },
  );

  test.each(["constant", "burst"] as const)(
    "%s caps in-flight work and handles partial final batches",
    async (loadModel) => {
      const firstWave = deferred();
      const release = deferred();
      let active = 0;
      let peak = 0;
      const resultPromise = runScenario(
        runOptions({
          loadModel,
          totalOps: 5,
          concurrency: 3,
          exec: async () => {
            active++;
            peak = Math.max(peak, active);
            if (active === 3) firstWave.resolve();
            await release.promise;
            active--;
            return sample();
          },
        }),
      );
      await firstWave.promise;
      expect(active).toBe(3);
      release.resolve();
      const result = await resultPromise;
      expect(peak).toBe(3);
      expect(active).toBe(0);
      expect(result.success).toBe(5);
    },
  );

  test("constant load refills a free slot while another request is still running", async () => {
    const slow = deferred();
    const refilled = deferred();
    const running = runScenario(
      runOptions({
        concurrency: 2,
        totalOps: 3,
        exec: async (op) => {
          if (op.params.id === 0) await slow.promise;
          if (op.params.id === 2) refilled.resolve();
          return sample();
        },
      }),
    );
    await refilled.promise;
    slow.resolve();
    expect((await running).success).toBe(3);
  });

  test.each(["constant", "burst"] as const)(
    "%s stops claiming at duration and drains in-flight requests",
    async (loadModel) => {
      jest.useFakeTimers();
      const entered = deferred();
      const release = deferred();
      const running = runScenario(
        runOptions({
          loadModel,
          totalOps: 100,
          durationSec: 1,
          exec: async () => {
            entered.resolve();
            await release.promise;
            return sample();
          },
          timeoutMs: 5000,
        }),
      );
      await entered.promise;
      jest.advanceTimersByTime(1000);
      release.resolve();
      const result = await running;
      expect(result.success).toBe(1);
      expect(result.failed).toBe(0);
    },
  );

  test("counts failed transactions and HTTP errors without polluting success percentiles", async () => {
    const tx = {
      attempted: 1,
      attemptsTotal: 3,
      committed: 0,
      conflicts: 3,
      retries: 2,
      successAfterRetry: 0,
      failedAfterRetries: 1,
      error: "write conflict",
    };
    const result = await runScenario(
      runOptions({
        exec: async (op) => {
          if (op.params.id === 1) throw new Error("HTTP 429 too many requests");
          if (op.params.id === 2)
            return sample({ e2eMs: 9999, dbMs: 9999, tx, txFailed: true });
          return sample({
            replica: true,
            replicaReads: 2,
            totalReads: 2,
            region: "test-region",
          });
        },
      }),
    );
    expect(result).toMatchObject({
      success: 2,
      failed: 2,
      errorRate: 0.5,
      e2e: { n: 2, max: 20 },
      db: { n: 2, max: 10 },
      replicaRate: 1,
      replicaReads: 4,
      totalReads: 4,
    });
    expect(result.errorsByClass.conflict).toBe(1);
    expect(result.errorsByStatus["http-429"]).toBe(1);
    expect(result.tx).toMatchObject(tx);
  });

  test("retains failure counters and routing evidence when all operations fail", async () => {
    const result = await runScenario(
      runOptions({
        exec: async () =>
          sample({
            txFailed: true,
            region: "test-region",
            totalReads: 1,
            primaryReads: 1,
          }),
      }),
    );
    expect(result).toMatchObject({
      success: 0,
      failed: 4,
      errorRate: 1,
      rps: 0,
      db: null,
      totalReads: 4,
      primaryReads: 4,
      regions: { "test-region": 4 },
    });
    expect(result.e2e.n).toBe(0);
    expect(result.errorsByClass.conflict).toBe(4);
  });
});

test("topology violations remain invalid after the diagnostic error sample cap fills", async () => {
  const result = await runScenario(
    runOptions({
      totalOps: 7,
      exec: async (op) => {
        if (Number(op.params.id) < 5) throw new Error("HTTP 503 unavailable");
        if (op.params.id === 5)
          throw new Error("TOPOLOGY_INVALID: unexpected binding");
        return sample();
      },
    }),
  );
  expect(result.sampleErrors).toHaveLength(5);
  expect(result.warnings).toContain("TOPOLOGY_INVALID");
  expect(result.failed).toBe(6);
});
test("an iteration cap that truncates an offered-load window is explicitly warned", async () => {
  let now = 0;
  const result = await runScenario(
    runOptions({
      totalOps: 2,
      durationSec: 1,
      loadModel: "arrival-rate",
      targetRps: 100,
      clock: {
        now: () => now,
        sleep: async (ms) => {
          now += ms;
        },
      },
    }),
  );
  expect(result.arrival?.offeredWindowMs).toBe(20);
  expect(result.warnings).toContain("BUDGET_LIMITED_WINDOW");
});
