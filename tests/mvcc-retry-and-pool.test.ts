// Regression suite for the parts of the benchmark harness that broke in
// production during the tursodb work:
//   * MVCC conflict retrying (attempt counting, latency across retries,
//     never retrying application errors) — the Worker's server-side loop;
//   * connection-pool queueing (a slot must never stay gated by an abandoned
//     or stalled request — that hang made Cloudflare cancel Worker requests
//     with "error code: 1101" and took every Turso backend down);
//   * test classification used by the reporters (scans pinned as `scan-100`
//     were reported as writes).
//
// Run with: bun test
import { describe, expect, test } from "bun:test";
import { execConcurrent, isConflictError } from "../worker/src/queries.ts";
import { txModeOf, withPooledTurso, withPooledTursoDb } from "../worker/src/backends.ts";
import { mergeTx } from "../src/stats.ts";
import { isReadTest, WRITE_TESTS } from "../shared/backend-tests.ts";

function conflictError(message: string, code?: string): Error {
  const error = new Error(message);
  if (code !== undefined) (error as Error & { code?: string }).code = code;
  return error;
}

/** attempt callback that fails according to `outcome(attemptNumber)`. */
function attempts(outcome: (attempt: number) => void): () => Promise<void> {
  let attempt = 0;
  return async () => {
    attempt++;
    outcome(attempt);
  };
}

describe("MVCC conflict retrying", () => {
  const retryScenarios = [
    { name: "no conflict commits on the first attempt", conflicts: 0 },
    { name: "one conflict commits after one retry", conflicts: 1 },
    { name: "two conflicts commit after two retries", conflicts: 2 },
  ];

  test.each(retryScenarios)("$name", async ({ conflicts }) => {
    const run = await execConcurrent(3, attempts((attempt) => {
      if (attempt <= conflicts) throw conflictError("Tursodb error: Write-write conflict");
    }));

    expect(run.committed).toBe(true);
    expect(run.stats.attemptsTotal).toBe(conflicts + 1);
    expect(run.stats.conflicts).toBe(conflicts);
    expect(run.stats.retries).toBe(conflicts);
    expect(run.stats.successAfterRetry).toBe(conflicts > 0 ? 1 : 0);
    expect(run.stats.failedAfterRetries).toBe(0);
  });

  test("latency covers every attempt plus its backoff", async () => {
    const run = await execConcurrent(2, attempts((attempt) => {
      if (attempt <= 2) throw conflictError("write conflict");
    }));

    // Backoffs before attempts 2 and 3 are at least 1ms and 2ms.
    expect(run.queryMs).toBeGreaterThanOrEqual(3);
  });

  test("exhausting the retry budget is reported as a failure, not dropped", async () => {
    const run = await execConcurrent(1, attempts(() => {
      throw conflictError("database is locked");
    }));

    expect(run.committed).toBe(false);
    expect(run.stats.attemptsTotal).toBe(2);
    expect(run.stats.retries).toBe(1);
    expect(run.stats.failedAfterRetries).toBe(1);
    expect(run.stats.error).toContain("locked");
  });

  test.each([
    ["constraint", conflictError("UNIQUE constraint failed: kv.id", "SQLITE_CONSTRAINT")],
    ["query timeout", conflictError("Query timed out", "TIMEOUT")],
    ["application", conflictError("no such table: nope")],
    ["connection limit", conflictError("Database connections limit exceeded, try to reduce concurrency")],
  ])("propagates %s errors instead of retrying them", async (_name, error) => {
    const run = execConcurrent(3, attempts(() => {
      throw error;
    }));

    await expect(run).rejects.toBe(error);
  });

  test.each([
    ["SQLITE_BUSY code", conflictError("x", "SQLITE_BUSY"), true],
    ["SQLITE_BUSY_SNAPSHOT code", conflictError("x", "SQLITE_BUSY_SNAPSHOT"), true],
    ["write/write conflict message", conflictError("Tursodb error: Write-write conflict"), true],
    ["locked message", conflictError("database is locked"), true],
    ["constraint code", conflictError("UNIQUE constraint failed", "SQLITE_CONSTRAINT"), false],
    ["timeout code", conflictError("Query timed out", "TIMEOUT"), false],
    ["connection limit", conflictError("Database connections limit exceeded"), false],
    ["non-error value", "boom", false],
  ])("classifies %s as retryable=%s", (_name, error, retryable) => {
    expect(isConflictError(error)).toBe(retryable);
  });

  test("rates aggregate across runs by summing counters", () => {
    const merged = mergeTx([
      { attempted: 10, attemptsTotal: 12, committed: 10, successAfterRetry: 2, failedAfterRetries: 0, conflicts: 2, retries: 2, error: null },
      { attempted: 10, attemptsTotal: 15, committed: 9, successAfterRetry: 3, failedAfterRetries: 1, conflicts: 6, retries: 5, error: "busy" },
    ]);

    expect(merged?.attemptsTotal).toBe(27);
    expect(merged?.conflicts).toBe(8);
    expect(merged?.conflictRate).toBeCloseTo(8 / 27, 10);
    expect(merged?.avgRetries).toBeCloseTo(7 / 20, 10);
  });

  test("a single-run scenario reports no transaction counters", () => {
    expect(mergeTx([])).toBeNull();
  });
});

describe("backend transaction modes", () => {
  test.each([
    ["tursodb-concurrent", "insert", "concurrent"],
    ["tursodb-concurrent", "independent-writes", "concurrent"],
    ["tursodb-concurrent", "hot-row-write", "concurrent"],
    ["tursodb-concurrent", "point-read", "none"],
    ["tursodb-reused", "independent-writes", "transaction"],
    ["tursodb-reused", "insert", "none"],
    ["turso-reused", "independent-writes", "transaction"],
    ["turso-reused", "insert", "none"],
    ["d1-eeur", "independent-writes", "transaction"],
    ["d1-eeur", "point-read", "none"],
  ] as const)("%s runs %s as %s", (backend, test, expected) => {
    expect(txModeOf(backend, test)).toBe(expected);
  });

  test("write tests include the MVCC benchmarks", () => {
    expect(WRITE_TESTS["hot-row-write"]).toBe(true);
    expect(WRITE_TESTS["independent-writes"]).toBe(true);
    expect(WRITE_TESTS["point-read"]).toBeUndefined();
  });
});

describe("report classification", () => {
  test.each([
    ["scan", true],
    ["scan-100", true],
    ["scan-1000", true],
    ["point-read", true],
    ["lesson-page", true],
    ["insert", false],
    ["mixed", false],
    ["submit-quiz-answer", false],
  ])("treats %s as a read: %s", (testName, expected) => {
    expect(isReadTest(testName)).toBe(expected);
  });
});

describe("connection pool queueing", () => {
  // The pool only needs config to key on: nothing below reaches the network
  // (tasks resolve locally or are abandoned), so only queueing is exercised.
  const poolEnv = {
    TURSO_URL: "https://pool-test.invalid",
    TURSO_TOKEN: "t",
    TURSODB_URL: "https://pool-test.invalid",
    TURSODB_TOKEN: "t",
  } as never;
  const slots = 8;

  /** Captures a rejection without relying on promise matchers. */
  async function rejectionOf(promise: Promise<unknown>): Promise<Error> {
    try {
      await promise;
    } catch (error) {
      return error as Error;
    }
    throw new Error("expected the pool call to reject");
  }

  test.each([
    ["libSQL pool", withPooledTurso],
    ["tursodb pool", withPooledTursoDb],
  ] as const)("%s: a checkout whose client already left never runs its operation", async (_name, pooled) => {
    const controller = new AbortController();
    controller.abort();
    let ran = false;
    const queued = pooled(poolEnv, async () => {
      ran = true;
      return "ran";
    }, controller.signal);

    expect((await rejectionOf(queued)).name).toBe("AbortError");
    expect(ran).toBe(false);
  });

  test("an abandoned queued request frees its slot for later requests", async () => {
    // Every slot busy, so the request below is genuinely queued.
    const busy = Array.from({ length: slots }, () => withPooledTurso(poolEnv, () => Bun.sleep(60)));
    const controller = new AbortController();
    let ran = false;
    const abandoned = withPooledTurso(poolEnv, async () => {
      ran = true;
      return "never";
    }, controller.signal);
    await Bun.sleep(5);
    controller.abort();

    expect((await rejectionOf(abandoned)).name).toBe("AbortError");
    expect(ran).toBe(false);
    await Promise.all(busy);

    // Before the fix the abandoned request left its slot gated forever and the
    // Workers runtime cancelled every later request that landed on it.
    const after = await Promise.all(Array.from({ length: slots }, () => withPooledTurso(poolEnv, async () => "ok")));
    expect(after.map((result) => result.out)).toEqual(Array.from({ length: slots }, () => "ok"));
  });

  test("a stalled request frees its slot after the pool deadline", async () => {
    // The pool deadline is 15s; a stalled slot used to block later requests
    // for the lifetime of the isolate.
    const stalled = Array.from({ length: slots }, () => withPooledTurso(poolEnv, () => new Promise<string>(() => {})).catch(() => null));
    void stalled;

    const started = Date.now();
    const later = await withPooledTurso(poolEnv, async () => "later");
    const waitedMs = Date.now() - started;

    expect(later.out).toBe("later");
    expect(waitedMs).toBeGreaterThanOrEqual(14_000);
  }, 30_000);
});
