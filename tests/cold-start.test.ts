import { describe, expect, test } from "bun:test";
import { coldStartCsv, runColdStart } from "../src/cold-start.ts";
import { createLifecycleTracker } from "../worker/src/lifecycle.ts";
import { parseLifecycle } from "../shared/lifecycle.ts";
import { sample } from "./helpers.ts";

const options = {
  run: 1, backend: "d1", tenantMode: "single", tenantCount: 1,
  samples: 2, idleMs: 30000, warmRequests: 2, timeoutMs: 1000,
  ops: (i: number) => ({ test: "point-read", method: "GET" as const, params: { id: i + 1, tenant: 1 } }),
  sleep: async (_ms: number) => {},
};

describe("cold-start measurement", () => {
  test("idles before every first request, skips warmup, then repeats the same read sequentially", async () => {
    const events: string[] = [];
    let request = 0;
    let track = createLifecycleTracker();
    const result = await runColdStart({ ...options,
      sleep: async (ms) => { events.push(`idle:${ms}`); track = createLifecycleTracker(); },
      exec: async (op) => {
        events.push(`read:${op.params.id}`);
        const lifecycle = track();
        const first = request++ % 3 === 0;
        return sample({ e2eMs: first ? 100 : 20, dbMs: first ? 60 : 10, lifecycle });
      },
    });
    expect(events).toEqual(["idle:30000", "read:1", "read:1", "read:1", "idle:30000", "read:2", "read:2", "read:2"]);
    expect(result).toMatchObject({ first: { success: 2, failed: 0, e2e: { p50: 100 } }, following: { success: 4, e2e: { p50: 20 } }, firstOnNewIsolate: 2, pairedTrials: 2, medianE2eDeltaMs: 80, medianDbDeltaMs: 50 });
  });

  test.each(["reused", "missing", "changed"] as const)("does not infer a cold isolate from idle time (%s lifecycle)", async (kind) => {
    let request = 0;
    const result = await runColdStart({ ...options, samples: 1, exec: async () => {
      const n = ++request;
      return sample({ lifecycle: kind === "missing" ? null : { isolateId: kind === "changed" ? `isolate-${n}` : "same", requestNumber: n + 4, firstRequest: false } });
    } });
    expect(result.firstOnNewIsolate).toBe(0);
    expect(result.firstOnReusedIsolate).toBe(kind === "missing" ? 0 : 1);
    expect(result.firstLifecycleUnknown).toBe(kind === "missing" ? 1 : 0);
    expect(result.pairedTrials).toBe(kind === "reused" ? 1 : 0);
    expect(result.medianE2eDeltaMs).toBe(kind === "reused" ? 0 : null);
  });

  test("records errors in both phases, keeps successful follow-ups, and excludes unpaired deltas", async () => {
    let request = 0;
    const result = await runColdStart({ ...options, samples: 1, exec: async () => {
      if (++request < 3) throw new Error("HTTP 503 backend unavailable");
      return sample();
    } });
    expect(result).toMatchObject({ first: { success: 0, failed: 1, e2e: null, db: null }, following: { success: 1, failed: 1 }, pairedTrials: 0, medianE2eDeltaMs: null });
    expect(result.trials[0]?.first).toMatchObject({ ok: false, err: { errorClass: "backend", httpStatus: 503 } });
    const [head, row] = coldStartCsv([result]).trimEnd().split("\n");
    const columns = head!.split(",");
    const fields = row!.split(",");
    expect(fields.length).toBe(columns.length);
    expect(fields[columns.indexOf("firstE2eP50")]).toBe("");
    expect(fields[columns.indexOf("firstErrors")]).toBe("1");
    expect(fields[columns.indexOf("pairedE2eDeltaMs")]).toBe("");
  });

  test("a failed first request is not retried or replaced with a successful sample", async () => {
    let requests = 0;
    const result = await runColdStart({ ...options, samples: 1, exec: async () => {
      if (++requests === 1) throw new DOMException("timed out", "TimeoutError");
      return sample();
    } });
    expect(requests).toBe(3);
    expect(result.first.failed).toBe(1);
    expect(result.following.success).toBe(2);
  });

  test("rejects writes before idling or issuing any request", async () => {
    let called = false;
    await expect(runColdStart({ ...options, ops: () => ({ test: "insert", method: "POST", params: {} }), sleep: async () => { called = true; }, exec: async () => { called = true; return sample(); } })).rejects.toThrow("read-only point-read");
    expect(called).toBe(false);
  });
});

describe("Worker lifecycle evidence", () => {
  test("each fresh tracker starts once and captures request order before completion", async () => {
    const a = createLifecycleTracker();
    const b = createLifecycleTracker();
    const first = a();
    const second = a();
    expect(first).toMatchObject({ requestNumber: 1, firstRequest: true });
    expect(second).toEqual({ isolateId: first.isolateId, requestNumber: 2, firstRequest: false });
    expect(b().isolateId).not.toBe(first.isolateId);
  });

  test.each([undefined, null, {}, { isolateId: "", requestNumber: 1, firstRequest: true }, { isolateId: "x", requestNumber: 2, firstRequest: true }, { isolateId: "x", requestNumber: -1, firstRequest: false }, { isolateId: "x", requestNumber: 1.5, firstRequest: false }])("invalid or missing evidence is unknown: %j", (value) => {
    expect(parseLifecycle(value)).toBeNull();
  });
});
