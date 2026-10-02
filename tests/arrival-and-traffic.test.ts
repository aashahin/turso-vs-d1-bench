import { afterEach, describe, expect, jest, test } from "bun:test";
import { arrivals } from "../src/scheduler.ts";
import {
  DEFAULT_TRAFFIC,
  tenantSelector,
  concentration,
} from "../src/workloads/distribution.ts";
import {
  LMS_PROFILES,
  operationSelector,
  examOp,
} from "../src/workloads/profiles.ts";
import { deferred, dims, runOptions, sample } from "./helpers.ts";
import { runScenario } from "../src/runner.ts";
afterEach(() => jest.useRealTimers());
const flush = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve();
};
describe("open loop arrivals", () => {
  test("offers every absolute arrival despite stalled requests; drops expose capacity loss", async () => {
    jest.useFakeTimers();
    const gate = deferred();
    const seen: number[] = [];
    const running = arrivals({
      total: 5,
      rps: 10,
      maxInFlight: 2,
      maxStartLagMs: 1000,
      execute: async (i) => {
        seen.push(i);
        await gate.promise;
      },
    });
    await flush();
    for (let i = 0; i < 5; i++) {
      jest.advanceTimersByTime(100);
      await flush();
    }
    expect(seen).toEqual([0, 1]);
    gate.resolve();
    const m = await running;
    expect(m).toMatchObject({
      scheduled: 5,
      started: 2,
      completed: 2,
      dropped: 3,
      peakInFlight: 2,
      inFlight: 0,
      offeredWindowMs: 500,
    });
    expect(jest.getTimerCount()).toBe(0);
  });
  test("generator lateness is recorded rather than shifting the next due time", async () => {
    let time = 0;
    const starts: number[] = [];
    const m = await arrivals({
      total: 4,
      rps: 10,
      maxInFlight: 100,
      maxStartLagMs: 1000,
      clock: {
        now: () => time,
        sleep: async (ms) => {
          time += ms + 50;
        },
      },
      execute: async () => {
        starts.push(time);
      },
    });
    expect(m.scheduled).toBe(4);
    expect(m.lateStarts).toBe(3);
    expect(m.queueDelayMs).toEqual([0, 50, 50, 50]);
    expect(starts).toEqual([0, 150, 250, 350]);
  });
  test("arrival run includes failures, drops and schedule-to-completion latency separately", async () => {
    let now = 0;
    const r = await runScenario(
      runOptions({
        loadModel: "arrival-rate",
        totalOps: 4,
        targetRps: 10,
        maxInFlight: 10,
        clock: {
          now: () => now,
          sleep: async (ms) => {
            now += ms + 10;
          },
        },
        exec: async (op) => {
          if (op.params.id === 1) throw new Error("HTTP 503 unavailable");
          return sample();
        },
      }),
    );
    expect(r.success + r.failed).toBe(4);
    expect(r.arrival).toMatchObject({
      scheduled: 4,
      started: 4,
      completed: 4,
      dropped: 0,
    });
    expect(r.scheduledLatency?.max).toBeGreaterThan(r.e2e.max);
    expect(r.errorRate).toBe(0.25);
    expect(r.warnings).toContain("OFFERED_LOAD_NOT_ACHIEVED");
  });
});
describe("deterministic SaaS traffic", () => {
  test.each(["uniform", "hotset", "zipf"] as const)(
    "%s replays by index and stays within tenant range",
    (distribution) => {
      const choose = tenantSelector(500, { ...DEFAULT_TRAFFIC, distribution });
      const seq = Array.from({ length: 1000 }, (_, i) => choose(i));
      expect(
        Array.from({ length: 1000 }, (_, i) => choose(999 - i)).reverse(),
      ).toEqual(seq);
      expect(Math.min(...seq)).toBeGreaterThanOrEqual(1);
      expect(Math.max(...seq)).toBeLessThanOrEqual(500);
    },
  );
  test("hotset places approximately 80% traffic on exactly 10% of tenants", () => {
    const choose = tenantSelector(100, {
      ...DEFAULT_TRAFFIC,
      distribution: "hotset",
    });
    let hot = 0;
    const counts: Record<string, number> = {};
    for (let i = 0; i < 100000; i++) {
      const t = choose(i);
      if (t <= 10) hot++;
      counts[t] = (counts[t] ?? 0) + 1;
    }
    expect(hot / 100000).toBeCloseTo(0.8, 2);
    expect(concentration(counts, 100).tenantsObserved).toBe(100);
  });
  test("Zipf concentration exceeds uniform and one-tenant hotset stays valid", () => {
    const tally = (distribution: "uniform" | "zipf") => {
      const choose = tenantSelector(100, { ...DEFAULT_TRAFFIC, distribution });
      let first = 0;
      for (let i = 0; i < 10000; i++) if (choose(i) === 1) first++;
      return first;
    };
    expect(tally("zipf")).toBeGreaterThan(tally("uniform") * 5);
    expect(
      tenantSelector(1, { ...DEFAULT_TRAFFIC, distribution: "hotset" })(999),
    ).toBe(1);
  });
  test.each(Object.entries(LMS_PROFILES))(
    "%s produces the exact configured operation mix every complete block",
    (_name, mix) => {
      const choose = operationSelector(mix);
      for (const block of [0, 1, 17]) {
        const counts: Record<string, number> = {};
        for (let i = block * 100; i < (block + 1) * 100; i++) {
          const op = choose(i);
          counts[op] = (counts[op] ?? 0) + 1;
        }
        expect(counts).toEqual(mix);
      }
    },
  );
  test("invalid mixes fail instead of silently changing workload", () => {
    expect(() => operationSelector({ "lesson-page": 99 })).toThrow();
    expect(() => operationSelector({ "lesson-page": 100.1 })).toThrow();
  });
  test("exam submissions target independent students and a common valid quiz", () => {
    const ops = Array.from({ length: 40 }, (_, i) => examOp(i, 2, dims));
    expect(new Set(ops.map((op) => op.params.student)).size).toBe(40);
    expect(new Set(ops.map((op) => op.params.quiz)).size).toBe(1);
    expect(() => examOp(40, 1, dims)).toThrow("seed");
  });
});
