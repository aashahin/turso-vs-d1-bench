import type { OpSample, RunOpts } from "../src/runner.ts";
import type { Dims } from "../src/workloads/common.ts";

export function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

export function sample(overrides: Partial<OpSample> = {}): OpSample {
  return {
    e2eMs: 20, dbMs: 10, queryMs: 8, buildMs: null,
    clientCreationMs: null, clientCloseMs: null, checkoutMs: null,
    replica: null, region: null, replicaReads: 0, primaryReads: 0, totalReads: 0,
    engine: "d1", transactionMode: "none", tx: null, txFailed: false,
    ...overrides,
  };
}

export function runOptions(overrides: Partial<RunOpts> = {}): RunOpts {
  return {
    label: "offline", ops: (index) => ({ test: "point-read", method: "GET", params: { id: index } }),
    totalOps: 4, durationSec: 0, concurrency: 1, warmup: 0,
    loadModel: "constant", timeoutMs: 1000, exec: async () => sample(), ...overrides,
  };
}

export const dims: Dims = {
  seedRows: 100, scanLimits: [10, 25], tenants: 3,
  studentsPerTenant: 40, coursesPerTenant: 5, lessonsPerCourse: 10,
};
