// Observe idle-to-first-request latency, followed by a sequential warm baseline.
// Idle time cannot force Worker eviction or database cache eviction. Keep the
// lifecycle evidence and raw samples so neither is inferred from a slow request.
import { execOne, type OpFn, type OpResult, type OpSample } from "./runner.ts";
import { median, summarize, type Summary } from "./stats.ts";
import type { OpQuery } from "./workloads/common.ts";

export interface ColdTrial {
  idleMs: number;
  first: OpResult;
  following: OpResult[];
  /** Deltas only pair successful requests on the same observed isolate. */
  e2eDeltaMs: number | null;
  dbDeltaMs: number | null;
}

export interface ColdPhase {
  success: number;
  failed: number;
  e2e: Summary | null;
  db: Summary | null;
  query: Summary | null;
}

export interface ColdStartRun {
  run: number;
  mode: "edge";
  backend: string;
  tenantMode: string;
  tenantCount: number;
  idleMs: number;
  warmRequests: number;
  first: ColdPhase;
  following: ColdPhase;
  firstOnNewIsolate: number;
  firstOnReusedIsolate: number;
  firstLifecycleUnknown: number;
  pairedTrials: number;
  medianE2eDeltaMs: number | null;
  medianDbDeltaMs: number | null;
  trials: ColdTrial[];
}

function successful(result: OpResult): result is { ok: true; sample: OpSample } {
  return result.ok && !result.sample.txFailed;
}

function phase(results: OpResult[]): ColdPhase {
  const samples = results.filter(successful).map((r) => r.sample);
  const wallMs = samples.reduce((sum, sample) => sum + sample.e2eMs, 0);
  const stats = (values: number[]): Summary | null => values.length ? summarize(values, wallMs) : null;
  return {
    success: samples.length,
    failed: results.length - samples.length,
    e2e: stats(samples.map((s) => s.e2eMs)),
    db: stats(samples.flatMap((s) => s.dbMs === null ? [] : [s.dbMs])),
    query: stats(samples.flatMap((s) => s.queryMs === null ? [] : [s.queryMs])),
  };
}

export async function runColdStart(opts: {
  run: number;
  backend: string;
  tenantMode: string;
  tenantCount: number;
  samples: number;
  idleMs: number;
  warmRequests: number;
  timeoutMs: number;
  ops: (index: number) => OpQuery;
  exec: OpFn;
  /** Injectable idle wait for deterministic offline tests. */
  sleep?: (ms: number) => Promise<void>;
}): Promise<ColdStartRun> {
  const sleep = opts.sleep ?? ((ms) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const trials: ColdTrial[] = [];
  for (let i = 0; i < opts.samples; i++) {
    const op = opts.ops(i);
    if (op.method !== "GET" || op.test !== "point-read") throw new Error("cold-start requires a read-only point-read operation");
    console.log(`edge/${opts.backend}/cold-start run=${opts.run} sample=${i + 1}/${opts.samples}: idle ${opts.idleMs}ms`);
    const idle0 = performance.now();
    await sleep(opts.idleMs);
    const idleMs = performance.now() - idle0;
    const first = await execOne(opts.exec, op, opts.timeoutMs);
    const following: OpResult[] = [];
    // Same row + tenant within a trial: comparisons don't change the query.
    // Each new trial derives its own deterministic ID, shared across backends.
    for (let j = 0; j < opts.warmRequests; j++) following.push(await execOne(opts.exec, op, opts.timeoutMs));
    const firstSample = successful(first) ? first.sample : null;
    const firstLifecycle = firstSample?.lifecycle;
    const sameIsolate = following.filter(successful).map((r) => r.sample).filter((s) =>
      firstLifecycle != null && s.lifecycle != null && !s.lifecycle.firstRequest &&
      s.lifecycle.isolateId === firstLifecycle.isolateId && s.lifecycle.requestNumber > firstLifecycle.requestNumber,
    );
    const db = sameIsolate.flatMap((s) => s.dbMs === null ? [] : [s.dbMs]);
    trials.push({
      idleMs, first, following,
      e2eDeltaMs: firstSample !== null && sameIsolate.length > 0 ? firstSample.e2eMs - median(sameIsolate.map((s) => s.e2eMs)) : null,
      dbDeltaMs: firstSample?.dbMs != null && db.length > 0 ? firstSample.dbMs - median(db) : null,
    });
  }
  const firstResults = trials.map((t) => t.first);
  const lifecycles = firstResults.map((r) => r.ok ? r.sample.lifecycle : null);
  const e2eDeltas = trials.flatMap((t) => t.e2eDeltaMs === null ? [] : [t.e2eDeltaMs]);
  const dbDeltas = trials.flatMap((t) => t.dbDeltaMs === null ? [] : [t.dbDeltaMs]);
  return {
    run: opts.run, mode: "edge", backend: opts.backend, tenantMode: opts.tenantMode, tenantCount: opts.tenantCount,
    idleMs: opts.idleMs, warmRequests: opts.warmRequests,
    first: phase(firstResults), following: phase(trials.flatMap((t) => t.following)),
    firstOnNewIsolate: lifecycles.filter((l) => l?.firstRequest === true).length,
    firstOnReusedIsolate: lifecycles.filter((l) => l?.firstRequest === false).length,
    firstLifecycleUnknown: lifecycles.filter((l) => l == null).length,
    pairedTrials: e2eDeltas.length,
    medianE2eDeltaMs: e2eDeltas.length ? median(e2eDeltas) : null,
    medianDbDeltaMs: dbDeltas.length ? median(dbDeltas) : null,
    trials,
  };
}

export function coldStartCsv(runs: ColdStartRun[]): string {
  const header = ["backend", "run", "tenantMode", "tenantCount", "idleMs", "samples", "warmRequests", "firstSuccess", "firstErrors", "followingSuccess", "followingErrors", "firstE2eP50", "followingE2eP50", "firstDbP50", "followingDbP50", "newIsolate", "reusedIsolate", "unknownLifecycle", "pairedTrials", "pairedE2eDeltaMs", "pairedDbDeltaMs"];
  return [header.join(","), ...runs.map((r) => [
    r.backend, r.run, r.tenantMode, r.tenantCount, r.idleMs, r.trials.length, r.warmRequests,
    r.first.success, r.first.failed, r.following.success, r.following.failed,
    r.first.e2e?.p50 ?? "", r.following.e2e?.p50 ?? "", r.first.db?.p50 ?? "", r.following.db?.p50 ?? "",
    r.firstOnNewIsolate, r.firstOnReusedIsolate, r.firstLifecycleUnknown, r.pairedTrials,
    r.medianE2eDeltaMs ?? "", r.medianDbDeltaMs ?? "",
  ].join(","))].join("\n") + "\n";
}
