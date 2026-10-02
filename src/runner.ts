import {
  arrivals,
  type ArrivalMetrics,
  type SchedulerClock,
} from "./scheduler.ts";
import type { PoolMetrics, PoolObservation } from "../worker/src/pool.ts";
import { concentration } from "./workloads/distribution.ts";
// Load execution: constant-concurrency worker pool (default) + legacy burst.
// Constant model: N virtual workers each start the next op as soon as the
// previous finishes, until the deadline expires or the op budget is reached
// (whichever comes first when both are set). Burst model: fixed batches joined
// with Promise.all, kept only for before/after comparison (--load-model=burst).
import {
  classifyError,
  mergeTx,
  summarize,
  type ClassifiedError,
  type ErrorClass,
  type Summary,
  type TxCounts,
  type TxStats,
} from "./stats.ts";
import type { OpQuery } from "./workloads/common.ts";
import type { WorkerLifecycle } from "../shared/lifecycle.ts";

export interface OpSample {
  timingQuality?: unknown;
  deploymentVersion?: string;
  sourceFingerprint?: string;
  e2eMs: number;
  timings?: Record<string, number | null>;
  pool?: PoolObservation | null;
  poolMetrics?: PoolMetrics | null;
  requestedTenant?: number;
  databaseIdentity?: string;
  tenantIsolation?: string;
  colo?: string | null;
  queries?: number;
  rowsRead?: number | null;
  rowsWritten?: number | null;
  dbMs: number | null;
  queryMs: number | null;
  buildMs: number | null;
  clientCreationMs: number | null;
  clientCloseMs: number | null;
  checkoutMs: number | null;
  replica: boolean | null;
  region: string | null;
  replicaReads: number;
  primaryReads: number;
  totalReads: number;
  /** Engine reported by the Worker (d1 | libsql | tursodb), null in direct mode. */
  engine: string | null;
  /** Transaction form actually executed (none | transaction | concurrent). */
  transactionMode: string | null;
  /** Server-side transaction counters; null for backends without transactions. */
  tx: TxCounts | null;
  /** True when a concurrent transaction exhausted its retry budget. */
  txFailed: boolean;
  /** Absent when an older Worker or direct mode cannot report lifecycle. */
  lifecycle?: WorkerLifecycle | null;
}

export type OpFn = (op: OpQuery, signal: AbortSignal) => Promise<OpSample>;

export interface RunOutcome {
  e2e: Summary;
  db: Summary | null;
  query: Summary | null;
  offeredFailureRate?: number;
  success: number;
  failed: number;
  errorRate: number;
  errorsByClass: Record<ErrorClass, number>;
  errorsByStatus: Record<string, number>;
  sampleErrors: string[];
  replicaRate: number | null;
  replicaReads: number;
  primaryReads: number;
  totalReads: number;
  regions: Record<string, number>;
  engine: string | null;
  transactionMode: string | null;
  tx: TxStats | null;
  wallMs: number;
  rps: number;
  errorsByTimeoutClass?: Record<string, number>;
  arrival?: Omit<ArrivalMetrics, "queueDelayMs"> & { queueDelay: Summary };
  scheduledLatency?: Summary | null;
  timings?: Record<string, Summary>;
  operationCounts?: Record<string, number>;
  tenantRequests?: ReturnType<typeof concentration>;
  pool?: {
    events: Record<string, number>;
    workerSnapshots: Record<string, PoolMetrics>;
    reuseRate: number | null;
    createdPerSec: number;
    evictionsPerSec: number;
    wait: Summary | null;
  };
  startedOperationCounts?: Record<string, number>;
  mixValidation?: unknown;
  lifecycleIds?: string[];
  colos?: Record<string, number>;
  cost?: {
    requests: number;
    queries: number;
    rowsRead: number | null;
    rowsWritten: number | null;
  };
  warnings?: string[];
}

export interface RunOpts {
  label: string;
  ops: (index: number) => OpQuery;
  totalOps: number;
  durationSec: number; // 0 = iteration-based only
  concurrency: number;
  warmup: number;
  loadModel: "constant" | "closed-loop" | "arrival-rate" | "burst";
  operationMix?: Record<string, number>;
  targetRps?: number;
  maxInFlight?: number;
  maxStartLagMs?: number;
  clock?: SchedulerClock;
  tenantCount?: number;
  timeoutMs: number;
  exec: OpFn;
}

function emptyOutcome(wallMs: number): RunOutcome {
  const zero = summarize([], wallMs);
  return {
    e2e: zero,
    db: null,
    query: null,
    success: 0,
    failed: 0,
    errorRate: 0,
    errorsByClass: {
      timeout: 0,
      http: 0,
      "rate-limit": 0,
      backend: 0,
      conflict: 0,
      unknown: 0,
      connection: 0,
      constraint: 0,
      application: 0,
    },
    errorsByStatus: {},
    sampleErrors: [],
    replicaRate: null,
    replicaReads: 0,
    primaryReads: 0,
    totalReads: 0,
    regions: {},
    engine: null,
    transactionMode: null,
    tx: null,
    wallMs,
    rps: 0,
  };
}

export type OpResult =
  { ok: true; sample: OpSample } | { ok: false; err: ClassifiedError };

export async function execOne(
  exec: OpFn,
  op: OpQuery,
  timeoutMs: number,
): Promise<OpResult> {
  const ctrl = new AbortController();
  let timer!: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      const error = new DOMException(
        `op timed out after ${timeoutMs}ms`,
        "TimeoutError",
      );
      reject(error);
      ctrl.abort(error);
    }, timeoutMs);
  });
  try {
    // Aborting a signal alone cannot bound an executor that ignores it. Racing
    // also observes late rejections, without counting a late result twice.
    const sample = await Promise.race([exec(op, ctrl.signal), deadline]);
    return { ok: true, sample };
  } catch (e) {
    return { ok: false, err: classifyError(e) };
  } finally {
    clearTimeout(timer);
  }
}

export async function runScenario(opts: RunOpts): Promise<RunOutcome> {
  const {
    label,
    ops,
    totalOps,
    durationSec,
    concurrency,
    warmup,
    loadModel,
    timeoutMs,
    exec,
  } = opts;

  for (let i = 0; i < warmup; i++) {
    await execOne(exec, ops(i), timeoutMs);
  }
  if (globalThis.gc) globalThis.gc();

  const scheduledLatency: number[] = [];
  const timingSamples: Record<string, number[]> = {};
  const operationCounts: Record<string, number> = {};
  const startedOperationCounts: Record<string, number> = {};
  const tenantCounts: Record<string, number> = {};
  const poolEvents: Record<string, number> = {};
  const workerSnapshots: Record<string, PoolMetrics> = {};
  const poolWait: number[] = [];
  const lifecycleIds = new Set<string>();
  const colos: Record<string, number> = {};
  let queryCount = 0;
  let rowsRead: number | null = null;
  let rowsWritten: number | null = null;
  const offered = (op: OpQuery, started = true) => {
    if (started)
      startedOperationCounts[op.test] =
        (startedOperationCounts[op.test] ?? 0) + 1;
    operationCounts[op.test] = (operationCounts[op.test] ?? 0) + 1;
    const t = String(op.params.tenant ?? 1);
    tenantCounts[t] = (tenantCounts[t] ?? 0) + 1;
  };
  const e2e: number[] = [];
  const db: number[] = [];
  const query: number[] = [];
  let replicaHits = 0;
  let replicaKnown = 0;
  let replicaReads = 0;
  let primaryReads = 0;
  let totalReads = 0;
  const regions: Record<string, number> = {};
  const errorsByClass: Record<ErrorClass, number> = {
    timeout: 0,
    http: 0,
    "rate-limit": 0,
    backend: 0,
    conflict: 0,
    unknown: 0,
    connection: 0,
    constraint: 0,
    application: 0,
  };
  const errorsByStatus: Record<string, number> = {};
  const sampleErrors: string[] = [];
  const txCounts: TxCounts[] = [];
  let engine: string | null = null;
  let transactionMode: string | null = null;
  let success = 0;
  let failed = 0;
  let topologyInvalid = false;
  let done = 0;

  const wall0 = performance.now();
  const deadline =
    durationSec > 0 ? wall0 + durationSec * 1000 : Number.POSITIVE_INFINITY;
  let next = warmup;
  let lastLog = wall0;

  const claim = (): number | null => {
    if (performance.now() >= deadline) return null;
    if (next >= warmup + totalOps) return null;
    return next++;
  };

  const recordObservation = (s: Partial<OpSample>) => {
    if (s.lifecycle) lifecycleIds.add(s.lifecycle.isolateId);
    if (s.colo) colos[s.colo] = (colos[s.colo] ?? 0) + 1;
    queryCount += s.queries ?? 0;
    if (s.rowsRead != null) rowsRead = (rowsRead ?? 0) + s.rowsRead;
    if (s.rowsWritten != null) rowsWritten = (rowsWritten ?? 0) + s.rowsWritten;
    if (s.poolMetrics) {
      const isolate = s.lifecycle?.isolateId ?? "unknown";
      const prior = workerSnapshots[isolate];
      if (
        !prior ||
        (s.poolMetrics.snapshotSequence ?? 0) >= (prior.snapshotSequence ?? 0)
      )
        workerSnapshots[isolate] = s.poolMetrics;
    }
    if (s.pool) {
      for (const [k, v] of Object.entries(s.pool.events))
        poolEvents[k] = (poolEvents[k] ?? 0) + v;
      poolWait.push(s.pool.checkoutMs);
    }
  };
  const errorsByTimeoutClass: Record<string, number> = {};
  const record = (r: OpResult, queueDelay = 0): void => {
    done++;
    if (r.ok) {
      const s = r.sample;
      recordObservation(s);
      if (s.engine !== null) engine = s.engine;
      // For mixed workloads, report the first transaction form that actually
      // ran (reads report "none"); all-read scenarios stay "none".
      if (
        s.transactionMode !== null &&
        (transactionMode === null ||
          (transactionMode === "none" && s.transactionMode !== "none"))
      ) {
        transactionMode = s.transactionMode;
      }
      if (s.tx !== null) txCounts.push(s.tx);
      // A transaction that exhausted its retry budget is a real failure: it is
      // counted as one (conflict class), never silently dropped, and its
      // latency is excluded from the success percentiles.
      if (s.txFailed) {
        failed++;
        errorsByClass.conflict++;
        errorsByStatus.conflict = (errorsByStatus.conflict ?? 0) + 1;
        const msg =
          s.tx?.error ?? "concurrent transaction failed after retries";
        if (sampleErrors.length < 5) sampleErrors.push(msg.slice(0, 200));
      } else {
        success++;
        e2e.push(s.e2eMs);
        scheduledLatency.push(s.e2eMs + queueDelay);
        for (const [k, v] of Object.entries(s.timings ?? {}))
          if (v !== null && Number.isFinite(v) && v >= 0)
            (timingSamples[k] ??= []).push(v);
        if (s.dbMs !== null) db.push(s.dbMs);
        if (s.queryMs !== null) query.push(s.queryMs);
        if (s.replica !== null) {
          replicaKnown++;
          if (s.replica) replicaHits++;
        }
      }
      replicaReads += s.replicaReads;
      primaryReads += s.primaryReads;
      totalReads += s.totalReads;
      if (s.region) regions[s.region] = (regions[s.region] ?? 0) + 1;
    } else {
      failed++;
      if (r.err.message.includes("TOPOLOGY_INVALID")) topologyInvalid = true;
      if (r.err.evidence)
        recordObservation(r.err.evidence as Partial<OpSample>);
      if (r.err.errorClass === "timeout") {
        const phase = r.err.timeoutClass ?? "runner-operation";
        errorsByTimeoutClass[phase] = (errorsByTimeoutClass[phase] ?? 0) + 1;
      }
      errorsByClass[r.err.errorClass]++;
      const key =
        r.err.httpStatus !== null
          ? `http-${r.err.httpStatus}`
          : r.err.errorClass;
      errorsByStatus[key] = (errorsByStatus[key] ?? 0) + 1;
      if (sampleErrors.length < 5)
        sampleErrors.push(r.err.message.slice(0, 200));
    }
    // Throttled progress from the hot path: no timer task, no tail latency.
    const now = performance.now();
    if (done === totalOps || now - lastLog > 2000) {
      lastLog = now;
      console.log(`${label}: ${done}/${totalOps} ok=${success} err=${failed}`);
    }
  };

  let arrival: ArrivalMetrics | undefined;
  if (loadModel === "arrival-rate") {
    const rps = opts.targetRps ?? 10;
    const total =
      durationSec > 0
        ? Math.min(totalOps, Math.ceil(durationSec * rps))
        : totalOps;
    arrival = await arrivals({
      total,
      rps,
      maxInFlight: opts.maxInFlight ?? 1000,
      maxStartLagMs: opts.maxStartLagMs ?? 1000,
      clock: opts.clock,
      execute: async (index, delay) => {
        const op = ops(warmup + index);
        offered(op);
        record(await execOne(exec, op, timeoutMs), delay);
      },
      onDrop: (index) => offered(ops(warmup + index), false),
    });
  } else if (loadModel === "burst") {
    while (done < totalOps && performance.now() < deadline) {
      const batch: OpQuery[] = [];
      while (batch.length < concurrency) {
        const idx = claim();
        if (idx === null) break;
        const op = ops(idx);
        offered(op);
        batch.push(op);
      }
      if (batch.length === 0) break;
      const results = await Promise.all(
        batch.map((op) => execOne(exec, op, timeoutMs)),
      );
      for (const r of results) record(r);
    }
  } else {
    const workers = Array.from(
      { length: Math.max(1, concurrency) },
      async () => {
        for (;;) {
          const idx = claim();
          if (idx === null) return;
          const op = ops(idx);
          offered(op);
          record(await execOne(exec, op, timeoutMs));
        }
      },
    );
    await Promise.all(workers);
  }
  console.log(`${label}: ${done}/${totalOps} ok=${success} err=${failed}`);

  const wallMs = performance.now() - wall0;
  const warnings: string[] = [];
  if (topologyInvalid) warnings.push("TOPOLOGY_INVALID");
  if (
    durationSec > 0 &&
    (arrival
      ? arrival.offeredWindowMs < durationSec * 1000
      : done >= totalOps && wallMs < durationSec * 1000)
  )
    warnings.push("BUDGET_LIMITED_WINDOW");
  if (success < 1000) warnings.push("INSUFFICIENT_SAMPLES");
  if (failed / Math.max(1, success + failed) >= 0.01)
    warnings.push("HIGH_ERROR_RATE");
  if (
    arrival &&
    (arrival.dropped > 0 ||
      arrival.lateStarts / Math.max(1, arrival.started) > 0.01)
  )
    warnings.push("OFFERED_LOAD_NOT_ACHIEVED");
  if (poolWait.some((t) => t > timeoutMs * 0.1))
    warnings.push("POOL_SATURATED");
  if (lifecycleIds.size > 1) warnings.push("WORKER_LIFECYCLE_CHANGED");
  const offeredTotal = Object.values(operationCounts).reduce(
    (a, b) => a + b,
    0,
  );
  const mixValidation = opts.operationMix
    ? {
        requestedPercent: opts.operationMix,
        offeredCounts: operationCounts,
        startedCounts: startedOperationCounts,
        observedPercent: Object.fromEntries(
          Object.entries(operationCounts).map(([k, n]) => [
            k,
            (100 * n) / Math.max(1, offeredTotal),
          ]),
        ),
        completeBlocks: Math.floor(offeredTotal / 100),
        partialBlockRequests: offeredTotal % 100,
      }
    : null;
  const extra = {
    startedOperationCounts,
    mixValidation,
    offeredFailureRate:
      (failed + (arrival?.dropped ?? 0)) /
      Math.max(1, success + failed + (arrival?.dropped ?? 0)),
    errorsByTimeoutClass,
    arrival: arrival
      ? {
          ...arrival,
          queueDelayMs: undefined,
          queueDelay: summarize(arrival.queueDelayMs, wallMs),
        }
      : undefined,
    scheduledLatency: scheduledLatency.length
      ? summarize(scheduledLatency, wallMs)
      : null,
    timings: Object.fromEntries(
      Object.entries(timingSamples).map(([k, v]) => [k, summarize(v, wallMs)]),
    ),
    operationCounts,
    tenantRequests: concentration(
      tenantCounts,
      opts.tenantCount ?? Object.keys(tenantCounts).length,
    ),
    pool: {
      events: poolEvents,
      workerSnapshots,
      reuseRate:
        (poolEvents.connectionCreated ?? 0) +
          (poolEvents.connectionReused ?? 0) >
        0
          ? (poolEvents.connectionReused ?? 0) /
            ((poolEvents.connectionCreated ?? 0) +
              (poolEvents.connectionReused ?? 0))
          : null,
      createdPerSec:
        (poolEvents.connectionCreated ?? 0) / Math.max(0.001, wallMs / 1000),
      evictionsPerSec:
        (poolEvents.poolEviction ?? 0) / Math.max(0.001, wallMs / 1000),
      wait: poolWait.length ? summarize(poolWait, wallMs) : null,
    },
    lifecycleIds: [...lifecycleIds],
    colos,
    cost: {
      requests: success + failed,
      queries: queryCount,
      rowsRead,
      rowsWritten,
    },
    warnings,
  };
  if (success === 0) {
    return {
      ...emptyOutcome(wallMs),
      ...extra,
      failed,
      errorRate: failed > 0 ? 1 : 0,
      errorsByClass,
      errorsByStatus,
      sampleErrors,
      engine,
      transactionMode,
      tx: mergeTx(txCounts),
      replicaReads,
      primaryReads,
      totalReads,
      regions,
      wallMs,
    };
  }
  return {
    ...extra,
    e2e: summarize(e2e, wallMs),
    db: db.length > 0 ? summarize(db, wallMs) : null,
    query: query.length > 0 ? summarize(query, wallMs) : null,
    success,
    failed,
    errorRate: failed / (success + failed),
    errorsByClass,
    errorsByStatus,
    sampleErrors,
    replicaRate: replicaKnown > 0 ? replicaHits / replicaKnown : null,
    replicaReads,
    primaryReads,
    totalReads,
    regions,
    engine,
    transactionMode,
    tx: mergeTx(txCounts),
    wallMs,
    rps: success / (wallMs / 1000),
  };
}
