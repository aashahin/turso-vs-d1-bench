// Load execution: constant-concurrency worker pool (default) + legacy burst.
// Constant model: N virtual workers each start the next op as soon as the
// previous finishes, until the deadline expires or the op budget is reached
// (whichever comes first when both are set). Burst model: fixed batches joined
// with Promise.all, kept only for before/after comparison (--load-model=burst).
import { classifyError, mergeTx, summarize, type ClassifiedError, type ErrorClass, type Summary, type TxCounts, type TxStats } from "./stats.ts";
import type { OpQuery } from "./workloads/common.ts";

export interface OpSample {
  e2eMs: number;
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
}

export type OpFn = (op: OpQuery, signal: AbortSignal) => Promise<OpSample>;

export interface RunOutcome {
  e2e: Summary;
  db: Summary | null;
  query: Summary | null;
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
}

export interface RunOpts {
  label: string;
  ops: (index: number) => OpQuery;
  totalOps: number;
  durationSec: number; // 0 = iteration-based only
  concurrency: number;
  warmup: number;
  loadModel: "constant" | "burst";
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
    errorsByClass: { timeout: 0, http: 0, "rate-limit": 0, backend: 0, conflict: 0, unknown: 0 },
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

async function execOne(exec: OpFn, op: OpQuery, timeoutMs: number): Promise<{ ok: true; sample: OpSample } | { ok: false; err: ClassifiedError }> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(new DOMException(`op timed out after ${timeoutMs}ms`, "TimeoutError")), timeoutMs);
  try {
    const sample = await exec(op, ctrl.signal);
    return { ok: true, sample };
  } catch (e) {
    return { ok: false, err: classifyError(e) };
  } finally {
    clearTimeout(timer);
  }
}

export async function runScenario(opts: RunOpts): Promise<RunOutcome> {
  const { label, ops, totalOps, durationSec, concurrency, warmup, loadModel, timeoutMs, exec } = opts;

  for (let i = 0; i < warmup; i++) {
    await execOne(exec, ops(i), timeoutMs);
  }
  if (globalThis.gc) globalThis.gc();

  const e2e: number[] = [];
  const db: number[] = [];
  const query: number[] = [];
  let replicaHits = 0;
  let replicaKnown = 0;
  let replicaReads = 0;
  let primaryReads = 0;
  let totalReads = 0;
  const regions: Record<string, number> = {};
  const errorsByClass: Record<ErrorClass, number> = { timeout: 0, http: 0, "rate-limit": 0, backend: 0, conflict: 0, unknown: 0 };
  const errorsByStatus: Record<string, number> = {};
  const sampleErrors: string[] = [];
  const txCounts: TxCounts[] = [];
  let txFailedOps = 0;
  let engine: string | null = null;
  let transactionMode: string | null = null;
  let success = 0;
  let failed = 0;
  let done = 0;

  const wall0 = performance.now();
  const deadline = durationSec > 0 ? wall0 + durationSec * 1000 : Number.POSITIVE_INFINITY;
  let next = warmup;
  let lastLog = wall0;

  const claim = (): number | null => {
    if (performance.now() >= deadline) return null;
    if (next >= warmup + totalOps) return null;
    return next++;
  };

  const record = (r: { ok: true; sample: OpSample } | { ok: false; err: ClassifiedError }): void => {
    done++;
    if (r.ok) {
      const s = r.sample;
      if (s.engine !== null) engine = s.engine;
      // For mixed workloads, report the first transaction form that actually
      // ran (reads report "none"); all-read scenarios stay "none".
      if (s.transactionMode !== null && (transactionMode === null || (transactionMode === "none" && s.transactionMode !== "none"))) {
        transactionMode = s.transactionMode;
      }
      if (s.tx !== null) txCounts.push(s.tx);
      // A transaction that exhausted its retry budget is a real failure: it is
      // counted as one (conflict class), never silently dropped, and its
      // latency is excluded from the success percentiles.
      if (s.txFailed) {
        failed++;
        txFailedOps++;
        errorsByClass.conflict++;
        errorsByStatus.conflict = (errorsByStatus.conflict ?? 0) + 1;
        const msg = s.tx?.error ?? "concurrent transaction failed after retries";
        if (sampleErrors.length < 5) sampleErrors.push(msg.slice(0, 200));
      } else {
        success++;
        e2e.push(s.e2eMs);
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
      errorsByClass[r.err.errorClass]++;
      const key = r.err.httpStatus !== null ? `http-${r.err.httpStatus}` : r.err.errorClass;
      errorsByStatus[key] = (errorsByStatus[key] ?? 0) + 1;
      if (sampleErrors.length < 5) sampleErrors.push(r.err.message.slice(0, 200));
    }
    // Throttled progress from the hot path: no timer task, no tail latency.
    const now = performance.now();
    if (done === totalOps || now - lastLog > 2000) {
      lastLog = now;
      console.log(`${label}: ${done}/${totalOps} ok=${success} err=${failed}`);
    }
  };

  if (loadModel === "burst") {
    while (done < totalOps && performance.now() < deadline) {
      const batch: OpQuery[] = [];
      while (batch.length < concurrency) {
        const idx = claim();
        if (idx === null) break;
        batch.push(ops(idx));
      }
      if (batch.length === 0) break;
      const results = await Promise.all(batch.map((op) => execOne(exec, op, timeoutMs)));
      for (const r of results) record(r);
    }
  } else {
    const workers = Array.from({ length: Math.max(1, concurrency) }, async () => {
      for (;;) {
        const idx = claim();
        if (idx === null) return;
        record(await execOne(exec, ops(idx), timeoutMs));
      }
    });
    await Promise.all(workers);
  }
  console.log(`${label}: ${done}/${totalOps} ok=${success} err=${failed}`);

  const wallMs = performance.now() - wall0;
  if (success === 0) {
    return { ...emptyOutcome(wallMs), failed, errorRate: 1, errorsByClass, errorsByStatus, sampleErrors, engine, transactionMode, tx: mergeTx(txCounts), wallMs };
  }
  return {
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
