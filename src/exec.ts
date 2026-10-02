import { scenarioSql, type OpParams, type ScenarioSql } from "../shared/sql.ts";
import { BenchError } from "../shared/policy.ts";
import type { PoolMetrics, PoolObservation } from "../worker/src/pool.ts";
// Operation executors: edge (through the deployed Worker, the only fair
// comparison path for D1) and direct (bun -> Turso Cloud, reference only).
// Direct mode is informational: it measures client-to-cloud latency, which is
// not comparable with edge-terminated D1 traffic.
import { connect } from "@tursodatabase/serverless";
import { TX_WRITE_TESTS } from "../shared/backend-tests.ts";
import type { OpSample, OpFn } from "./runner.ts";
import type { TxCounts } from "./stats.ts";
import type { OpQuery } from "./workloads/common.ts";
import { parseLifecycle } from "../shared/lifecycle.ts";

interface EdgeBody {
  timingQuality?: unknown;
  deploymentVersion?: string;
  sourceFingerprint?: string;
  dbMs: number;
  pool?: PoolObservation;
  poolMetrics?: PoolMetrics;
  workerHandlerMs?: number;
  tenantRoutingMs?: number;
  dbExecutionMs?: number;
  requestedTenant?: number;
  databaseIdentity?: string;
  tenantIsolation?: string;
  colo?: string;
  queries?: number;
  rowsRead?: number;
  rowsWritten?: number;
  queryMs?: number | null;
  buildMs?: number | null;
  clientCreationMs?: number | null;
  clientCloseMs?: number | null;
  checkoutMs?: number | null;
  servedByPrimary?: boolean | null;
  servedByRegion?: string | null;
  replicaReads?: number;
  primaryReads?: number;
  totalReads?: number;
  engine?: string | null;
  transactionMode?: string | null;
  tx?: TxCounts | null;
  txFailed?: boolean;
  error?: string;
  lifecycle?: unknown;
}

/** Server-side transaction counters as reported by the Worker. */
function txOf(raw: unknown): TxCounts | null {
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  const n = (k: string): number =>
    typeof o[k] === "number" ? (o[k] as number) : 0;
  return {
    attempted: n("attempted"),
    attemptsTotal: n("attemptsTotal"),
    committed: n("committed"),
    successAfterRetry: n("successAfterRetry"),
    failedAfterRetries: n("failedAfterRetries"),
    conflicts: n("conflicts"),
    retries: n("retries"),
    error: typeof o.error === "string" ? o.error : null,
  };
}

export function makeEdgeExec(
  workerUrl: string,
  adminToken: string,
  backend: string,
  timeoutMs = 5000,
): OpFn {
  return async (op: OpQuery, signal: AbortSignal): Promise<OpSample> => {
    const t0 = performance.now();
    const qs = new URLSearchParams();
    qs.set("timeoutMs", String(timeoutMs));
    const body: Record<string, number | string> = {};
    for (const [k, v] of Object.entries(op.params)) {
      if (op.method === "GET") qs.set(k, String(v));
      else {
        body[k] = v;
        if (k === "tenant" || k === "id" || k === "startId" || k === "limit")
          qs.set(k, String(v));
      }
    }
    const res = await fetch(`${workerUrl}/bench/${backend}/${op.test}?${qs}`, {
      method: op.method,
      headers: {
        ...(adminToken ? { authorization: `Bearer ${adminToken}` } : {}),
        ...(op.method === "POST" ? { "content-type": "application/json" } : {}),
      },
      body: op.method === "POST" ? JSON.stringify(body) : undefined,
      signal,
    });
    const text = await res.text();
    if (!res.ok) {
      let error: {
        errorClass?: string;
        timeoutClass?: string;
        error?: string;
        evidence?: Record<string, unknown>;
      } = {};
      try {
        error = JSON.parse(text);
      } catch {}
      if (error.errorClass) {
        const failure = new BenchError(
          error.errorClass,
          error.error ?? "backend error",
          error.timeoutClass ?? null,
        );
        failure.evidence = error.evidence;
        failure.httpStatus = res.status;
        throw failure;
      }
      throw new Error(`edge ${backend}/${op.test}: HTTP ${res.status}`);
    }
    const parsed = JSON.parse(text) as EdgeBody;
    if (
      typeof parsed.dbMs !== "number" ||
      !Number.isFinite(parsed.dbMs) ||
      parsed.dbMs < 0
    )
      throw new Error(`edge ${backend}/${op.test}: bad response`);
    const replica =
      parsed.servedByPrimary === false
        ? true
        : parsed.servedByPrimary === true
          ? false
          : null;
    return {
      timingQuality: parsed.timingQuality,
      deploymentVersion: parsed.deploymentVersion,
      sourceFingerprint: parsed.sourceFingerprint,
      pool: parsed.pool,
      poolMetrics: parsed.poolMetrics,
      requestedTenant: parsed.requestedTenant,
      databaseIdentity: parsed.databaseIdentity,
      tenantIsolation: parsed.tenantIsolation,
      colo: parsed.colo,
      queries: parsed.queries,
      rowsRead: parsed.rowsRead,
      rowsWritten: parsed.rowsWritten,
      timings: {
        workerHandlerMs: parsed.workerHandlerMs ?? null,
        tenantRoutingMs: parsed.tenantRoutingMs ?? null,
        poolCheckoutMs: parsed.checkoutMs ?? null,
        connectionCreationMs: parsed.clientCreationMs ?? null,
        dbExecutionMs: parsed.queryMs ?? null,
        totalServerMs: res.headers.has("X-Bench-Total-Server-Ms")
          ? Number(res.headers.get("X-Bench-Total-Server-Ms"))
          : null,
        serializationMs: res.headers.has("X-Bench-Serialization-Ms")
          ? Number(res.headers.get("X-Bench-Serialization-Ms"))
          : null,
        runnerToEdgeMs: null,
      },
      e2eMs: performance.now() - t0,
      dbMs: parsed.dbMs,
      queryMs: typeof parsed.queryMs === "number" ? parsed.queryMs : null,
      buildMs: typeof parsed.buildMs === "number" ? parsed.buildMs : null,
      clientCreationMs:
        typeof parsed.clientCreationMs === "number"
          ? parsed.clientCreationMs
          : null,
      clientCloseMs:
        typeof parsed.clientCloseMs === "number" ? parsed.clientCloseMs : null,
      checkoutMs:
        typeof parsed.checkoutMs === "number" ? parsed.checkoutMs : null,
      replica,
      region:
        typeof parsed.servedByRegion === "string"
          ? parsed.servedByRegion
          : null,
      replicaReads:
        typeof parsed.replicaReads === "number" ? parsed.replicaReads : 0,
      primaryReads:
        typeof parsed.primaryReads === "number" ? parsed.primaryReads : 0,
      totalReads: typeof parsed.totalReads === "number" ? parsed.totalReads : 0,
      engine: typeof parsed.engine === "string" ? parsed.engine : null,
      transactionMode:
        typeof parsed.transactionMode === "string"
          ? parsed.transactionMode
          : null,
      tx: txOf(parsed.tx),
      txFailed: parsed.txFailed === true,
      lifecycle: parseLifecycle(parsed.lifecycle),
    };
  };
}

// Direct reference uses the same SQL registry as edge execution.
function directScenario(
  test: string,
  params: Record<string, number | string>,
): ScenarioSql {
  const defaults: OpParams = {
    id: 1,
    startId: 1,
    limit: 100,
    tenant: 1,
    student: 1,
    course: 1,
    lesson: 1,
    quiz: 1,
    question: 1,
    attempt: 1,
    position: 1,
    payload: "",
    answer: "B",
    completed: 1,
    retries: 0,
  };
  return scenarioSql(test, { ...defaults, ...params } as OpParams);
}
interface DirectSql {
  reads: { text: string; params: unknown[]; many: boolean }[];
  writes: { text: string; params: unknown[] }[];
}

interface DirectRunner {
  get(text: string, params: unknown[]): Promise<unknown>;
  all(text: string, params: unknown[]): Promise<unknown[]>;
  run(text: string, params: unknown[]): Promise<unknown>;
}

const directAbort = (): never => {
  throw new DOMException("op timed out", "TimeoutError");
};

async function directExecScenario(
  r: DirectRunner,
  sc: DirectSql,
  signal: AbortSignal,
): Promise<void> {
  for (const q of sc.reads) {
    if (signal.aborted) directAbort();
    if (q.many) await r.all(q.text, q.params);
    else await r.get(q.text, q.params);
  }
  for (const q of sc.writes) {
    if (signal.aborted) directAbort();
    if (q.text.startsWith("SELECT")) await r.get(q.text, q.params);
    else await r.run(q.text, q.params);
  }
}

export function makeDirectExec(
  tursoUrl: string,
  tursoToken: string,
  timeoutMs = 5000,
): OpFn {
  const url = tursoUrl.replace(/^libsql:\/\//, "https://");
  return async (op: OpQuery, signal: AbortSignal): Promise<OpSample> => {
    const t0 = performance.now();
    if (signal.aborted) throw new DOMException("op timed out", "TimeoutError");
    const c0 = performance.now();
    const conn = connect({
      url,
      authToken: tursoToken,
      defaultQueryTimeout: timeoutMs,
    });
    const clientCreationMs = performance.now() - c0;

    try {
      const q0 = performance.now();
      let sc = directScenario(op.test, op.params);
      if (op.test === "submit-quiz-answer" && op.params.attempt === 0) {
        type AttemptRow = { id: number };
        const attemptRow = (await conn.get(
          "SELECT id FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1",
          op.params.tenant as number,
          op.params.quiz as number,
          op.params.student as number,
        )) as AttemptRow | undefined | null;
        type InsertInfo = { lastInsertRowid?: string | number | bigint };
        const insertInfo = (
          attemptRow
            ? undefined
            : await conn.run(
                "INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, state) VALUES (?, ?, ?, 'started')",
                op.params.tenant as number,
                op.params.quiz as number,
                op.params.student as number,
              )
        ) as InsertInfo | undefined;
        const attemptId = attemptRow?.id ?? Number(insertInfo?.lastInsertRowid);
        sc = directScenario(op.test, { ...op.params, attempt: attemptId });
      }
      // Direct mode is a libSQL reference path: transactional write tests run
      // as one atomic BEGIN IMMEDIATE batch (same form as the Worker paths).
      if (TX_WRITE_TESTS[op.test] === true) {
        const stmts = [
          ...sc.reads.map((q) => ({ sql: q.text, args: q.params })),
          ...sc.writes.map((q) => ({ sql: q.text, args: q.params })),
        ];
        await conn.batch(stmts, "immediate");
      } else {
        await directExecScenario(
          {
            get: (text, params) => conn.get(text, ...params),
            all: (text, params) => conn.all(text, ...params),
            run: (text, params) => conn.run(text, ...params),
          },
          sc,
          signal,
        );
      }
      const queryMs = performance.now() - q0;
      const x0 = performance.now();
      await conn.close();
      const clientCloseMs = performance.now() - x0;
      const total = performance.now() - t0;
      return {
        e2eMs: total,
        dbMs: queryMs + clientCreationMs + clientCloseMs,
        queryMs,
        buildMs: null,
        clientCreationMs,
        clientCloseMs,
        checkoutMs: null,
        replica: null,
        region: null,
        replicaReads: 0,
        primaryReads: 0,
        totalReads: 0,
        engine: "libsql",
        transactionMode:
          TX_WRITE_TESTS[op.test] === true ? "transaction" : "none",
        tx: null,
        txFailed: false,
      };
    } finally {
      await conn.close();
    }
  };
}
