import {
  scenarioSql,
  type OpParams,
  type ScenarioSql,
} from "../../shared/sql.ts";
import { meteredD1, meteredTurso, type QueryCounters } from "./cost.ts";
// Equivalent query implementations for every backend. Fairness rules:
// identical SQL text and bind order on D1 and Turso wherever the SQLite
// dialect permits; multi-query LMS scenarios run the same sequence on both;
// Responses carry counts/instrumentation; DB row decoding is timed separately.
import { backendSpec } from "../../shared/topology.ts";
import { externalConfig } from "./validation.ts";
import { sql } from "drizzle-orm";
import {
  freshTursoConn,
  tenantManager,
  poolConfig,
  txModeOf,
  type BackendName,
  type Env,
  type TursoClient,
} from "./backends.ts";
import type { TenantDbKind } from "./tenant.ts";

export type { OpParams } from "../../shared/sql.ts";
export interface OpTimings {
  dbMs: number;
  queryMs: number;
  buildMs: number | null;
  clientCreationMs: number | null;
  clientCloseMs: number | null;
  checkoutMs: number | null;
  totalDbMs: number;
  servedByPrimary: boolean | null;
  servedByRegion: string | null;
  replicaReads: number;
  primaryReads: number;
  totalReads: number;
  tenantDb: TenantDbKind;
  orm: "raw" | "drizzle";
}

export interface OpOutcome {
  t: OpTimings;
  extra: Record<string, unknown>;
}

function baseTimings(
  tenantDb: TenantDbKind,
  orm: "raw" | "drizzle",
): OpTimings {
  return {
    dbMs: 0,
    queryMs: 0,
    buildMs: null,
    clientCreationMs: null,
    clientCloseMs: null,
    checkoutMs: null,
    totalDbMs: 0,
    servedByPrimary: null,
    servedByRegion: null,
    replicaReads: 0,
    primaryReads: 0,
    totalReads: 0,
    tenantDb,
    orm,
  };
}

function metaOf(rs: unknown): {
  primary: boolean | null;
  region: string | null;
} {
  const meta = (rs as { meta?: Record<string, unknown> } | null)?.meta;
  if (!meta) return { primary: null, region: null };
  const p = meta.served_by_primary;
  const r = meta.served_by_region;
  return {
    primary: typeof p === "boolean" ? p : null,
    region: typeof r === "string" ? r : null,
  };
}

// Drizzle path composes the identical statement through drizzle-orm's `sql`
// template so builder overhead is measured (buildMs); execution goes through
// the same underlying client as the raw counterpart. This is the honest
// subset: it captures query-composition cost, not the relational mapper.
function drizzleText(text: string): { text: string; buildMs: number } {
  const t0 = performance.now();
  // Compose through drizzle-orm's `sql` tag so template overhead is measured;
  // the statement text stays identical to the raw path.
  const chunks = text.split("?");
  const q =
    chunks.length > 1
      ? sql.join(
          chunks.map((c) => sql.raw(c)),
          sql.raw("?"),
        )
      : sql.raw(text);
  void q;
  return { text, buildMs: performance.now() - t0 };
}
// ---- D1 executors -----------------------------------------------------------

async function d1Get(
  db: D1Database,
  text: string,
  params: unknown[],
): Promise<unknown> {
  return db
    .prepare(text)
    .bind(...params)
    .first();
}

async function d1All(
  db: D1Database,
  text: string,
  params: unknown[],
): Promise<{ rows: unknown[] }> {
  const rs = await db
    .prepare(text)
    .bind(...params)
    .all();
  return { rows: rs.results };
}

async function d1Run(
  db: D1Database,
  text: string,
  params: unknown[],
): Promise<unknown> {
  return db
    .prepare(text)
    .bind(...params)
    .run();
}

interface RRTracker {
  session: D1Database;
  replicaReads: number;
  primaryReads: number;
  totalReads: number;
  lastPrimary: boolean | null;
  lastRegion: string | null;
}

function rrTracker(db: D1Database): RRTracker {
  return {
    session: db.withSession(),
    replicaReads: 0,
    primaryReads: 0,
    totalReads: 0,
    lastPrimary: null,
    lastRegion: null,
  };
}

async function rrAll(
  tr: RRTracker,
  text: string,
  params: unknown[],
): Promise<{ rows: unknown[] }> {
  const rs = await tr.session
    .prepare(text)
    .bind(...params)
    .all();
  const { primary, region } = metaOf(rs);
  tr.totalReads++;
  if (primary === false) tr.replicaReads++;
  else if (primary === true) tr.primaryReads++;
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  return { rows: rs.results };
}

async function rrGet(
  tr: RRTracker,
  text: string,
  params: unknown[],
): Promise<unknown> {
  const rs = await tr.session
    .prepare(text)
    .bind(...params)
    .all();
  const { primary, region } = metaOf(rs);
  tr.totalReads++;
  if (primary === false) tr.replicaReads++;
  else if (primary === true) tr.primaryReads++;
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  const rows = (rs as unknown as { results: unknown[] }).results;
  return rows[0] ?? null;
}

async function rrRun(
  tr: RRTracker,
  text: string,
  params: unknown[],
): Promise<unknown> {
  const rs = await tr.session
    .prepare(text)
    .bind(...params)
    .run();
  const { primary, region } = metaOf(rs);
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  return rs;
}

// ---- Scenario SQL (shared text both backends) -------------------------------

// ---- Attempt resolution (submit-quiz-answer with attempt=0) -------------------
// Attempt rowids are autoincrement and cannot be derived arithmetically, so
// the runner passes attempt=0 and the Worker resolves the latest attempt for
// (tenant, quiz, student), creating one if none exists. This runs inside the
// timed region: real apps do this lookup too.

async function d1ResolveAttempt(
  db: D1Database,
  key: { tenant: number; quiz: number; student: number },
  signal?: AbortSignal,
): Promise<number> {
  const { tenant, quiz, student } = key;
  const row = await db
    .prepare(
      "SELECT id FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(tenant, quiz, student)
    .first<{ id: number }>();
  if (signal?.aborted) throw new DOMException("request aborted", "AbortError");
  if (row) return row.id;
  const rs = await db
    .prepare(
      "INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, state) VALUES (?, ?, ?, 'started')",
    )
    .bind(tenant, quiz, student)
    .run();
  return Number(rs.meta.last_row_id);
}

async function rrResolveAttempt(
  tr: RRTracker,
  key: { tenant: number; quiz: number; student: number },
  signal?: AbortSignal,
): Promise<number> {
  const { tenant, quiz, student } = key;
  const rs = await tr.session
    .prepare(
      "SELECT id FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(tenant, quiz, student)
    .all<{ id: number }>();
  const { primary, region } = metaOf(rs);
  tr.totalReads++;
  if (primary === false) tr.replicaReads++;
  else if (primary === true) tr.primaryReads++;
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  if (signal?.aborted) throw new DOMException("request aborted", "AbortError");
  if (rs.results[0]) return rs.results[0].id;
  const ins = await tr.session
    .prepare(
      "INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, state) VALUES (?, ?, ?, 'started')",
    )
    .bind(tenant, quiz, student)
    .run();
  tr.lastPrimary = metaOf(ins).primary;
  tr.lastRegion = metaOf(ins).region;
  return Number(ins.meta.last_row_id);
}

async function tursoResolveAttempt(
  conn: TursoClient,
  key: { tenant: number; quiz: number; student: number },
  signal?: AbortSignal,
): Promise<number> {
  const { tenant, quiz, student } = key;
  const row = (await conn.get(
    "SELECT id FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1",
    tenant,
    quiz,
    student,
  )) as { id: number } | undefined | null;
  if (signal?.aborted) throw new DOMException("request aborted", "AbortError");
  if (row) return row.id;
  const info = (await conn.run(
    "INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, state) VALUES (?, ?, ?, 'started')",
    tenant,
    quiz,
    student,
  )) as { lastInsertRowid?: string | number | bigint };
  return Number(info.lastInsertRowid);
}

// ---- Turso Database (tursodb) transaction paths ------------------------------
// `tursodb-reused` runs the normal transaction form (BEGIN IMMEDIATE);
// `tursodb-concurrent` runs BEGIN CONCURRENT (MVCC) with conflict retries.
// Everything measured here is server-side wall time: for a retried operation
// `queryMs` spans every attempt plus the backoff between them.

export interface TxStats {
  /** Operations that entered the retry loop (1 per request). */
  attempted: number;
  /** Transaction attempts issued, including retries. */
  attemptsTotal: number;
  /** Operations that committed. */
  committed: number;
  /** Operations that committed only after at least one conflict. */
  successAfterRetry: number;
  /** Operations that still conflicted after the retry budget. */
  failedAfterRetries: number;
  /** Retryable conflict errors observed (one per failed attempt). */
  conflicts: number;
  /** Retry attempts scheduled (= failed attempts that still had budget). */
  retries: number;
  error: string | null;
}

interface TxRun {
  stats: TxStats;
  committed: boolean;
  queryMs: number;
}

const CONFLICT_MARKERS = ["conflict", "busy", "locked", "snapshot"];

/**
 * MVCC conflicts surface as SQLITE_BUSY / SQLITE_BUSY_SNAPSHOT ("snapshot")
 * or messages containing "conflict". Query timeouts are NOT conflicts, and
 * neither are constraint/application errors, so they are never retried.
 */
export function isConflictError(e: unknown): boolean {
  const err = e as {
    code?: unknown;
    rawCode?: unknown;
    message?: unknown;
  } | null;
  const parts = [err?.code, err?.rawCode, err?.message].filter(
    (v): v is string => typeof v === "string",
  );
  const text = (parts.length > 0 ? parts.join(" ") : String(e)).toLowerCase();
  if (text.includes("timeout")) return false;
  return CONFLICT_MARKERS.some((marker) => text.includes(marker));
}

/** Small exponential backoff with jitter: ~1-3ms, 2-6ms, 4-12ms, … capped at 50ms. */
async function sleepBackoff(attempt: number): Promise<void> {
  const base = Math.min(50, 2 ** (attempt + 1));
  await new Promise<void>((resolve) =>
    setTimeout(resolve, base / 2 + Math.random() * base),
  );
}

/**
 * Runs an atomic write transaction inside BEGIN CONCURRENT (MVCC), retrying
 * only on MVCC conflicts. The latency returned spans every attempt plus the
 * backoff between them.
 *
 * Implemented with `Connection.batch(statements, "concurrent")` rather than
 * `transactionAsync(...).concurrent()`: the latter opens a dedicated server
 * session per transaction, and Turso Cloud rejects the resulting session count
 * at benchmark concurrency ("DatabaseError: Database connections limit
 * exceeded, try to reduce concurrency"), which then poisons even simple reads
 * on that database. An atomic batch sends BEGIN CONCURRENT .. COMMIT plus the
 * statements as ONE request on a pooled connection, so concurrency is bounded
 * by the connection pool — the same reuse model as `turso-reused` — with bounded
 * pool queueing exposed in checkout timing.
 */
export async function execConcurrent(
  retries: number,
  attempt: () => Promise<void>,
  signal?: AbortSignal,
): Promise<TxRun> {
  const stats: TxStats = {
    attempted: 1,
    attemptsTotal: 0,
    committed: 0,
    successAfterRetry: 0,
    failedAfterRetries: 0,
    conflicts: 0,
    retries: 0,
    error: null,
  };
  const t0 = performance.now();
  for (let retry = 0; retry <= retries; retry++) {
    if (signal?.aborted)
      throw new DOMException("request aborted", "AbortError");
    stats.attemptsTotal++;
    try {
      await attempt();
      stats.committed = 1;
      if (retry > 0) stats.successAfterRetry = 1;
      return { stats, committed: true, queryMs: performance.now() - t0 };
    } catch (e) {
      if (!isConflictError(e)) throw e;
      stats.conflicts++;
      if (retry === retries) {
        stats.failedAfterRetries = 1;
        stats.error = "retryable write conflict (busy/locked)";
        return { stats, committed: false, queryMs: performance.now() - t0 };
      }
      stats.retries++;
      await sleepBackoff(retry);
    }
  }
  // Unreachable: every loop iteration returns or throws. Kept for the type checker.
  return { stats, committed: false, queryMs: performance.now() - t0 };
}

interface SqlStmt {
  sql: string;
  args: unknown[];
}

/**
 * Flattens a scenario into one statement list, resolving the quiz attempt
 * first when needed (the resolve step runs on the connection, outside the
 * batch — D1 has no interactive transactions and does the same).
 */
async function scenarioStatements(
  conn: TursoClient,
  options: {
    test: string;
    p: OpParams;
    sc: ScenarioSql;
    needsResolve: boolean;
    signal?: AbortSignal;
  },
): Promise<SqlStmt[]> {
  const { test, p, sc, needsResolve, signal } = options;
  const attemptId = needsResolve
    ? await tursoResolveAttempt(conn, p, signal)
    : p.attempt;
  const s =
    attemptId === p.attempt
      ? sc
      : scenarioSql(test, { ...p, attempt: attemptId });
  return [
    ...s.reads.map((q) => ({ sql: q.text, args: [...q.params] })),
    ...s.writes.map((q) => ({ sql: q.text, args: [...q.params] })),
  ];
}

/** Atomic transaction via `Connection.batch(stmts, mode)`; one round trip. */
async function runAtomicBatch(
  conn: TursoClient,
  mode: "immediate" | "concurrent",
  stmts: SqlStmt[],
): Promise<number> {
  const rs = (await conn.batch(stmts, mode)) as unknown;
  return Array.isArray(rs) ? rs.length : stmts.length;
}

/** Copies the MVCC counters into the response payload. */
function applyTx(extra: Record<string, unknown>, run: TxRun | undefined): void {
  if (run === undefined) return;
  extra.tx = run.stats;
  extra.txFailed = !run.committed;
  if (!run.committed) extra.txError = run.stats.error;
}

/** Statement walk over a plain Connection (autocommit), shared by libSQL/tursodb paths. */
async function runSequential(
  conn: TursoClient,
  sc: ScenarioSql,
  signal?: AbortSignal,
): Promise<number> {
  let n = 0;
  for (const q of sc.reads) {
    if (signal?.aborted)
      throw new DOMException("request aborted", "AbortError");
    if (q.many) await conn.all(q.text, ...q.params);
    else await conn.get(q.text, ...q.params);
    n++;
  }
  for (const q of sc.writes) {
    if (signal?.aborted)
      throw new DOMException("request aborted", "AbortError");
    if (q.text.startsWith("SELECT")) await conn.get(q.text, ...q.params);
    else await conn.run(q.text, ...q.params);
    n++;
  }
  return n;
}

// ---- Entrypoint -------------------------------------------------------------

export async function runOp(
  backend: BackendName,
  d1db: D1Database | null,
  env: Env,
  test: string,
  p: OpParams,
  tenantDb: TenantDbKind,
  options: {
    signal?: AbortSignal;
    timeoutMs?: number;
    waitUntil?: (promise: Promise<unknown>) => void;
  } = {},
): Promise<OpOutcome> {
  const { signal, timeoutMs = 5000, waitUntil } = options;
  const counters: QueryCounters = {
    queries: 0,
    rowsRead: null,
    rowsWritten: null,
  };
  if (d1db) d1db = meteredD1(d1db, counters);
  const requestedBackend = backend;
  const spec = backendSpec(backend);
  backend = spec.execution as BackendName;
  const orm =
    backend === "d1-drizzle" ||
    backend === "d1-eeur-drizzle" ||
    backend === "turso-drizzle"
      ? "drizzle"
      : "raw";
  const t = baseTimings(tenantDb, orm);
  const sc = scenarioSql(test, p);
  const needsResolve =
    (test === "submit-quiz-answer" || test === "exam-submit") && p.attempt <= 0;
  const mode = txModeOf(backend, test);
  const wall0 = performance.now();
  const extra: Record<string, unknown> = {
    transactionMode: mode,
    queries: sc.reads.length + sc.writes.length,
    rowsRead: null,
    rowsWritten: null,
  };
  let buildMs = 0;
  // Work for a client that already timed out is pure waste (and saturates the
  // backend). Each phase below re-checks the signal the Worker request carries.
  const ensureLive = (): void => {
    if (signal?.aborted === true)
      throw new DOMException(
        "bench request aborted by the client",
        "AbortError",
      );
  };

  // Drizzle composition pass over the identical statements (timed separately).
  if (orm === "drizzle") {
    for (const q of [...sc.reads, ...sc.writes]) {
      const b = drizzleText(q.text);
      buildMs += b.buildMs;
      void b.text;
    }
    t.buildMs = buildMs;
  }

  try {
    if (
      backend === "d1" ||
      backend === "d1-eeur" ||
      backend === "d1-drizzle" ||
      backend === "d1-eeur-drizzle"
    ) {
      if (!d1db) throw new Error("TOPOLOGY_INVALID: D1 database is missing");
      const db = d1db;
      ensureLive();
      const q0 = performance.now();
      const attemptId = needsResolve
        ? await d1ResolveAttempt(db, p, signal)
        : p.attempt;
      const s =
        attemptId === p.attempt
          ? sc
          : scenarioSql(test, { ...p, attempt: attemptId });
      // Transactional write tests use D1's atomic batch (D1 has no interactive
      // transactions); everything else keeps the original statement-per-call path.
      if (mode === "transaction") {
        ensureLive();
        const stmts = [
          ...s.reads.map((q) => db.prepare(q.text).bind(...q.params)),
          ...s.writes.map((q) => db.prepare(q.text).bind(...q.params)),
        ];
        const rs = await db.batch(stmts);
        t.queryMs = performance.now() - q0;
        extra.results = rs.length;
      } else {
        const out: unknown[] = [];
        for (const q of s.reads) {
          ensureLive();
          out.push(
            q.many
              ? await d1All(db, q.text, q.params)
              : await d1Get(db, q.text, q.params),
          );
        }
        for (const q of s.writes) {
          ensureLive();
          if (q.text.startsWith("SELECT"))
            out.push(await d1Get(db, q.text, q.params));
          else out.push(await d1Run(db, q.text, q.params));
        }
        t.queryMs = performance.now() - q0;
        extra.results = out.length;
      }
    } else if (backend === "d1-rr" || backend === "d1-eeur-rr") {
      if (!d1db) throw new Error("TOPOLOGY_INVALID: D1 database is missing");
      const db = d1db;
      const tr = rrTracker(db);
      ensureLive();
      const q0 = performance.now();
      const attemptId = needsResolve
        ? await rrResolveAttempt(tr, p, signal)
        : p.attempt;
      const s =
        attemptId === p.attempt
          ? sc
          : scenarioSql(test, { ...p, attempt: attemptId });
      if (mode === "transaction") {
        ensureLive();
        const stmts = [
          ...s.reads.map((q) => tr.session.prepare(q.text).bind(...q.params)),
          ...s.writes.map((q) => tr.session.prepare(q.text).bind(...q.params)),
        ];
        const rs = await tr.session.batch(stmts);
        for (let i = 0; i < rs.length; i++) {
          const r = rs[i];
          if (i < s.reads.length) {
            tr.totalReads++;
            const { primary } = metaOf(r);
            if (primary === false) tr.replicaReads++;
            else if (primary === true) tr.primaryReads++;
          }
          const { primary, region } = metaOf(r);
          tr.lastPrimary = primary;
          tr.lastRegion = region;
        }
        t.queryMs = performance.now() - q0;
        extra.results = rs.length;
      } else {
        const out: unknown[] = [];
        for (const q of s.reads) {
          ensureLive();
          out.push(
            q.many
              ? await rrAll(tr, q.text, q.params)
              : await rrGet(tr, q.text, q.params),
          );
        }
        for (const q of s.writes) {
          ensureLive();
          if (q.text.startsWith("SELECT"))
            out.push(await rrGet(tr, q.text, q.params));
          else out.push(await rrRun(tr, q.text, q.params));
        }
        t.queryMs = performance.now() - q0;
        extra.results = out.length;
      }
      t.replicaReads = tr.replicaReads;
      t.primaryReads = tr.primaryReads;
      t.totalReads = tr.totalReads;
      t.servedByPrimary = tr.lastPrimary;
      t.servedByRegion = tr.lastRegion;
    } else if (
      backend === "turso" ||
      backend === "turso-drizzle" ||
      backend === "turso-reused" ||
      backend === "tursodb-reused" ||
      backend === "tursodb-concurrent"
    ) {
      // Fresh-client libSQL (turso), pooled libSQL (turso-reused), pooled Turso
      // Database (tursodb-reused), and concurrent Turso Database
      // (tursodb-concurrent) all execute the same statement text; only the
      // transaction form and the connection pool differ.
      const fresh =
        spec.tenantIsolation === "shared-schema" &&
        (backend === "turso" || backend === "turso-drizzle");
      const body = async (
        unmetered: TursoClient,
      ): Promise<{ res: number; queryMs: number; tx?: TxRun }> => {
        const conn = meteredTurso(unmetered, counters);
        ensureLive();
        const q0 = performance.now();
        if (mode === "transaction") {
          const stmts = await scenarioStatements(conn, {
            test,
            p,
            sc,
            needsResolve,
            signal,
          });
          ensureLive();
          const res = await runAtomicBatch(conn, "immediate", stmts);
          return { res, queryMs: performance.now() - q0 };
        }
        if (mode === "concurrent") {
          // BEGIN CONCURRENT .. COMMIT + statements as one atomic round trip on
          // a pooled connection; conflicts retried by the server-side loop.
          const stmts = await scenarioStatements(conn, {
            test,
            p,
            sc,
            needsResolve,
            signal,
          });
          const run = await execConcurrent(
            p.retries,
            async () => {
              await runAtomicBatch(conn, "concurrent", stmts);
            },
            signal,
          );
          return { res: stmts.length, queryMs: run.queryMs, tx: run };
        }
        const attemptId = needsResolve
          ? await tursoResolveAttempt(conn, p, signal)
          : p.attempt;
        const res = await runSequential(
          conn,
          attemptId === p.attempt
            ? sc
            : scenarioSql(test, { ...p, attempt: attemptId }),
          signal,
        );
        return { res, queryMs: performance.now() - q0 };
      };
      if (fresh) {
        const c0 = performance.now();
        const conn = freshTursoConn(env, timeoutMs);
        t.clientCreationMs = performance.now() - c0;
        try {
          const out = await body(conn);
          t.queryMs = out.queryMs;
          extra.results = out.res;
          applyTx(extra, out.tx);
        } finally {
          const x0 = performance.now();
          await conn.close();
          t.clientCloseMs = performance.now() - x0;
        }
      } else {
        const config = externalConfig(env, requestedBackend, p.tenant);
        const manager = tenantManager(spec.engine, poolConfig(env, timeoutMs));
        const { out, checkoutMs, observation } = await manager.use(
          config.route.mapping?.databaseId ?? `${spec.engine}-shared`,
          JSON.stringify({ url: config.url, token: config.token }),
          body,
          { signal, waitUntil },
        );
        extra.pool = observation;
        extra.poolMetrics = manager.snapshot();
        t.clientCreationMs = observation.connectionCreateMs;
        t.checkoutMs = checkoutMs;
        t.queryMs = out.queryMs;
        extra.results = out.res;
        applyTx(extra, out.tx);
      }
    } else {
      throw new Error(`unhandled backend ${backend}`);
    }
  } catch (error) {
    if (error instanceof Error) {
      const details = (error as Error & { evidence?: Record<string, unknown> })
        .evidence;
      Object.assign(error, { evidence: { ...details, ...counters } });
    }
    throw error;
  }
  Object.assign(extra, counters);
  t.dbMs = performance.now() - wall0;
  t.totalDbMs = t.dbMs;
  return { t, extra };
}
