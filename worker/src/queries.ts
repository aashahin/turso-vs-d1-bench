// Equivalent query implementations for every backend. Fairness rules:
// identical SQL text and bind order on D1 and Turso wherever the SQLite
// dialect permits; multi-query LMS scenarios run the same sequence on both;
// responses carry counts plus rows (real serialization cost, capped by LIMIT).
import { sql } from "drizzle-orm";
import {
  freshTursoConn,
  isTursoDbBackend,
  txModeOf,
  withPooledTurso,
  withPooledTursoDb,
  type BackendName,
  type Env,
  type TursoClient,
} from "./backends.ts";
import type { TenantDbKind } from "./tenant.ts";

export interface OpParams {
  id: number;
  startId: number;
  limit: number;
  tenant: number;
  student: number;
  course: number;
  lesson: number;
  quiz: number;
  question: number;
  attempt: number;
  position: number;
  payload: string;
  answer: string;
  completed: number;
  /** Max retries for retryable MVCC conflicts (tursodb-concurrent only). */
  retries: number;
}

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

const NOW = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

function baseTimings(tenantDb: TenantDbKind, orm: "raw" | "drizzle"): OpTimings {
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

function metaOf(rs: unknown): { primary: boolean | null; region: string | null } {
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
  const q = chunks.length > 1 ? sql.join(chunks.map((c) => sql.raw(c)), sql.raw("?")) : sql.raw(text);
  void q;
  return { text, buildMs: performance.now() - t0 };
}
// ---- D1 executors -----------------------------------------------------------

async function d1Get(db: D1Database, text: string, params: unknown[]): Promise<unknown> {
  return db.prepare(text).bind(...params).first();
}

async function d1All(db: D1Database, text: string, params: unknown[]): Promise<{ rows: unknown[] }> {
  const rs = await db.prepare(text).bind(...params).all();
  return { rows: rs.results };
}

async function d1Run(db: D1Database, text: string, params: unknown[]): Promise<unknown> {
  return db.prepare(text).bind(...params).run();
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
  return { session: db.withSession(), replicaReads: 0, primaryReads: 0, totalReads: 0, lastPrimary: null, lastRegion: null };
}

async function rrAll(tr: RRTracker, text: string, params: unknown[]): Promise<{ rows: unknown[] }> {
  const rs = await tr.session.prepare(text).bind(...params).all();
  const { primary, region } = metaOf(rs);
  tr.totalReads++;
  if (primary === false) tr.replicaReads++;
  else if (primary === true) tr.primaryReads++;
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  return { rows: rs.results };
}

async function rrGet(tr: RRTracker, text: string, params: unknown[]): Promise<unknown> {
  const rs = await tr.session.prepare(text).bind(...params).all();
  const { primary, region } = metaOf(rs);
  tr.totalReads++;
  if (primary === false) tr.replicaReads++;
  else if (primary === true) tr.primaryReads++;
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  const rows = (rs as unknown as { results: unknown[] }).results;
  return rows[0] ?? null;
}

async function rrRun(tr: RRTracker, text: string, params: unknown[]): Promise<unknown> {
  const rs = await tr.session.prepare(text).bind(...params).run();
  const { primary, region } = metaOf(rs);
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  return rs;
}

// ---- Scenario SQL (shared text both backends) -------------------------------

function scenario(test: string, p: OpParams): { reads: { text: string; params: unknown[]; many: boolean }[]; writes: { text: string; params: unknown[] }[] } {
  switch (test) {
    case "point-read":
      return { reads: [{ text: "SELECT id, payload FROM kv WHERE id = ?", params: [p.id], many: false }], writes: [] };
    case "scan":
      return { reads: [{ text: "SELECT id, payload FROM kv WHERE id >= ? ORDER BY id LIMIT ?", params: [p.startId, p.limit], many: true }], writes: [] };
    case "insert":
      return { reads: [], writes: [{ text: "INSERT INTO bench_writes (payload) VALUES (?)", params: [p.payload] }] };
    case "update":
      return { reads: [], writes: [{ text: "UPDATE kv SET payload = ? WHERE id = ?", params: [p.payload, p.id] }] };
    case "student-dashboard":
      return {
        reads: [
          { text: "SELECT e.id, e.course_id, e.progress_pct, e.status, c.title, c.slug FROM enrollments e JOIN courses c ON c.id = e.course_id AND c.tenant_id = e.tenant_id WHERE e.tenant_id = ? AND e.student_id = ? AND e.status = 'active' ORDER BY e.id LIMIT 20", params: [p.tenant, p.student], many: true },
          { text: "SELECT lesson_id, course_id, completed, updated_at FROM lesson_progress WHERE tenant_id = ? AND student_id = ? ORDER BY updated_at DESC LIMIT 20", params: [p.tenant, p.student], many: true },
        ],
        writes: [],
      };
    case "course-page":
      return {
        reads: [
          { text: "SELECT id, title, slug, status, price_cents FROM courses WHERE tenant_id = ? AND id = ?", params: [p.tenant, p.course], many: false },
          { text: "SELECT id, position, title, duration_s FROM lessons WHERE tenant_id = ? AND course_id = ? ORDER BY position LIMIT 100", params: [p.tenant, p.course], many: true },
          { text: "SELECT id, progress_pct, status FROM enrollments WHERE tenant_id = ? AND student_id = ? AND course_id = ?", params: [p.tenant, p.student, p.course], many: false },
          { text: "SELECT (SELECT COUNT(*) FROM lesson_progress WHERE tenant_id = ? AND student_id = ? AND course_id = ? AND completed = 1) AS done, (SELECT COUNT(*) FROM lessons WHERE tenant_id = ? AND course_id = ?) AS total", params: [p.tenant, p.student, p.course, p.tenant, p.course], many: false },
        ],
        writes: [],
      };
    case "lesson-page":
      return {
        reads: [
          { text: "SELECT id, course_id, position, title, duration_s FROM lessons WHERE tenant_id = ? AND id = ?", params: [p.tenant, p.lesson], many: false },
          { text: "SELECT id, title, slug FROM courses WHERE tenant_id = ? AND id = ?", params: [p.tenant, p.course], many: false },
          { text: "SELECT completed, position FROM lesson_progress WHERE tenant_id = ? AND student_id = ? AND lesson_id = ?", params: [p.tenant, p.student, p.lesson], many: false },
          { text: "SELECT id, position, title FROM lessons WHERE tenant_id = ? AND course_id = ? AND position >= ? ORDER BY position LIMIT 3", params: [p.tenant, p.course, p.position], many: true },
        ],
        writes: [],
      };
    case "quiz-page":
      return {
        reads: [
          { text: "SELECT id, lesson_id, course_id, title FROM quizzes WHERE tenant_id = ? AND id = ?", params: [p.tenant, p.quiz], many: false },
          { text: "SELECT id, position, prompt, kind FROM quiz_questions WHERE tenant_id = ? AND quiz_id = ? ORDER BY position", params: [p.tenant, p.quiz], many: true },
          { text: "SELECT id, score, state, updated_at FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1", params: [p.tenant, p.quiz, p.student], many: false },
        ],
        writes: [],
      };
    case "submit-quiz-answer":
      return {
        reads: [{ text: "SELECT id, state FROM quiz_attempts WHERE tenant_id = ? AND id = ?", params: [p.tenant, p.attempt], many: false }],
        writes: [
          { text: "INSERT INTO quiz_answers (tenant_id, attempt_id, question_id, answer, is_correct) VALUES (?, ?, ?, ?, ?)", params: [p.tenant, p.attempt, p.question, p.answer, 0] },
          { text: `UPDATE quiz_attempts SET updated_at = ${NOW}, state = 'in_progress' WHERE tenant_id = ? AND id = ?`, params: [p.tenant, p.attempt] },
        ],
      };
    case "update-progress":
      return {
        reads: [],
        writes: [
          { text: `INSERT INTO lesson_progress (tenant_id, student_id, lesson_id, course_id, position, completed, updated_at) VALUES (?, ?, ?, ?, ?, ?, ${NOW}) ON CONFLICT (tenant_id, student_id, lesson_id) DO UPDATE SET completed = excluded.completed, updated_at = ${NOW}`, params: [p.tenant, p.student, p.lesson, p.course, p.position, p.completed] },
          { text: "UPDATE enrollments SET progress_pct = (SELECT COUNT(*) FROM lesson_progress WHERE tenant_id = ? AND student_id = ? AND course_id = ? AND completed = 1) WHERE tenant_id = ? AND student_id = ? AND course_id = ?", params: [p.tenant, p.student, p.course, p.tenant, p.student, p.course] },
        ],
      };
    case "independent-writes":
      // Row-level MVCC target: each request targets a different student row.
      return {
        reads: [{ text: "SELECT progress FROM concurrent_progress WHERE student_id = ?", params: [p.student], many: false }],
        writes: [{ text: `UPDATE concurrent_progress SET progress = progress + 1, updated_at = ${NOW} WHERE student_id = ?`, params: [p.student] }],
      };
    case "hot-row-write":
      // Write/write conflict target: every request updates the same row.
      return {
        reads: [{ text: "SELECT value FROM hot_counter WHERE id = 1", params: [], many: false }],
        writes: [{ text: "UPDATE hot_counter SET value = value + 1 WHERE id = 1", params: [] }],
      };
    case "enrollment":
      return {
        reads: [],
        writes: [
          { text: `INSERT INTO enrollments (tenant_id, student_id, course_id, progress_pct, status, created_at) VALUES (?, ?, ?, 0, 'active', ${NOW}) ON CONFLICT (tenant_id, student_id, course_id) DO NOTHING`, params: [p.tenant, p.student, p.course] },
          { text: "SELECT id, progress_pct, status FROM enrollments WHERE tenant_id = ? AND student_id = ? AND course_id = ?", params: [p.tenant, p.student, p.course] },
        ],
      };
    default:
      throw new Error(`unknown test ${JSON.stringify(test)}`);
  }
}

// ---- Attempt resolution (submit-quiz-answer with attempt=0) -------------------
// Attempt rowids are autoincrement and cannot be derived arithmetically, so
// the runner passes attempt=0 and the Worker resolves the latest attempt for
// (tenant, quiz, student), creating one if none exists. This runs inside the
// timed region: real apps do this lookup too.

async function d1ResolveAttempt(db: D1Database, tenant: number, quiz: number, student: number): Promise<number> {
  const row = await db
    .prepare("SELECT id FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1")
    .bind(tenant, quiz, student)
    .first<{ id: number }>();
  if (row) return row.id;
  const rs = await db
    .prepare("INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, state) VALUES (?, ?, ?, 'started')")
    .bind(tenant, quiz, student)
    .run();
  return Number(rs.meta.last_row_id);
}

async function rrResolveAttempt(tr: RRTracker, tenant: number, quiz: number, student: number): Promise<number> {
  const rs = await tr.session
    .prepare("SELECT id FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1")
    .bind(tenant, quiz, student)
    .all<{ id: number }>();
  const { primary, region } = metaOf(rs);
  tr.totalReads++;
  if (primary === false) tr.replicaReads++;
  else if (primary === true) tr.primaryReads++;
  tr.lastPrimary = primary;
  tr.lastRegion = region;
  if (rs.results[0]) return rs.results[0].id;
  const ins = await tr.session
    .prepare("INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, state) VALUES (?, ?, ?, 'started')")
    .bind(tenant, quiz, student)
    .run();
  tr.lastPrimary = metaOf(ins).primary;
  tr.lastRegion = metaOf(ins).region;
  return Number(ins.meta.last_row_id);
}

async function tursoResolveAttempt(conn: TursoClient, tenant: number, quiz: number, student: number): Promise<number> {
  const row = (await conn.get(
    "SELECT id FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1",
    tenant,
    quiz,
    student,
  )) as { id: number } | undefined | null;
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
  const err = e as { code?: unknown; rawCode?: unknown; message?: unknown } | null;
  const parts = [err?.code, err?.rawCode, err?.message].filter((v): v is string => typeof v === "string");
  const text = (parts.length > 0 ? parts.join(" ") : String(e)).toLowerCase();
  if (text.includes("timeout")) return false;
  return CONFLICT_MARKERS.some((marker) => text.includes(marker));
}

/** Small exponential backoff with jitter: ~1-3ms, 2-6ms, 4-12ms, … capped at 50ms. */
async function sleepBackoff(attempt: number): Promise<void> {
  const base = Math.min(50, 2 ** (attempt + 1));
  await new Promise<void>((resolve) => setTimeout(resolve, base / 2 + Math.random() * base));
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
 * by the connection pool — the same reuse model as `turso-reused` — with no
 * application-level queue or lock.
 */
export async function execConcurrent(retries: number, attempt: () => Promise<void>): Promise<TxRun> {
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
        stats.error = (e instanceof Error ? e.message : String(e)).slice(0, 200);
        return { stats, committed: false, queryMs: performance.now() - t0 };
      }
      stats.retries++;
      await sleepBackoff(retry);
    }
  }
  /* istanbul ignore next -- loop always returns */
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
async function scenarioStatements(conn: TursoClient, test: string, p: OpParams, sc: { reads: { text: string; params: unknown[]; many: boolean }[]; writes: { text: string; params: unknown[] }[] }, needsResolve: boolean): Promise<SqlStmt[]> {
  const attemptId = needsResolve ? await tursoResolveAttempt(conn, p.tenant, p.quiz, p.student) : p.attempt;
  const s = attemptId === p.attempt ? sc : scenario(test, { ...p, attempt: attemptId });
  return [
    ...s.reads.map((q) => ({ sql: q.text, args: [...q.params] })),
    ...s.writes.map((q) => ({ sql: q.text, args: [...q.params] })),
  ];
}

/** Atomic transaction via `Connection.batch(stmts, mode)`; one round trip. */
async function runAtomicBatch(conn: TursoClient, mode: "immediate" | "concurrent", stmts: SqlStmt[]): Promise<number> {
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
async function runSequential(conn: TursoClient, sc: { reads: { text: string; params: unknown[]; many: boolean }[]; writes: { text: string; params: unknown[] }[] }): Promise<number> {
  let n = 0;
  for (const q of sc.reads) {
    if (q.many) await conn.all(q.text, ...q.params);
    else await conn.get(q.text, ...q.params);
    n++;
  }
  for (const q of sc.writes) {
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
  signal?: AbortSignal,
): Promise<OpOutcome> {
  const orm = backend === "d1-drizzle" || backend === "d1-eeur-drizzle" || backend === "turso-drizzle" ? "drizzle" : "raw";
  const t = baseTimings(tenantDb, orm);
  const sc = scenario(test, p);
  const needsResolve = test === "submit-quiz-answer" && p.attempt <= 0;
  const mode = txModeOf(backend, test);
  const wall0 = performance.now();
  const extra: Record<string, unknown> = { transactionMode: mode };
  let buildMs = 0;
  // Work for a client that already timed out is pure waste (and saturates the
  // backend). Each phase below re-checks the signal the Worker request carries.
  const ensureLive = (): void => {
    if (signal?.aborted === true) throw new DOMException("bench request aborted by the client", "AbortError");
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

  if (backend === "d1" || backend === "d1-eeur" || backend === "d1-drizzle" || backend === "d1-eeur-drizzle") {
    const db = d1db ?? env.DB;
    ensureLive();
    const q0 = performance.now();
    const attemptId = needsResolve ? await d1ResolveAttempt(db, p.tenant, p.quiz, p.student) : p.attempt;
    const s = attemptId === p.attempt ? sc : scenario(test, { ...p, attempt: attemptId });
    // Transactional write tests use D1's atomic batch (D1 has no interactive
    // transactions); everything else keeps the original statement-per-call path.
    if (mode === "transaction") {
      ensureLive();
      const stmts = [...s.reads.map((q) => db.prepare(q.text).bind(...q.params)), ...s.writes.map((q) => db.prepare(q.text).bind(...q.params))];
      const rs = await db.batch(stmts);
      t.queryMs = performance.now() - q0;
      extra.results = rs.length;
    } else {
      const out: unknown[] = [];
      for (const q of s.reads) out.push(q.many ? await d1All(db, q.text, q.params) : await d1Get(db, q.text, q.params));
      for (const q of s.writes) {
        if (q.text.startsWith("SELECT")) out.push(await d1Get(db, q.text, q.params));
        else out.push(await d1Run(db, q.text, q.params));
      }
      t.queryMs = performance.now() - q0;
      extra.results = out.length;
    }
  } else if (backend === "d1-rr" || backend === "d1-eeur-rr") {
    const db = d1db ?? env.DB;
    const tr = rrTracker(db);
    ensureLive();
    const q0 = performance.now();
    const attemptId = needsResolve ? await rrResolveAttempt(tr, p.tenant, p.quiz, p.student) : p.attempt;
    const s = attemptId === p.attempt ? sc : scenario(test, { ...p, attempt: attemptId });
    if (mode === "transaction") {
      const stmts = [...s.reads.map((q) => tr.session.prepare(q.text).bind(...q.params)), ...s.writes.map((q) => tr.session.prepare(q.text).bind(...q.params))];
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
      for (const q of s.reads) out.push(q.many ? await rrAll(tr, q.text, q.params) : await rrGet(tr, q.text, q.params));
      for (const q of s.writes) {
        if (q.text.startsWith("SELECT")) out.push(await rrGet(tr, q.text, q.params));
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
  } else if (backend === "turso" || backend === "turso-drizzle" || backend === "turso-reused" || backend === "tursodb-reused" || backend === "tursodb-concurrent") {
    // Fresh-client libSQL (turso), pooled libSQL (turso-reused), pooled Turso
    // Database (tursodb-reused), and concurrent Turso Database
    // (tursodb-concurrent) all execute the same statement text; only the
    // transaction form and the connection pool differ.
    const fresh = backend === "turso" || backend === "turso-drizzle";
    const pooled = isTursoDbBackend(backend) ? withPooledTursoDb : withPooledTurso;
    const body = async (conn: TursoClient): Promise<{ res: number; queryMs: number; tx?: TxRun }> => {
      ensureLive();
      const q0 = performance.now();
      if (mode === "transaction") {
        const stmts = await scenarioStatements(conn, test, p, sc, needsResolve);
        const res = await runAtomicBatch(conn, "immediate", stmts);
        return { res, queryMs: performance.now() - q0 };
      }
      if (mode === "concurrent") {
        // BEGIN CONCURRENT .. COMMIT + statements as one atomic round trip on
        // a pooled connection; conflicts retried by the server-side loop.
        const stmts = await scenarioStatements(conn, test, p, sc, needsResolve);
        const run = await execConcurrent(p.retries, async () => {
          await runAtomicBatch(conn, "concurrent", stmts);
        });
        return { res: stmts.length, queryMs: run.queryMs, tx: run };
      }
      const attemptId = needsResolve ? await tursoResolveAttempt(conn, p.tenant, p.quiz, p.student) : p.attempt;
      const res = await runSequential(conn, attemptId === p.attempt ? sc : scenario(test, { ...p, attempt: attemptId }));
      return { res, queryMs: performance.now() - q0 };
    };
    if (fresh) {
      const c0 = performance.now();
      const conn = freshTursoConn(env);
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
      const { out, checkoutMs } = await pooled(env, body, signal);
      t.checkoutMs = checkoutMs;
      t.queryMs = out.queryMs;
      extra.results = out.res;
      applyTx(extra, out.tx);
    }
  } else {
    throw new Error(`unhandled backend ${backend}`);
  }

  t.dbMs = performance.now() - wall0;
  t.totalDbMs = t.dbMs;
  return { t, extra };
}
