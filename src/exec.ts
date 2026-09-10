// Operation executors: edge (through the deployed Worker, the only fair
// comparison path for D1) and direct (bun -> Turso Cloud, reference only).
// Direct mode is informational: it measures client-to-cloud latency, which is
// not comparable with edge-terminated D1 traffic.
import { connect } from "@tursodatabase/serverless";
import { TX_WRITE_TESTS } from "../shared/backend-tests.ts";
import type { OpSample, OpFn } from "./runner.ts";
import type { TxCounts } from "./stats.ts";
import type { OpQuery } from "./workloads/common.ts";

interface EdgeBody {
  dbMs: number;
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
}

/** Server-side transaction counters as reported by the Worker. */
function txOf(raw: unknown): TxCounts | null {
  if (typeof raw !== "object" || raw === null) return null;
  const o = raw as Record<string, unknown>;
  const n = (k: string): number => (typeof o[k] === "number" ? (o[k] as number) : 0);
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

export function makeEdgeExec(workerUrl: string, adminToken: string, backend: string): OpFn {
  return async (op: OpQuery, signal: AbortSignal): Promise<OpSample> => {
    const t0 = performance.now();
    const qs = new URLSearchParams();
    const body: Record<string, number | string> = {};
    for (const [k, v] of Object.entries(op.params)) {
      if (op.method === "GET") qs.set(k, String(v));
      else {
        body[k] = v;
        if (k === "tenant" || k === "id" || k === "startId" || k === "limit") qs.set(k, String(v));
      }
    }
    const res = await fetch(`${workerUrl}/bench/${backend}/${op.test}?${qs}`, {
      method: op.method,
      headers: {
        ...(op.method === "POST" ? { "content-type": "application/json", authorization: `Bearer ${adminToken}` } : {}),
      },
      body: op.method === "POST" ? JSON.stringify(body) : undefined,
      signal,
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`edge ${backend}/${op.test}: HTTP ${res.status} ${text.slice(0, 200)}`);
    const parsed = JSON.parse(text) as EdgeBody;
    if (typeof parsed.dbMs !== "number") throw new Error(`edge ${backend}/${op.test}: bad response ${text.slice(0, 200)}`);
    const replica = parsed.servedByPrimary === false ? true : parsed.servedByPrimary === true ? false : null;
    return {
      e2eMs: performance.now() - t0,
      dbMs: parsed.dbMs,
      queryMs: typeof parsed.queryMs === "number" ? parsed.queryMs : null,
      buildMs: typeof parsed.buildMs === "number" ? parsed.buildMs : null,
      clientCreationMs: typeof parsed.clientCreationMs === "number" ? parsed.clientCreationMs : null,
      clientCloseMs: typeof parsed.clientCloseMs === "number" ? parsed.clientCloseMs : null,
      checkoutMs: typeof parsed.checkoutMs === "number" ? parsed.checkoutMs : null,
      replica,
      region: typeof parsed.servedByRegion === "string" ? parsed.servedByRegion : null,
      replicaReads: typeof parsed.replicaReads === "number" ? parsed.replicaReads : 0,
      primaryReads: typeof parsed.primaryReads === "number" ? parsed.primaryReads : 0,
      totalReads: typeof parsed.totalReads === "number" ? parsed.totalReads : 0,
      engine: typeof parsed.engine === "string" ? parsed.engine : null,
      transactionMode: typeof parsed.transactionMode === "string" ? parsed.transactionMode : null,
      tx: txOf(parsed.tx),
      txFailed: parsed.txFailed === true,
    };
  };
}

// Direct Hrana SQL mirror of worker/src/queries.ts scenario(). Kept in sync
// by hand; direct mode is reference-only so drift fails safe (it never feeds
// the D1 comparison), but identical text is the goal.
function directScenario(test: string, p: Record<string, number | string>): { reads: { text: string; params: unknown[]; many: boolean }[]; writes: { text: string; params: unknown[] }[] } {
  const N = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";
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
          { text: `UPDATE quiz_attempts SET updated_at = ${N}, state = 'in_progress' WHERE tenant_id = ? AND id = ?`, params: [p.tenant, p.attempt] },
        ],
      };
    case "independent-writes":
      return {
        reads: [{ text: "SELECT progress FROM concurrent_progress WHERE student_id = ?", params: [p.student], many: false }],
        writes: [{ text: `UPDATE concurrent_progress SET progress = progress + 1, updated_at = ${N} WHERE student_id = ?`, params: [p.student] }],
      };
    case "hot-row-write":
      return {
        reads: [{ text: "SELECT value FROM hot_counter WHERE id = 1", params: [], many: false }],
        writes: [{ text: "UPDATE hot_counter SET value = value + 1 WHERE id = 1", params: [] }],
      };
    case "update-progress":
      return {
        reads: [],
        writes: [
          { text: `INSERT INTO lesson_progress (tenant_id, student_id, lesson_id, course_id, position, completed, updated_at) VALUES (?, ?, ?, ?, ?, ?, ${N}) ON CONFLICT (tenant_id, student_id, lesson_id) DO UPDATE SET completed = excluded.completed, updated_at = ${N}`, params: [p.tenant, p.student, p.lesson, p.course, p.position, p.completed] },
          { text: "UPDATE enrollments SET progress_pct = (SELECT COUNT(*) FROM lesson_progress WHERE tenant_id = ? AND student_id = ? AND course_id = ? AND completed = 1) WHERE tenant_id = ? AND student_id = ? AND course_id = ?", params: [p.tenant, p.student, p.course, p.tenant, p.student, p.course] },
        ],
      };
    default:
      return {
        reads: [],
        writes: [
          { text: `INSERT INTO enrollments (tenant_id, student_id, course_id, progress_pct, status, created_at) VALUES (?, ?, ?, 0, 'active', ${N}) ON CONFLICT (tenant_id, student_id, course_id) DO NOTHING`, params: [p.tenant, p.student, p.course] },
          { text: "SELECT id, progress_pct, status FROM enrollments WHERE tenant_id = ? AND student_id = ? AND course_id = ?", params: [p.tenant, p.student, p.course] },
        ],
      };
  }
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

async function directExecScenario(r: DirectRunner, sc: DirectSql, signal: AbortSignal): Promise<void> {
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

export function makeDirectExec(tursoUrl: string, tursoToken: string): OpFn {
  const url = tursoUrl.replace(/^libsql:\/\//, "https://");
  return async (op: OpQuery, signal: AbortSignal): Promise<OpSample> => {
    const t0 = performance.now();
    if (signal.aborted) throw new DOMException("op timed out", "TimeoutError");
    const c0 = performance.now();
    const conn = connect({ url, authToken: tursoToken });
    const clientCreationMs = performance.now() - c0;
    const onAbort = (): void => {
      void conn.close();
    };
    signal.addEventListener("abort", onAbort, { once: true });
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
        const insertInfo = (await conn.run(
          "INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, state) VALUES (?, ?, ?, 'started')",
          op.params.tenant as number,
          op.params.quiz as number,
          op.params.student as number,
        )) as InsertInfo;
        const attemptId = attemptRow?.id ?? Number(insertInfo.lastInsertRowid);
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
        transactionMode: TX_WRITE_TESTS[op.test] === true ? "transaction" : "none",
        tx: null,
        txFailed: false,
      };
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  };
}
