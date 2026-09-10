// Deterministic seed generator: identical logical data on the libSQL database,
// the new Turso Database (tursodb, when configured) and every D1 database.
// Usage:
//   bun run seed -- --tenants=10 --students-per-tenant=200 \
//     --courses-per-tenant=5 --lessons-per-course=10 --rows=10000
// Env vars (WORKER_URL, TURSO_URL, TURSO_TOKEN, TURSODB_URL, TURSODB_TOKEN,
// ADMIN_TOKEN, ROWS, TENANTS, STUDENTS_PER_TENANT, COURSES_PER_TENANT,
// LESSONS_PER_COURSE) take precedence.
import { connect, type Connection } from "@tursodatabase/serverless";
import { getRaw } from "./config.ts";
import { SEED_UPDATED_AT, courseId, lessonId, questionId, quizId, userId } from "../worker/src/seed.ts";

const MAX_SEED_ROWS = 50000;

const rows = Number(getRaw("rows", "10000"));
if (!Number.isInteger(rows) || rows < 1 || rows > MAX_SEED_ROWS) {
  throw new Error(`--rows must be an integer 1..${MAX_SEED_ROWS} (got ${JSON.stringify(getRaw("rows", "10000"))})`);
}
const T = Number(getRaw("tenants", "10"));
const S = Number(getRaw("students-per-tenant", "200"));
const C = Number(getRaw("courses-per-tenant", "5"));
const L = Number(getRaw("lessons-per-course", "10"));
for (const [name, v, max] of [["tenants", T, 200], ["students-per-tenant", S, 2000], ["courses-per-tenant", C, 50], ["lessons-per-course", L, 100]] as const) {
  if (!Number.isInteger(v) || v < 1 || v > max) throw new Error(`--${name} must be an integer 1..${max} (got ${v})`);
}

const workerUrl = getRaw("worker-url", "http://127.0.0.1:8787").replace(/\/$/, "");
const toHttp = (u: string): string => u.replace(/^(libsql|turso):\/\//, "https://");
const tursoUrl = toHttp(getRaw("turso-url", ""));
const tursoToken = getRaw("turso-token", "");
const tursodbUrl = toHttp(getRaw("tursodb-url", ""));
const tursodbToken = getRaw("tursodb-token", "");
const adminToken = getRaw("admin-token", "");
if (!tursoUrl || !tursoToken) throw new Error("need --turso-url and --turso-token (or TURSO_URL/TURSO_TOKEN env)");
if (!adminToken) throw new Error("need --admin-token (or ADMIN_TOKEN env) for Worker admin endpoints");
const adminHeaders = { authorization: `Bearer ${adminToken}` };
if (tursodbUrl !== "" && tursodbUrl === tursoUrl) {
  throw new Error("TURSODB_URL must be a different database than TURSO_URL: the new Turso Database must actually be a tursodb database (turso db create --tursodb)");
}
const tursodbConfigured = tursodbUrl !== "" && tursodbToken !== "";

// 0. Preflight: the Worker must target the same Turso databases the client seeds.
const identRes = await fetch(`${workerUrl}/bench/identity`);
if (!identRes.ok) throw new Error(`worker identity check failed: HTTP ${identRes.status} ${await identRes.text()}`);
const ident = (await identRes.json()) as { tursoHost: string | null; tursodbHost: string | null; tursodbConfigured?: boolean };
const clientHost = new URL(tursoUrl).host;
if (ident.tursoHost !== clientHost) {
  throw new Error(`worker targets Turso host ${JSON.stringify(ident.tursoHost)}, client targets ${JSON.stringify(clientHost)}; refusing to seed`);
}
if (tursodbConfigured) {
  const clientTursoDbHost = new URL(tursodbUrl).host;
  if (ident.tursodbHost !== clientTursoDbHost) {
    throw new Error(`worker targets tursodb host ${JSON.stringify(ident.tursodbHost)}, client targets ${JSON.stringify(clientTursoDbHost)}; refusing to seed`);
  }
} else {
  console.warn("WARNING: TURSODB_URL/TURSODB_TOKEN not set — the tursodb backends will not be seeded or benchmarkable.");
}

// Strip comment lines: Hrana exec() rejects leading `--` comments.
const schema = (await Bun.file("schema.sql").text())
  .split("\n")
  .filter((line) => !line.trimStart().startsWith("--"))
  .join("\n");

/** Applies the shared schema (outside any concurrent transaction) and ANALYZE. */
async function applySchema(conn: Connection, label: string): Promise<boolean> {
  for (const stmt of schema.split(";").map((s) => s.trim()).filter(Boolean)) await conn.exec(stmt);
  try {
    await conn.exec("ANALYZE;");
    return true;
  } catch {
    console.log(`${label}: ANALYZE not supported, skipped`);
    return false;
  }
}

/**
 * Groups rows into multi-row INSERT statements (100 rows per statement).
 * Measured on the new engine: a 100-statement `conn.batch()` of single-row
 * INSERTs takes ~10.5s (~105ms per statement, i.e. no pipelining), while one
 * multi-row INSERT of 100 rows takes ~383ms. The logical data is identical on
 * every backend; only the statement shape changes.
 */
function makeRowBatcher(conn: Connection, size = 100) {
  interface Group {
    table: string;
    columns: string[];
    rows: unknown[][];
  }
  const groups = new Map<string, Group>();
  let count = 0;
  const flushGroup = async (g: Group): Promise<void> => {
    if (g.rows.length === 0) return;
    const values = g.rows.map(() => `(${g.columns.map(() => "?").join(",")})`).join(",");
    await conn.run(`INSERT INTO ${g.table} (${g.columns.join(",")}) VALUES ${values}`, ...g.rows.flat());
    g.rows = [];
  };
  return {
    add: async (table: string, columns: string[], row: unknown[]): Promise<void> => {
      const key = `${table}|${columns.join(",")}`;
      let g = groups.get(key);
      if (g === undefined) {
        g = { table, columns, rows: [] };
        groups.set(key, g);
      }
      g.rows.push(row);
      count++;
      if (g.rows.length >= size) await flushGroup(g);
    },
    flush: async (): Promise<void> => {
      for (const g of groups.values()) await flushGroup(g);
    },
    count: (): number => count,
  };
}

/** Seeds KV + LMS + concurrency tables with the same layout the Worker uses for D1. */
async function seedSqliteLike(conn: Connection, label: string): Promise<void> {
  console.log(`seeding ${label} kv=${rows} lms=T${T}/S${S}/C${C}/L${L}…`);
  const analyzed = await applySchema(conn, label);
  await conn.exec("DELETE FROM kv; DELETE FROM bench_writes; DELETE FROM concurrent_progress; DELETE FROM hot_counter;");

  // KV + concurrency tables.
  {
    const t0 = performance.now();
    const batcher = makeRowBatcher(conn);
    const payload = "x".repeat(200);
    for (let i = 1; i <= rows; i++) {
      await batcher.add("kv", ["id", "payload"], [i, `${i}:${payload}`]);
      if (i % 2000 === 0) process.stdout.write(`\r${label} kv: ${i}/${rows}`);
    }
    for (let i = 1; i <= rows; i++) await batcher.add("concurrent_progress", ["student_id", "progress", "updated_at"], [i, 0, SEED_UPDATED_AT]);
    await batcher.add("hot_counter", ["id", "value"], [1, 0]);
    await batcher.flush();
    console.log(`\n${label} kv seed (${batcher.count()} rows) in ${Math.round(performance.now() - t0)}ms (analyzed=${analyzed})`);
  }

  // LMS.
  {
    const t0 = performance.now();
    const batcher = makeRowBatcher(conn);
    await conn.exec("DELETE FROM quiz_answers; DELETE FROM quiz_attempts; DELETE FROM quiz_questions; DELETE FROM quizzes; DELETE FROM lesson_progress; DELETE FROM enrollments; DELETE FROM lessons; DELETE FROM courses; DELETE FROM users; DELETE FROM orders;");
    for (let t = 1; t <= T; t++) {
      for (let s = 1; s <= S; s++) {
        const uid = userId(t, s, S);
        await batcher.add("users", ["id", "tenant_id", "email", "name", "role"], [uid, t, `student${uid}@t${t}.test`, `Student ${uid}`, "student"]);
      }
      for (let c = 1; c <= C; c++) {
        const cid = courseId(t, c, C);
        await batcher.add("courses", ["id", "tenant_id", "title", "slug", "status", "price_cents"], [cid, t, `Course ${cid}`, `course-${cid}`, "published", 1000 + cid]);
        for (let l = 1; l <= L; l++) {
          await batcher.add(
            "lessons",
            ["id", "tenant_id", "course_id", "position", "title", "body", "duration_s"],
            [lessonId(t, c, l, C, L), t, cid, l, `Lesson ${l} of ${cid}`, `Body of lesson ${l}. ` + "y".repeat(120), 600],
          );
        }
        const qid = quizId(t, c, C);
        await batcher.add("quizzes", ["id", "tenant_id", "lesson_id", "course_id", "title"], [qid, t, lessonId(t, c, 1, C, L), cid, `Quiz ${qid}`]);
        for (let q = 1; q <= 5; q++) {
          await batcher.add("quiz_questions", ["id", "tenant_id", "quiz_id", "position", "prompt", "kind"], [questionId(t, c, q, C), t, qid, q, `Question ${q} of quiz ${qid}?`, "mcq"]);
        }
      }
      for (let s = 1; s <= S; s++) {
        const uid = userId(t, s, S);
        const c1 = ((s - 1) % C) + 1;
        const c2 = (s % C) + 1;
        for (const c of [c1, c2]) {
          await batcher.add("enrollments", ["tenant_id", "student_id", "course_id", "progress_pct", "status"], [t, uid, courseId(t, c, C), 0, "active"]);
        }
        if (s % 10 === 1) {
          for (const c of [c1, c2]) {
            const cid = courseId(t, c, C);
            for (let l = 1; l <= Math.min(4, L); l++) {
              await batcher.add("lesson_progress", ["tenant_id", "student_id", "lesson_id", "course_id", "position", "completed"], [t, uid, lessonId(t, c, l, C, L), cid, l, 1]);
            }
          }
        }
        if (s % 20 === 1) {
          for (const c of [c1, c2]) {
            await batcher.add("quiz_attempts", ["tenant_id", "quiz_id", "student_id", "score", "state"], [t, quizId(t, c, C), uid, null, "started"]);
          }
        }
        if (s % 5 === 1) {
          await batcher.add("orders", ["tenant_id", "student_id", "course_id", "amount_cents", "status"], [t, uid, courseId(t, c1, C), 1000 + courseId(t, c1, C), "paid"]);
        }
      }
      process.stdout.write(`\r${label} lms: tenant ${t}/${T}`);
    }
    await batcher.flush();
    console.log(`\n${label} lms seeded (${batcher.count()} rows) in ${Math.round(performance.now() - t0)}ms`);
  }
}

/** Verifies the concurrency tables exist with the expected row counts. */
async function verify(conn: Connection, label: string): Promise<void> {
  const count = async (sql: string): Promise<number> => {
    const row = (await conn.get(sql)) as { n: number } | undefined;
    return row?.n ?? -1;
  };
  const kv = await count("SELECT COUNT(*) AS n FROM kv");
  const progress = await count("SELECT COUNT(*) AS n FROM concurrent_progress");
  const hot = await count("SELECT COUNT(*) AS n FROM hot_counter");
  if (kv !== rows) throw new Error(`${label} kv verification failed: expected ${rows}, found ${kv}`);
  if (progress !== rows) throw new Error(`${label} concurrent_progress verification failed: expected ${rows}, found ${progress}`);
  if (hot !== 1) throw new Error(`${label} hot_counter verification failed: expected 1, found ${hot}`);
  console.log(`${label} verified: kv=${kv} concurrent_progress=${progress} hot_counter=${hot}`);
}

// 1. Existing Turso Cloud / libSQL database.
const conn = connect({ url: tursoUrl, authToken: tursoToken });
await seedSqliteLike(conn, "turso(libsql)");
await verify(conn, "turso(libsql)");
await conn.close();

// 2. New Turso Database engine (MVCC / concurrent writes).
if (tursodbConfigured) {
  const dbConn = connect({ url: tursodbUrl, authToken: tursodbToken });
  await seedSqliteLike(dbConn, "tursodb");
  await verify(dbConn, "tursodb");
  await dbConn.close();
}

// 3. D1 via worker (seeds WEUR + EEUR primaries + any tenant bindings).
console.log(`seeding d1 kv=${rows}…`);
{
  const res = await fetch(`${workerUrl}/admin/seed-d1?rows=${rows}`, { method: "POST", headers: adminHeaders });
  if (!res.ok) throw new Error(`seed-d1 failed: HTTP ${res.status} ${await res.text()}`);
  console.log("d1 kv:", await res.json());
}
console.log(`seeding d1 lms=T${T}/S${S}/C${C}/L${L}…`);
{
  const res = await fetch(
    `${workerUrl}/admin/seed-lms?tenants=${T}&students-per-tenant=${S}&courses-per-tenant=${C}&lessons-per-course=${L}`,
    { method: "POST", headers: adminHeaders },
  );
  if (!res.ok) throw new Error(`seed-lms failed: HTTP ${res.status} ${await res.text()}`);
  console.log("d1 lms:", await res.json());
}

// 4. Verify all sides agree on KV + the concurrency tables; LMS must be non-zero.
const metaRes = await fetch(`${workerUrl}/bench/meta`);
if (!metaRes.ok) throw new Error(`meta check failed: HTTP ${metaRes.status} ${await metaRes.text()}`);
const meta = (await metaRes.json()) as {
  d1_weur_kv_rows: number;
  d1_eeur_kv_rows: number;
  turso_kv_rows: number;
  tursodb_kv_rows: number | null;
  tursodb_progress_rows: number | null;
  tursodb_hot_rows: number | null;
  tursodb_error: string | null;
  d1_lms: { users: number; courses: number; lessons: number; enrollments: number };
  d1_concurrency_rows: { concurrent_progress: number | null; hot_counter: number | null; };
};
console.log("meta:", meta);
if (meta.d1_weur_kv_rows !== rows || meta.d1_eeur_kv_rows !== rows || meta.turso_kv_rows !== rows) {
  throw new Error(`kv row count mismatch, expected ${rows}: ${JSON.stringify(meta)}`);
}
if (meta.d1_concurrency_rows.concurrent_progress !== rows || meta.d1_concurrency_rows.hot_counter !== 1) {
  throw new Error(`D1 concurrency table mismatch, expected ${rows}/1: ${JSON.stringify(meta.d1_concurrency_rows)}`);
}
if (tursodbConfigured) {
  if (meta.tursodb_kv_rows !== rows || meta.tursodb_progress_rows !== rows || meta.tursodb_hot_rows !== 1) {
    throw new Error(`tursodb row counts mismatch (kv/progress/hot expected ${rows}/${rows}/1): ${JSON.stringify(meta)} ${meta.tursodb_error ?? ""}`);
  }
}
if (!meta.d1_lms.users || meta.d1_lms.users <= 0) throw new Error(`lms seed verification failed: ${JSON.stringify(meta.d1_lms)}`);
console.log(
  tursodbConfigured
    ? "seed ok: identical KV + concurrency row counts on libSQL, tursodb and D1; LMS present"
    : "seed ok: identical KV + concurrency row counts on libSQL and D1; LMS present (tursodb skipped)",
);
