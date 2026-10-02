// Deterministic LMS + KV seeding shared by all D1 targets. ID layout is pure
// arithmetic so the bench runner can pick valid random IDs without asking:
//   users:         id = (t-1)*S + s            (s = 1..S, tenant t)
//   courses:       id = (t-1)*C + c
//   lessons:       id = (t-1)*C*L + (c-1)*L + l (course c, position l)
//   quizzes:       id = (t-1)*C + c            (1 per course, first lesson)
//   questions:     id = ((t-1)*C + (c-1))*5 + q (5 per quiz, q = 1..5)
//   enrollments:   2 per student (courses ((s-1)%C)+1, (s%C)+1)
//   lesson_progress: sparse — students with s%10==1, first min(4,L) lessons
//   quiz_attempts:  sparse — students with s%20==1, one attempt per course quiz
//   orders:        sparse — students with s%5==1, first enrolled course
// Must stay in sync with schema.sql, src/seed.ts, and src/workloads/lms.ts.

export interface SeedDims {
  tenants: number;
  studentsPerTenant: number;
  coursesPerTenant: number;
  lessonsPerCourse: number;
}

import { SCHEMA_SQL } from "../../shared/schema.ts";
export const LMS_DDL = SCHEMA_SQL;
export const KV_DDL = SCHEMA_SQL;

// D1 exec() splits at line boundaries. Prepared DDL batches preserve multi-line SQL.
export async function applySchema(db: D1Database): Promise<void> {
  const statements = SCHEMA_SQL.split(";")
    .map((sql) => sql.trim())
    .filter(Boolean);
  await db.batch(statements.map((sql) => db.prepare(sql)));
}
async function resetTables(db: D1Database, tables: string[]): Promise<void> {
  await db.batch(tables.map((table) => db.prepare(`DELETE FROM ${table}`)));
}

/** Fixed seed timestamp so every backend stores byte-identical rows. */
export const SEED_UPDATED_AT = "2026-01-01T00:00:00.000Z";

/** Refreshes planner statistics; best-effort because D1 restricts some statements. */
export async function analyze(db: D1Database): Promise<boolean> {
  try {
    await db.exec("ANALYZE;");
    return true;
  } catch {
    return false;
  }
}

export function userId(t: number, s: number, S: number): number {
  return (t - 1) * S + s;
}
export function courseId(t: number, c: number, C: number): number {
  return (t - 1) * C + c;
}
export function lessonId(
  t: number,
  c: number,
  l: number,
  C: number,
  L: number,
): number {
  return (t - 1) * C * L + (c - 1) * L + l;
}
export function quizId(t: number, c: number, C: number): number {
  return (t - 1) * C + c;
}
export function questionId(t: number, c: number, q: number, C: number): number {
  return ((t - 1) * C + (c - 1)) * 5 + q;
}

async function flush(
  db: D1Database,
  batch: D1PreparedStatement[],
): Promise<void> {
  if (batch.length === 0) return;
  await db.batch(batch);
}

export async function seedKv(db: D1Database, rows: number): Promise<void> {
  await applySchema(db);
  await resetTables(db, [
    "kv",
    "bench_writes",
    "concurrent_progress",
    "hot_counter",
  ]);
  const payload = "x".repeat(200);
  let batch: D1PreparedStatement[] = [];
  for (let i = 1; i <= rows; i++) {
    batch.push(
      db
        .prepare("INSERT INTO kv (id, payload) VALUES (?, ?)")
        .bind(i, `${i}:${payload}`),
    );
    if (batch.length === 100) {
      await flush(db, batch);
      batch = [];
    }
  }
  await flush(db, batch);
  // Concurrency tables: one progress row per student id, one hot counter row.
  for (let i = 1; i <= rows; i++) {
    batch.push(
      db
        .prepare(
          "INSERT INTO concurrent_progress (student_id, progress, updated_at) VALUES (?, 0, ?)",
        )
        .bind(i, SEED_UPDATED_AT),
    );
    if (batch.length === 100) {
      await flush(db, batch);
      batch = [];
    }
  }
  batch.push(db.prepare("INSERT INTO hot_counter (id, value) VALUES (1, 0)"));
  await flush(db, batch);
}

export interface LmsCounts {
  users: number;
  courses: number;
  lessons: number;
  enrollments: number;
  progress: number;
  quizzes: number;
  questions: number;
  attempts: number;
  orders: number;
}

export async function seedLms(
  db: D1Database,
  d: SeedDims,
  onlyTenant?: number,
): Promise<LmsCounts> {
  const {
    tenants: T,
    studentsPerTenant: S,
    coursesPerTenant: C,
    lessonsPerCourse: L,
  } = d;
  await applySchema(db);
  await resetTables(db, [
    "quiz_answers",
    "quiz_attempts",
    "quiz_questions",
    "quizzes",
    "lesson_progress",
    "enrollments",
    "lessons",
    "courses",
    "users",
    "orders",
  ]);
  const counts: LmsCounts = {
    users: 0,
    courses: 0,
    lessons: 0,
    enrollments: 0,
    progress: 0,
    quizzes: 0,
    questions: 0,
    attempts: 0,
    orders: 0,
  };
  let batch: D1PreparedStatement[] = [];
  const push = async (stmt: D1PreparedStatement): Promise<void> => {
    batch.push(stmt);
    if (batch.length === 50) {
      await flush(db, batch);
      batch = [];
    }
  };

  for (let t = onlyTenant ?? 1; t <= (onlyTenant ?? T); t++) {
    for (let s = 1; s <= S; s++) {
      const uid = userId(t, s, S);
      await push(
        db
          .prepare(
            "INSERT INTO users (id, tenant_id, email, name, role) VALUES (?, ?, ?, ?, 'student')",
          )
          .bind(uid, t, `student${uid}@t${t}.test`, `Student ${uid}`),
      );
      counts.users++;
    }
    for (let c = 1; c <= C; c++) {
      const cid = courseId(t, c, C);
      await push(
        db
          .prepare(
            "INSERT INTO courses (id, tenant_id, title, slug, status, price_cents) VALUES (?, ?, ?, ?, 'published', ?)",
          )
          .bind(cid, t, `Course ${cid}`, `course-${cid}`, 1000 + cid),
      );
      counts.courses++;
      for (let l = 1; l <= L; l++) {
        const lid = lessonId(t, c, l, C, L);
        await push(
          db
            .prepare(
              "INSERT INTO lessons (id, tenant_id, course_id, position, title, body, duration_s) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(
              lid,
              t,
              cid,
              l,
              `Lesson ${lid}`,
              `Body of lesson ${lid}. ` + "y".repeat(120),
              600,
            ),
        );
        counts.lessons++;
      }
      const qid = quizId(t, c, C);
      await push(
        db
          .prepare(
            "INSERT INTO quizzes (id, tenant_id, lesson_id, course_id, title) VALUES (?, ?, ?, ?, ?)",
          )
          .bind(qid, t, lessonId(t, c, 1, C, L), cid, `Quiz ${qid}`),
      );
      counts.quizzes++;
      for (let q = 1; q <= 5; q++) {
        await push(
          db
            .prepare(
              "INSERT INTO quiz_questions (id, tenant_id, quiz_id, position, prompt, kind) VALUES (?, ?, ?, ?, ?, 'mcq')",
            )
            .bind(
              questionId(t, c, q, C),
              t,
              qid,
              q,
              `Question ${q} of quiz ${qid}?`,
            ),
        );
        counts.questions++;
      }
    }
    // Enrollments: 2 courses per student, deterministic spread.
    for (let s = 1; s <= S; s++) {
      const uid = userId(t, s, S);
      const c1 = ((s - 1) % C) + 1;
      const c2 = (s % C) + 1;
      for (const c of new Set([c1, c2, 1])) {
        const cid = courseId(t, c, C);
        await push(
          db
            .prepare(
              "INSERT INTO enrollments (tenant_id, student_id, course_id, progress_pct, status, created_at) VALUES (?, ?, ?, 0, 'active', '2026-01-01T00:00:00.000Z')",
            )
            .bind(t, uid, cid),
        );
        counts.enrollments++;
      }
      if (s % 10 === 1) {
        // Sparse progress: first min(4,L) lessons of each enrolled course.
        for (const c of new Set([c1, c2])) {
          const cid = courseId(t, c, C);
          const n = Math.min(4, L);
          for (let l = 1; l <= n; l++) {
            const lid = lessonId(t, c, l, C, L);
            await push(
              db
                .prepare(
                  "INSERT INTO lesson_progress (tenant_id, student_id, lesson_id, course_id, position, completed, updated_at) VALUES (?, ?, ?, ?, ?, 1, '2026-01-01T00:00:00.000Z')",
                )
                .bind(t, uid, lid, cid, l),
            );
            counts.progress++;
          }
        }
      }
      if (s % 20 === 1) {
        for (const c of new Set([c1, c2])) {
          const qid = quizId(t, c, C);
          await push(
            db
              .prepare(
                "INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, score, state, started_at, updated_at) VALUES (?, ?, ?, NULL, 'started', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z')",
              )
              .bind(t, qid, uid),
          );
          counts.attempts++;
        }
      }
      if (s % 5 === 1) {
        const cid = courseId(t, c1, C);
        await push(
          db
            .prepare(
              "INSERT INTO orders (tenant_id, student_id, course_id, amount_cents, status, created_at) VALUES (?, ?, ?, ?, 'paid', '2026-01-01T00:00:00.000Z')",
            )
            .bind(t, uid, cid, 1000 + cid),
        );
        counts.orders++;
      }
    }
  }
  await flush(db, batch);
  return counts;
}
