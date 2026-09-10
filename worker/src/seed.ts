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

export const LMS_DDL =
  "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, email TEXT NOT NULL, name TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'student');" +
  "CREATE INDEX IF NOT EXISTS idx_users_tenant ON users (tenant_id, id);" +
  "CREATE INDEX IF NOT EXISTS idx_users_tenant_role ON users (tenant_id, role, id);" +
  "CREATE TABLE IF NOT EXISTS courses (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, title TEXT NOT NULL, slug TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'published', price_cents INTEGER NOT NULL DEFAULT 0);" +
  "CREATE INDEX IF NOT EXISTS idx_courses_tenant ON courses (tenant_id, id);" +
  "CREATE TABLE IF NOT EXISTS lessons (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, course_id INTEGER NOT NULL, position INTEGER NOT NULL, title TEXT NOT NULL, body TEXT NOT NULL, duration_s INTEGER NOT NULL DEFAULT 600);" +
  "CREATE INDEX IF NOT EXISTS idx_lessons_course ON lessons (tenant_id, course_id, position);" +
  "CREATE INDEX IF NOT EXISTS idx_lessons_tenant ON lessons (tenant_id, id);" +
  "CREATE TABLE IF NOT EXISTS enrollments (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, student_id INTEGER NOT NULL, course_id INTEGER NOT NULL, progress_pct INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), UNIQUE (tenant_id, student_id, course_id));" +
  "CREATE INDEX IF NOT EXISTS idx_enroll_student ON enrollments (tenant_id, student_id);" +
  "CREATE INDEX IF NOT EXISTS idx_enroll_course ON enrollments (tenant_id, course_id);" +
  "CREATE TABLE IF NOT EXISTS lesson_progress (tenant_id INTEGER NOT NULL, student_id INTEGER NOT NULL, lesson_id INTEGER NOT NULL, course_id INTEGER NOT NULL, position INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), PRIMARY KEY (tenant_id, student_id, lesson_id));" +
  "CREATE INDEX IF NOT EXISTS idx_progress_student ON lesson_progress (tenant_id, student_id);" +
  "CREATE TABLE IF NOT EXISTS quizzes (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, lesson_id INTEGER NOT NULL, course_id INTEGER NOT NULL, title TEXT NOT NULL);" +
  "CREATE INDEX IF NOT EXISTS idx_quizzes_lesson ON quizzes (tenant_id, lesson_id);" +
  "CREATE TABLE IF NOT EXISTS quiz_questions (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, quiz_id INTEGER NOT NULL, position INTEGER NOT NULL, prompt TEXT NOT NULL, kind TEXT NOT NULL DEFAULT 'mcq');" +
  "CREATE INDEX IF NOT EXISTS idx_questions_quiz ON quiz_questions (tenant_id, quiz_id, position);" +
  "CREATE TABLE IF NOT EXISTS quiz_attempts (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, quiz_id INTEGER NOT NULL, student_id INTEGER NOT NULL, score INTEGER, state TEXT NOT NULL DEFAULT 'started', started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')));" +
  "CREATE INDEX IF NOT EXISTS idx_attempts_quiz_student ON quiz_attempts (tenant_id, quiz_id, student_id);" +
  "CREATE TABLE IF NOT EXISTS quiz_answers (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, attempt_id INTEGER NOT NULL, question_id INTEGER NOT NULL, answer TEXT NOT NULL, is_correct INTEGER NOT NULL DEFAULT 0);" +
  "CREATE INDEX IF NOT EXISTS idx_answers_attempt ON quiz_answers (tenant_id, attempt_id);" +
  "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL, student_id INTEGER NOT NULL, course_id INTEGER NOT NULL, amount_cents INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'paid', created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')));" +
  "CREATE INDEX IF NOT EXISTS idx_orders_student ON orders (tenant_id, student_id);";

export const KV_DDL =
  "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);" +
  "CREATE TABLE IF NOT EXISTS bench_writes (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')));" +
  "CREATE TABLE IF NOT EXISTS concurrent_progress (student_id INTEGER PRIMARY KEY, progress INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);" +
  "CREATE TABLE IF NOT EXISTS hot_counter (id INTEGER PRIMARY KEY, value INTEGER NOT NULL DEFAULT 0);";

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
export function lessonId(t: number, c: number, l: number, C: number, L: number): number {
  return (t - 1) * C * L + (c - 1) * L + l;
}
export function quizId(t: number, c: number, C: number): number {
  return (t - 1) * C + c;
}
export function questionId(t: number, c: number, q: number, C: number): number {
  return ((t - 1) * C + (c - 1)) * 5 + q;
}

async function flush(db: D1Database, batch: D1PreparedStatement[]): Promise<void> {
  if (batch.length === 0) return;
  await db.batch(batch);
}

export async function seedKv(db: D1Database, rows: number): Promise<void> {
  await db.exec(`${KV_DDL}DELETE FROM kv; DELETE FROM bench_writes; DELETE FROM concurrent_progress; DELETE FROM hot_counter;`);
  const payload = "x".repeat(200);
  let batch: D1PreparedStatement[] = [];
  for (let i = 1; i <= rows; i++) {
    batch.push(db.prepare("INSERT INTO kv (id, payload) VALUES (?, ?)").bind(i, `${i}:${payload}`));
    if (batch.length === 100) {
      await flush(db, batch);
      batch = [];
    }
  }
  await flush(db, batch);
  // Concurrency tables: one progress row per student id, one hot counter row.
  for (let i = 1; i <= rows; i++) {
    batch.push(db.prepare("INSERT INTO concurrent_progress (student_id, progress, updated_at) VALUES (?, 0, ?)").bind(i, SEED_UPDATED_AT));
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

export async function seedLms(db: D1Database, d: SeedDims): Promise<LmsCounts> {
  const { tenants: T, studentsPerTenant: S, coursesPerTenant: C, lessonsPerCourse: L } = d;
  await db.exec(
    `${LMS_DDL}DELETE FROM quiz_answers; DELETE FROM quiz_attempts; DELETE FROM quiz_questions; DELETE FROM quizzes; DELETE FROM lesson_progress; DELETE FROM enrollments; DELETE FROM lessons; DELETE FROM courses; DELETE FROM users; DELETE FROM orders;`,
  );
  const counts: LmsCounts = { users: 0, courses: 0, lessons: 0, enrollments: 0, progress: 0, quizzes: 0, questions: 0, attempts: 0, orders: 0 };
  let batch: D1PreparedStatement[] = [];
  const push = async (stmt: D1PreparedStatement): Promise<void> => {
    batch.push(stmt);
    if (batch.length === 50) {
      await flush(db, batch);
      batch = [];
    }
  };

  for (let t = 1; t <= T; t++) {
    for (let s = 1; s <= S; s++) {
      const uid = userId(t, s, S);
      await push(db.prepare("INSERT INTO users (id, tenant_id, email, name, role) VALUES (?, ?, ?, ?, 'student')").bind(uid, t, `student${uid}@t${t}.test`, `Student ${uid}`));
      counts.users++;
    }
    for (let c = 1; c <= C; c++) {
      const cid = courseId(t, c, C);
      await push(db.prepare("INSERT INTO courses (id, tenant_id, title, slug, status, price_cents) VALUES (?, ?, ?, ?, 'published', ?)").bind(cid, t, `Course ${cid}`, `course-${cid}`, 1000 + cid));
      counts.courses++;
      for (let l = 1; l <= L; l++) {
        const lid = lessonId(t, c, l, C, L);
        await push(db.prepare("INSERT INTO lessons (id, tenant_id, course_id, position, title, body, duration_s) VALUES (?, ?, ?, ?, ?, ?, ?)").bind(lid, t, cid, l, `Lesson ${lid}`, `Body of lesson ${lid}. ` + "y".repeat(120), 600));
        counts.lessons++;
      }
      const qid = quizId(t, c, C);
      await push(db.prepare("INSERT INTO quizzes (id, tenant_id, lesson_id, course_id, title) VALUES (?, ?, ?, ?, ?)").bind(qid, t, lessonId(t, c, 1, C, L), cid, `Quiz ${qid}`));
      counts.quizzes++;
      for (let q = 1; q <= 5; q++) {
        await push(db.prepare("INSERT INTO quiz_questions (id, tenant_id, quiz_id, position, prompt, kind) VALUES (?, ?, ?, ?, ?, 'mcq')").bind(questionId(t, c, q, C), t, qid, q, `Question ${q} of quiz ${qid}?`));
        counts.questions++;
      }
    }
    // Enrollments: 2 courses per student, deterministic spread.
    for (let s = 1; s <= S; s++) {
      const uid = userId(t, s, S);
      const c1 = ((s - 1) % C) + 1;
      const c2 = (s % C) + 1;
      for (const c of [c1, c2]) {
        const cid = courseId(t, c, C);
        await push(db.prepare("INSERT INTO enrollments (tenant_id, student_id, course_id, progress_pct, status) VALUES (?, ?, ?, 0, 'active')").bind(t, uid, cid));
        counts.enrollments++;
      }
      if (s % 10 === 1) {
        // Sparse progress: first min(4,L) lessons of each enrolled course.
        for (const c of [c1, c2]) {
          const cid = courseId(t, c, C);
          const n = Math.min(4, L);
          for (let l = 1; l <= n; l++) {
            const lid = lessonId(t, c, l, C, L);
            await push(db.prepare("INSERT INTO lesson_progress (tenant_id, student_id, lesson_id, course_id, position, completed) VALUES (?, ?, ?, ?, ?, 1)").bind(t, uid, lid, cid, l));
            counts.progress++;
          }
        }
      }
      if (s % 20 === 1) {
        for (const c of [c1, c2]) {
          const qid = quizId(t, c, C);
          await push(db.prepare("INSERT INTO quiz_attempts (tenant_id, quiz_id, student_id, score, state) VALUES (?, ?, ?, NULL, 'started')").bind(t, qid, uid));
          counts.attempts++;
        }
      }
      if (s % 5 === 1) {
        const cid = courseId(t, c1, C);
        await push(db.prepare("INSERT INTO orders (tenant_id, student_id, course_id, amount_cents, status) VALUES (?, ?, ?, ?, 'paid')").bind(t, uid, cid, 1000 + cid));
        counts.orders++;
      }
    }
  }
  await flush(db, batch);
  return counts;
}
