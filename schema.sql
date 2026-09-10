-- Shared schema for Turso vs D1 benchmark. Identical tables on both sides.
-- fairness: same DDL, same payload sizes, same deterministic seed data.
-- KV tables exercise trivial lookups; LMS tables exercise a realistic
-- multi-tenant workload (tenant_id scoping on every query).

CREATE TABLE IF NOT EXISTS kv (
  id INTEGER PRIMARY KEY,
  payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bench_writes (
  id INTEGER PRIMARY KEY,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Concurrency tables for the tursodb MVCC benchmarks. Same DDL on every
-- backend so `independent-writes` (row-level MVCC benefit) and `hot-row-write`
-- (write/write conflict) measure the same logical work everywhere.
-- concurrent_progress has one row per student id (1..--rows); hot_counter has
-- a single row (id=1) that every hot-row request updates.
CREATE TABLE IF NOT EXISTS concurrent_progress (
  student_id INTEGER PRIMARY KEY,
  progress INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hot_counter (
  id INTEGER PRIMARY KEY,
  value INTEGER NOT NULL DEFAULT 0
);

-- Multi-tenant LMS (small but realistic: users/courses/lessons/enrollments).
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  email TEXT NOT NULL,
  name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'student'
);
CREATE INDEX IF NOT EXISTS idx_users_tenant ON users (tenant_id, id);
CREATE INDEX IF NOT EXISTS idx_users_tenant_role ON users (tenant_id, role, id);

CREATE TABLE IF NOT EXISTS courses (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  slug TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'published',
  price_cents INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_courses_tenant ON courses (tenant_id, id);

CREATE TABLE IF NOT EXISTS lessons (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  course_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  duration_s INTEGER NOT NULL DEFAULT 600
);
CREATE INDEX IF NOT EXISTS idx_lessons_course ON lessons (tenant_id, course_id, position);
CREATE INDEX IF NOT EXISTS idx_lessons_tenant ON lessons (tenant_id, id);

CREATE TABLE IF NOT EXISTS enrollments (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  student_id INTEGER NOT NULL,
  course_id INTEGER NOT NULL,
  progress_pct INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  UNIQUE (tenant_id, student_id, course_id)
);
CREATE INDEX IF NOT EXISTS idx_enroll_student ON enrollments (tenant_id, student_id);
CREATE INDEX IF NOT EXISTS idx_enroll_course ON enrollments (tenant_id, course_id);

CREATE TABLE IF NOT EXISTS lesson_progress (
  tenant_id INTEGER NOT NULL,
  student_id INTEGER NOT NULL,
  lesson_id INTEGER NOT NULL,
  course_id INTEGER NOT NULL,
  position INTEGER NOT NULL DEFAULT 0,
  completed INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (tenant_id, student_id, lesson_id)
);
CREATE INDEX IF NOT EXISTS idx_progress_student ON lesson_progress (tenant_id, student_id);

CREATE TABLE IF NOT EXISTS quizzes (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  lesson_id INTEGER NOT NULL,
  course_id INTEGER NOT NULL,
  title TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quizzes_lesson ON quizzes (tenant_id, lesson_id);

CREATE TABLE IF NOT EXISTS quiz_questions (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  quiz_id INTEGER NOT NULL,
  position INTEGER NOT NULL,
  prompt TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'mcq'
);
CREATE INDEX IF NOT EXISTS idx_questions_quiz ON quiz_questions (tenant_id, quiz_id, position);

CREATE TABLE IF NOT EXISTS quiz_attempts (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  quiz_id INTEGER NOT NULL,
  student_id INTEGER NOT NULL,
  score INTEGER,
  state TEXT NOT NULL DEFAULT 'started',
  started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_attempts_quiz_student ON quiz_attempts (tenant_id, quiz_id, student_id);

CREATE TABLE IF NOT EXISTS quiz_answers (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  attempt_id INTEGER NOT NULL,
  question_id INTEGER NOT NULL,
  answer TEXT NOT NULL,
  is_correct INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_answers_attempt ON quiz_answers (tenant_id, attempt_id);

CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY,
  tenant_id INTEGER NOT NULL,
  student_id INTEGER NOT NULL,
  course_id INTEGER NOT NULL,
  amount_cents INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'paid',
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_orders_student ON orders (tenant_id, student_id);
