export const SCHEMA_VERSION = 2;
export const SCHEMA_SQL =
  "\nCREATE TABLE IF NOT EXISTS kv (\n  id INTEGER PRIMARY KEY,\n  payload TEXT NOT NULL\n);\n\nCREATE TABLE IF NOT EXISTS bench_writes (\n  id INTEGER PRIMARY KEY,\n  payload TEXT NOT NULL,\n  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))\n);\n\nCREATE TABLE IF NOT EXISTS concurrent_progress (\n  student_id INTEGER PRIMARY KEY,\n  progress INTEGER NOT NULL DEFAULT 0,\n  updated_at TEXT NOT NULL\n);\n\nCREATE TABLE IF NOT EXISTS hot_counter (\n  id INTEGER PRIMARY KEY,\n  value INTEGER NOT NULL DEFAULT 0\n);\n\nCREATE TABLE IF NOT EXISTS users (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  email TEXT NOT NULL,\n  name TEXT NOT NULL,\n  role TEXT NOT NULL DEFAULT 'student'\n);\nCREATE INDEX IF NOT EXISTS idx_users_tenant ON users (tenant_id, id);\nCREATE INDEX IF NOT EXISTS idx_users_tenant_role ON users (tenant_id, role, id);\n\nCREATE TABLE IF NOT EXISTS courses (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  title TEXT NOT NULL,\n  slug TEXT NOT NULL,\n  status TEXT NOT NULL DEFAULT 'published',\n  price_cents INTEGER NOT NULL DEFAULT 0\n);\nCREATE INDEX IF NOT EXISTS idx_courses_tenant ON courses (tenant_id, id);\n\nCREATE TABLE IF NOT EXISTS lessons (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  course_id INTEGER NOT NULL,\n  position INTEGER NOT NULL,\n  title TEXT NOT NULL,\n  body TEXT NOT NULL,\n  duration_s INTEGER NOT NULL DEFAULT 600\n);\nCREATE INDEX IF NOT EXISTS idx_lessons_course ON lessons (tenant_id, course_id, position);\nCREATE INDEX IF NOT EXISTS idx_lessons_tenant ON lessons (tenant_id, id);\n\nCREATE TABLE IF NOT EXISTS enrollments (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  student_id INTEGER NOT NULL,\n  course_id INTEGER NOT NULL,\n  progress_pct INTEGER NOT NULL DEFAULT 0,\n  status TEXT NOT NULL DEFAULT 'active',\n  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),\n  UNIQUE (tenant_id, student_id, course_id)\n);\nCREATE INDEX IF NOT EXISTS idx_enroll_student ON enrollments (tenant_id, student_id);\nCREATE INDEX IF NOT EXISTS idx_enroll_course ON enrollments (tenant_id, course_id);\n\nCREATE TABLE IF NOT EXISTS lesson_progress (\n  tenant_id INTEGER NOT NULL,\n  student_id INTEGER NOT NULL,\n  lesson_id INTEGER NOT NULL,\n  course_id INTEGER NOT NULL,\n  position INTEGER NOT NULL DEFAULT 0,\n  completed INTEGER NOT NULL DEFAULT 0,\n  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),\n  PRIMARY KEY (tenant_id, student_id, lesson_id)\n);\nCREATE INDEX IF NOT EXISTS idx_progress_student ON lesson_progress (tenant_id, student_id);\n\nCREATE TABLE IF NOT EXISTS quizzes (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  lesson_id INTEGER NOT NULL,\n  course_id INTEGER NOT NULL,\n  title TEXT NOT NULL\n);\nCREATE INDEX IF NOT EXISTS idx_quizzes_lesson ON quizzes (tenant_id, lesson_id);\n\nCREATE TABLE IF NOT EXISTS quiz_questions (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  quiz_id INTEGER NOT NULL,\n  position INTEGER NOT NULL,\n  prompt TEXT NOT NULL,\n  kind TEXT NOT NULL DEFAULT 'mcq'\n);\nCREATE INDEX IF NOT EXISTS idx_questions_quiz ON quiz_questions (tenant_id, quiz_id, position);\n\nCREATE TABLE IF NOT EXISTS quiz_attempts (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  quiz_id INTEGER NOT NULL,\n  student_id INTEGER NOT NULL,\n  score INTEGER,\n  state TEXT NOT NULL DEFAULT 'started',\n  started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),\n  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))\n);\nCREATE INDEX IF NOT EXISTS idx_attempts_quiz_student ON quiz_attempts (tenant_id, quiz_id, student_id);\n\nCREATE TABLE IF NOT EXISTS quiz_answers (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  attempt_id INTEGER NOT NULL,\n  question_id INTEGER NOT NULL,\n  answer TEXT NOT NULL,\n  is_correct INTEGER NOT NULL DEFAULT 0\n);\nCREATE INDEX IF NOT EXISTS idx_answers_attempt ON quiz_answers (tenant_id, attempt_id);\n\nCREATE TABLE IF NOT EXISTS orders (\n  id INTEGER PRIMARY KEY,\n  tenant_id INTEGER NOT NULL,\n  student_id INTEGER NOT NULL,\n  course_id INTEGER NOT NULL,\n  amount_cents INTEGER NOT NULL,\n  status TEXT NOT NULL DEFAULT 'paid',\n  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))\n);\nCREATE INDEX IF NOT EXISTS idx_orders_student ON orders (tenant_id, student_id);\n\nCREATE TABLE IF NOT EXISTS bench_identity (\n  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),\n  database_id TEXT NOT NULL,\n  engine TEXT NOT NULL,\n  schema_version INTEGER NOT NULL,\n  seed_dims TEXT NOT NULL,\n  tenant INTEGER\n);";
export interface SchemaEntry {
  type: string;
  name: string;
  tbl_name: string;
  sql: string | null;
}
export function normalizeSql(sql: string): string {
  return sql
    .replace(/IF NOT EXISTS/gi, "")
    .replace(/\s+/g, "")
    .replace(/["`]/g, "")
    .toLowerCase();
}
export async function schemaFingerprint(
  entries: SchemaEntry[],
): Promise<string> {
  const canonical = entries
    .filter((e) => !e.name.startsWith("sqlite_") && !e.name.startsWith("_cf_") && !e.name.startsWith("__turso_internal_"))
    .sort((a, b) => a.name.localeCompare(b.name))
    .map((e) => [
      e.type,
      e.name,
      e.tbl_name,
      e.sql ? normalizeSql(e.sql) : null,
    ]);
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(canonical)),
  );
  return Array.from(new Uint8Array(digest), (b) =>
    b.toString(16).padStart(2, "0"),
  ).join("");
}
