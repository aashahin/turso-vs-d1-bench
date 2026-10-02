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

const NOW = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";
interface ScenarioQuery {
  text: string;
  params: unknown[];
  many: boolean;
}

/** The statement list a scenario executes: reads first, then writes. */
export interface ScenarioSql {
  reads: ScenarioQuery[];
  writes: { text: string; params: unknown[] }[];
}

export function scenarioSql(test: string, p: OpParams): ScenarioSql {
  switch (test) {
    case "point-read":
      return {
        reads: [
          {
            text: "SELECT id, payload FROM kv WHERE id = ?",
            params: [p.id],
            many: false,
          },
        ],
        writes: [],
      };
    case "scan":
      return {
        reads: [
          {
            text: "SELECT id, payload FROM kv WHERE id >= ? ORDER BY id LIMIT ?",
            params: [p.startId, p.limit],
            many: true,
          },
        ],
        writes: [],
      };
    case "insert":
      return {
        reads: [],
        writes: [
          {
            text: "INSERT INTO bench_writes (payload) VALUES (?)",
            params: [p.payload],
          },
        ],
      };
    case "update":
      return {
        reads: [],
        writes: [
          {
            text: "UPDATE kv SET payload = ? WHERE id = ?",
            params: [p.payload, p.id],
          },
        ],
      };
    case "student-dashboard":
      return {
        reads: [
          {
            text: "SELECT e.id, e.course_id, e.progress_pct, e.status, c.title, c.slug FROM enrollments e JOIN courses c ON c.id = e.course_id AND c.tenant_id = e.tenant_id WHERE e.tenant_id = ? AND e.student_id = ? AND e.status = 'active' ORDER BY e.id LIMIT 20",
            params: [p.tenant, p.student],
            many: true,
          },
          {
            text: "SELECT lesson_id, course_id, completed, updated_at FROM lesson_progress WHERE tenant_id = ? AND student_id = ? ORDER BY updated_at DESC LIMIT 20",
            params: [p.tenant, p.student],
            many: true,
          },
        ],
        writes: [],
      };
    case "course-page":
      return {
        reads: [
          {
            text: "SELECT id, title, slug, status, price_cents FROM courses WHERE tenant_id = ? AND id = ?",
            params: [p.tenant, p.course],
            many: false,
          },
          {
            text: "SELECT id, position, title, duration_s FROM lessons WHERE tenant_id = ? AND course_id = ? ORDER BY position LIMIT 100",
            params: [p.tenant, p.course],
            many: true,
          },
          {
            text: "SELECT id, progress_pct, status FROM enrollments WHERE tenant_id = ? AND student_id = ? AND course_id = ?",
            params: [p.tenant, p.student, p.course],
            many: false,
          },
          {
            text: "SELECT (SELECT COUNT(*) FROM lesson_progress WHERE tenant_id = ? AND student_id = ? AND course_id = ? AND completed = 1) AS done, (SELECT COUNT(*) FROM lessons WHERE tenant_id = ? AND course_id = ?) AS total",
            params: [p.tenant, p.student, p.course, p.tenant, p.course],
            many: false,
          },
        ],
        writes: [],
      };
    case "lesson-page":
      return {
        reads: [
          {
            text: "SELECT id, course_id, position, title, duration_s FROM lessons WHERE tenant_id = ? AND id = ?",
            params: [p.tenant, p.lesson],
            many: false,
          },
          {
            text: "SELECT id, title, slug FROM courses WHERE tenant_id = ? AND id = ?",
            params: [p.tenant, p.course],
            many: false,
          },
          {
            text: "SELECT completed, position FROM lesson_progress WHERE tenant_id = ? AND student_id = ? AND lesson_id = ?",
            params: [p.tenant, p.student, p.lesson],
            many: false,
          },
          {
            text: "SELECT id, position, title FROM lessons WHERE tenant_id = ? AND course_id = ? AND position >= ? ORDER BY position LIMIT 3",
            params: [p.tenant, p.course, p.position],
            many: true,
          },
        ],
        writes: [],
      };
    case "quiz-page":
      return {
        reads: [
          {
            text: "SELECT id, lesson_id, course_id, title FROM quizzes WHERE tenant_id = ? AND id = ?",
            params: [p.tenant, p.quiz],
            many: false,
          },
          {
            text: "SELECT id, position, prompt, kind FROM quiz_questions WHERE tenant_id = ? AND quiz_id = ? ORDER BY position",
            params: [p.tenant, p.quiz],
            many: true,
          },
          {
            text: "SELECT id, score, state, updated_at FROM quiz_attempts WHERE tenant_id = ? AND quiz_id = ? AND student_id = ? ORDER BY id DESC LIMIT 1",
            params: [p.tenant, p.quiz, p.student],
            many: false,
          },
        ],
        writes: [],
      };
    case "submit-quiz-answer":
      return {
        reads: [
          {
            text: "SELECT id, state FROM quiz_attempts WHERE tenant_id = ? AND id = ?",
            params: [p.tenant, p.attempt],
            many: false,
          },
        ],
        writes: [
          {
            text: "INSERT INTO quiz_answers (tenant_id, attempt_id, question_id, answer, is_correct) VALUES (?, ?, ?, ?, ?)",
            params: [p.tenant, p.attempt, p.question, p.answer, 0],
          },
          {
            text: `UPDATE quiz_attempts SET updated_at = ${NOW}, state = 'in_progress' WHERE tenant_id = ? AND id = ?`,
            params: [p.tenant, p.attempt],
          },
        ],
      };
    case "update-progress":
      return {
        reads: [],
        writes: [
          {
            text: `INSERT INTO lesson_progress (tenant_id, student_id, lesson_id, course_id, position, completed, updated_at) VALUES (?, ?, ?, ?, ?, ?, ${NOW}) ON CONFLICT (tenant_id, student_id, lesson_id) DO UPDATE SET completed = excluded.completed, updated_at = ${NOW}`,
            params: [
              p.tenant,
              p.student,
              p.lesson,
              p.course,
              p.position,
              p.completed,
            ],
          },
          {
            text: "UPDATE enrollments SET progress_pct = (SELECT CAST(100.0 * COUNT(*) / MAX(1, (SELECT COUNT(*) FROM lessons WHERE course_id = lesson_progress.course_id)) AS INTEGER) FROM lesson_progress WHERE tenant_id = ? AND student_id = ? AND course_id = ? AND completed = 1) WHERE tenant_id = ? AND student_id = ? AND course_id = ?",
            params: [
              p.tenant,
              p.student,
              p.course,
              p.tenant,
              p.student,
              p.course,
            ],
          },
        ],
      };
    case "independent-writes":
      // Row-level MVCC target: each request targets a different student row.
      return {
        reads: [
          {
            text: "SELECT progress FROM concurrent_progress WHERE student_id = ?",
            params: [p.student],
            many: false,
          },
        ],
        writes: [
          {
            text: `UPDATE concurrent_progress SET progress = progress + 1, updated_at = ${NOW} WHERE student_id = ?`,
            params: [p.student],
          },
        ],
      };
    case "hot-row-write":
      // Write/write conflict target: every request updates the same row.
      return {
        reads: [
          {
            text: "SELECT value FROM hot_counter WHERE id = 1",
            params: [],
            many: false,
          },
        ],
        writes: [
          {
            text: "UPDATE hot_counter SET value = value + 1 WHERE id = 1",
            params: [],
          },
        ],
      };
    case "order-event":
      return {
        reads: [],
        writes: [
          {
            text: "INSERT INTO orders (tenant_id,student_id,course_id,amount_cents,status) VALUES (?,?,?,?,?)",
            params: [p.tenant, p.student, p.course, 1000, "paid"],
          },
        ],
      };
    case "exam-submit": {
      const progress = scenarioSql("update-progress", p);
      const answer = scenarioSql("submit-quiz-answer", p);
      return {
        reads: answer.reads,
        writes: [...progress.writes, ...answer.writes],
      };
    }
    case "enrollment":
      return {
        reads: [],
        writes: [
          {
            text: `INSERT INTO enrollments (tenant_id, student_id, course_id, progress_pct, status, created_at) VALUES (?, ?, ?, 0, 'active', ${NOW}) ON CONFLICT (tenant_id, student_id, course_id) DO NOTHING`,
            params: [p.tenant, p.student, p.course],
          },
          {
            text: "SELECT id, progress_pct, status FROM enrollments WHERE tenant_id = ? AND student_id = ? AND course_id = ?",
            params: [p.tenant, p.student, p.course],
          },
        ],
      };
    default:
      throw new Error(`unknown test ${JSON.stringify(test)}`);
  }
}
