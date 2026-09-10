// Client-side test classification, shared by the orchestrator and the
// reporters so a test is never treated as a read in one place and a write in
// another. The Worker has its own copy in worker/src/backends.ts (it cannot
// import client code); keep the two in sync.

export const KV_TESTS: Readonly<Record<string, true>> = { "point-read": true, scan: true, insert: true, update: true };

/** Row-level MVCC / write-conflict benchmarks (read-then-write transactions). */
export const CONCURRENCY_TESTS: Readonly<Record<string, true>> = { "independent-writes": true, "hot-row-write": true };

export const WRITE_TESTS: Readonly<Record<string, true>> = {
  insert: true,
  update: true,
  "submit-quiz-answer": true,
  "update-progress": true,
  enrollment: true,
  ...CONCURRENCY_TESTS,
};

export const READ_TESTS: Readonly<Record<string, true>> = {
  "point-read": true,
  scan: true,
  "student-dashboard": true,
  "course-page": true,
  "lesson-page": true,
  "quiz-page": true,
};
