// Test classification shared by the Worker (which executes the SQL) and the
// runner + reporters (which build the matrix and label the results). One copy,
// so a test can never be a write on one side and a read on the other.
//
// `Record<string, true>` rather than `Set`: these are static lookup tables.

export const KV_TESTS: Readonly<Record<string, true>> = { "point-read": true, scan: true, insert: true, update: true };

/** Row-level MVCC / write-conflict benchmarks (read-then-write transactions). */
export const CONCURRENCY_TESTS: Readonly<Record<string, true>> = { "independent-writes": true, "hot-row-write": true };

/** Tests that mutate data (POST + admin auth in edge mode). */
export const WRITE_TESTS: Readonly<Record<string, true>> = {
  insert: true,
  update: true,
  "submit-quiz-answer": true,
  "update-progress": true,
  enrollment: true,
  ...CONCURRENCY_TESTS,
};

/**
 * Write tests whose logical operation is a read-then-write transaction rather
 * than a single autocommit statement. Non-MVCC backends run these in their
 * normal transaction equivalent; `tursodb-concurrent` uses BEGIN CONCURRENT.
 * Same members as CONCURRENCY_TESTS today, but a different rule.
 */
export const TX_WRITE_TESTS: Readonly<Record<string, true>> = { "independent-writes": true, "hot-row-write": true };

export const READ_TESTS: Readonly<Record<string, true>> = {
  "point-read": true,
  scan: true,
  "student-dashboard": true,
  "course-page": true,
  "lesson-page": true,
  "quiz-page": true,
};

/**
 * Scan scenarios pin their limit in the test name (`scan-100`), so report
 * classification must go through this predicate instead of a raw table lookup.
 */
export function isReadTest(test: string): boolean {
  return READ_TESTS[test] === true || READ_TESTS[test.replace(/^scan-\d+$/, "scan")] === true;
}
