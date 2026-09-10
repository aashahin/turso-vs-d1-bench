// Application-style LMS operations. ID layout mirrors worker/src/seed.ts so
// every generated ID is valid; submit-quiz-answer targets students that own
// attempts (s%20==1) and passes attempt=0 so the Worker resolves the latest
// attempt for that student/quiz (attempt rowids are autoincrement and cannot
// be derived arithmetically).
import { hash32, payloadFor, type Dims, type OpQuery } from "./common.ts";

const QUESTIONS_PER_QUIZ = 5;

export function lmsOp(test: string, opIndex: number, tenant: number, dims: Dims): OpQuery {
  const S = dims.studentsPerTenant;
  const C = dims.coursesPerTenant;
  const L = dims.lessonsPerCourse;
  const h = (salt: number): number => hash32((opIndex ^ salt) >>> 0);
  const student = (t: number, s: number): number => (t - 1) * S + s;
  const course = (t: number, c: number): number => (t - 1) * C + c;
  const lesson = (t: number, c: number, l: number): number => (t - 1) * C * L + (c - 1) * L + l;

  if (test === "student-dashboard") {
    const s = 1 + (h(0xa1) % S);
    return { test, method: "GET", params: { tenant, student: student(tenant, s) } };
  }
  if (test === "course-page") {
    const c = 1 + (h(0xa2) % C);
    const s = 1 + (h(0xa3) % S);
    return { test, method: "GET", params: { tenant, course: course(tenant, c), student: student(tenant, s) } };
  }
  if (test === "lesson-page") {
    const c = 1 + (h(0xa4) % C);
    const l = 1 + (h(0xa5) % L);
    const s = 1 + (h(0xa6) % S);
    return {
      test,
      method: "GET",
      params: { tenant, lesson: lesson(tenant, c, l), course: course(tenant, c), student: student(tenant, s), position: l },
    };
  }
  if (test === "quiz-page") {
    const c = 1 + (h(0xa7) % C);
    const s = 1 + (h(0xa8) % S);
    return { test, method: "GET", params: { tenant, quiz: course(tenant, c), student: student(tenant, s) } };
  }
  if (test === "submit-quiz-answer") {
    const c = 1 + (h(0xa9) % C);
    // Attempt owners only: s%20==1 keeps the get-attempt hit rate realistic.
    const s = 1 + (h(0xaa) % Math.max(1, Math.floor(S / 20))) * 20;
    const q = 1 + (h(0xab) % QUESTIONS_PER_QUIZ);
    return {
      test,
      method: "POST",
      params: {
        tenant,
        quiz: course(tenant, c),
        student: student(tenant, Math.min(s, S)),
        question: ((tenant - 1) * C + (c - 1)) * QUESTIONS_PER_QUIZ + q,
        attempt: 0,
        answer: `opt-${1 + (h(0xac) % 4)}`,
      },
    };
  }
  if (test === "update-progress") {
    const c = 1 + (h(0xad) % C);
    const l = 1 + (h(0xae) % L);
    const s = 1 + (h(0xaf) % S);
    return {
      test,
      method: "POST",
      params: {
        tenant,
        student: student(tenant, s),
        lesson: lesson(tenant, c, l),
        course: course(tenant, c),
        position: l,
        completed: h(0xb0) % 2,
        payload: payloadFor(opIndex, 16),
      },
    };
  }
  // enrollment: random student + course (ON CONFLICT DO NOTHING keeps reruns fair).
  const c = 1 + (h(0xb1) % C);
  const s = 1 + (h(0xb2) % S);
  return { test: "enrollment", method: "POST", params: { tenant, student: student(tenant, s), course: course(tenant, c) } };
}

// Default LMS mix: 40% lesson-page, 20% course-page, 15% student-dashboard,
// 10% update-progress, 10% quiz-page, 5% submit-quiz-answer.
export function lmsMixedTest(opIndex: number): string {
  const r = hash32((opIndex ^ 0x51ab) >>> 0) % 100;
  if (r < 40) return "lesson-page";
  if (r < 60) return "course-page";
  if (r < 75) return "student-dashboard";
  if (r < 85) return "update-progress";
  if (r < 95) return "quiz-page";
  return "submit-quiz-answer";
}

// Default KV mix: 70% point-reads, 15% scans, 10% updates, 5% inserts.
export function kvMixedTest(opIndex: number): string {
  const r = hash32((opIndex ^ 0x6b7d) >>> 0) % 100;
  if (r < 70) return "point-read";
  if (r < 85) return "scan";
  if (r < 95) return "update";
  return "insert";
}
