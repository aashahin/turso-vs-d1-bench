import { hash32, type Dims, type OpQuery } from "./common.ts";
export const LMS_PROFILES: Record<string, Record<string, number>> = {
  "lms-balanced": {
    "lesson-page": 40,
    "course-page": 20,
    "student-dashboard": 15,
    "update-progress": 10,
    "quiz-page": 10,
    "submit-quiz-answer": 5,
  },
  "lms-read-heavy": {
    "lesson-page": 50,
    "course-page": 25,
    "student-dashboard": 15,
    "quiz-page": 5,
    "update-progress": 4,
    "submit-quiz-answer": 1,
  },
  "lms-write-heavy": {
    "lesson-page": 10,
    "student-dashboard": 5,
    "update-progress": 35,
    "submit-quiz-answer": 30,
    enrollment: 10,
    "order-event": 10,
  },
};
export function operationSelector(
  mix: Record<string, number>,
  seed = 42,
): (index: number) => string {
  if (
    Object.keys(mix).some(
      (name) =>
        ![
          "lesson-page",
          "course-page",
          "student-dashboard",
          "quiz-page",
          "update-progress",
          "submit-quiz-answer",
          "enrollment",
          "order-event",
        ].includes(name),
    ) ||
    Object.values(mix).some((v) => !Number.isInteger(v) || v < 0) ||
    Object.values(mix).reduce((a, b) => a + b, 0) !== 100
  )
    throw new Error(
      "operation mix must contain integer percentages summing to 100",
    );
  const deck = Object.entries(mix).flatMap(([name, n]) =>
    Array<string>(n).fill(name),
  );
  // Deterministic permutation within each block gives exact requested counts per 100 offers.
  return (index) => {
    const block = Math.floor(index / 100);
    const shift = hash32(block ^ seed) % 100;
    return deck[(((index * 37 + shift) % 100) + 100) % 100]!;
  };
}
export function examOp(index: number, tenant: number, d: Dims): OpQuery {
  if (index >= d.studentsPerTenant)
    throw new Error("exam students exceed seed dimensions");
  const student = (tenant - 1) * d.studentsPerTenant + index + 1;
  const course = (tenant - 1) * d.coursesPerTenant + 1;
  return {
    test: "exam-submit",
    method: "POST",
    params: {
      tenant,
      student,
      course,
      lesson: (course - 1) * d.lessonsPerCourse + 1,
      quiz: course,
      question: (course - 1) * 5 + 1,
      attempt: 0,
      completed: 1,
      position: 1,
      answer: "B",
    },
  };
}

export function profileMix(
  test: string,
  workload: string,
  custom: Record<string, number> | null,
): Record<string, number> | null {
  const standard =
    LMS_PROFILES[test] ??
    (test === "mixed" && workload === "lms"
      ? LMS_PROFILES["lms-balanced"]
      : null);
  return standard ? (custom ?? standard) : null;
}
