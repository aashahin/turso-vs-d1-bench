import { describe, expect, test } from "bun:test";
import { kvOp, lmsOp, writeOp } from "../src/workloads/index.ts";
import { tenantFor } from "../src/workloads/common.ts";
import { dims } from "./helpers.ts";

describe("deterministic workload fairness", () => {
  test.each([1, 2, 17, 100])(
    "independent writes visit each of %i rows once before wrapping",
    (seedRows) => {
      const make = (index: number) =>
        writeOp("independent-writes", index, 1, { ...dims, seedRows });
      const cycle = Array.from({ length: seedRows }, (_, i) =>
        Number(make(i).params.student),
      );
      expect(new Set(cycle).size).toBe(seedRows);
      expect(Math.min(...cycle)).toBe(1);
      expect(Math.max(...cycle)).toBe(seedRows);
      expect(make(seedRows)).toEqual(make(0));
      expect(make(seedRows - 1).method).toBe("POST");
    },
  );

  test("hot-row writes keep every operation on the same row", () => {
    expect(writeOp("hot-row-write", 10, 1, dims)).toEqual(
      writeOp("hot-row-write", 200, 1, dims),
    );
  });

  test("scans can return the requested number of seeded rows and replay out of order", () => {
    const indices = [0, 1, 2, 42, 101, 9999];
    const reference = indices.map((i) => kvOp("scan", i, 1, dims));
    const reordered = [...indices]
      .reverse()
      .map((i) => kvOp("scan", i, 1, dims))
      .reverse();
    expect(reordered).toEqual(reference);
    for (const op of reference) {
      expect(Number(op.params.startId)).toBeGreaterThanOrEqual(1);
      expect(
        Number(op.params.startId) + Number(op.params.limit) - 1,
      ).toBeLessThanOrEqual(dims.seedRows);
    }
  });

  test("distributed lesson reads stay inside their tenant and course ranges", () => {
    const seen = new Set<number>();
    for (let i = 0; i < 100; i++) {
      const tenant = tenantFor(i, "distributed", dims.tenants);
      seen.add(tenant);
      const { params: p } = lmsOp("lesson-page", i, tenant, dims);
      const course = Number(p.course);
      const student = Number(p.student);
      const lesson = Number(p.lesson);
      expect(course).toBeGreaterThan((tenant - 1) * dims.coursesPerTenant);
      expect(course).toBeLessThanOrEqual(tenant * dims.coursesPerTenant);
      expect(student).toBeGreaterThan((tenant - 1) * dims.studentsPerTenant);
      expect(student).toBeLessThanOrEqual(tenant * dims.studentsPerTenant);
      expect(lesson).toBe(
        (course - 1) * dims.lessonsPerCourse + Number(p.position),
      );
    }
    expect([...seen].sort()).toEqual([1, 2, 3]);
    expect(tenantFor(99, "single", 100)).toBe(1);
  });
});
