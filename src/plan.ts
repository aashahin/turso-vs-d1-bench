import type { BenchArgs } from "./config.ts";
import { backendSpec } from "../shared/topology.ts";
import { WRITE_TESTS } from "../shared/backend-tests.ts";
import type { Distribution } from "./workloads/distribution.ts";
export interface ScenarioPlan {
  mode: string;
  backend: string;
  suite: string;
  test: string;
  scanLimit: number | null;
  tenantCount: number;
  tenantMode: string;
  concurrency: number;
  loadModel: BenchArgs["loadModel"];
  offeredRps: number | null;
  distribution: Distribution;
  idleMs?: number;
  variant?: string;
  examStudents?: number;
}
export function plans(args: BenchArgs): ScenarioPlan[] {
  const result: ScenarioPlan[] = [];
  for (const mode of args.modes)
    for (let backend of args.backends) {
      // Legacy distributed D1 intent becomes strict per-tenant; legacy Turso is explicitly shared.
      if (
        args.tenantMode === "distributed" &&
        backend.startsWith("d1") &&
        !backend.includes("shared") &&
        !backend.includes("per-tenant")
      )
        backend =
          `${backend.includes("eeur") ? "d1-eeur" : "d1"}-per-tenant${backend.endsWith("drizzle") ? "-drizzle" : backend.endsWith("rr") ? "-rr" : ""}` as typeof backend;
      if (
        mode === "direct" &&
        (backendSpec(backend).engine !== "libsql" ||
          backendSpec(backend).tenantIsolation === "database-per-tenant" ||
          backend.endsWith("drizzle"))
      )
        continue;
      for (const suite of args.suites) {
        const tests =
          suite === "idle-start"
            ? ["first-request-after-idle"]
            : suite === "tenant-churn"
              ? ["tenant-churn"]
              : suite === "mixed-lms"
                ? ["lms-balanced"]
                : suite === "read-heavy"
                  ? ["lms-read-heavy"]
                  : suite === "writes"
                    ? ["update-progress", "submit-quiz-answer", "order-event"]
                    : args.tests;
        for (const raw of tests) {
          const test = raw === "scan-100" ? "scan" : raw;
          const idle =
            test === "cold-start" || test === "first-request-after-idle";
          if (idle && mode !== "edge")
            throw new Error(
              "cold-start requires --modes=edge (idle observations require Worker evidence)",
            );
          if (
            mode === "edge" &&
            (WRITE_TESTS[test] ||
              test === "mixed" ||
              test.startsWith("lms-") ||
              test === "exam-burst") &&
            !args.adminToken
          )
            throw new Error("edge writes need --admin-token");
          for (const count of args.tenantMode === "distributed"
            ? args.tenantCounts
            : [1]) {
            const base = {
              mode,
              backend,
              suite,
              test,
              scanLimit: null,
              tenantCount: count,
              tenantMode: args.tenantMode,
              concurrency: 1,
              loadModel: args.loadModel,
              offeredRps: null,
              distribution: args.traffic.distribution,
            } satisfies ScenarioPlan;
            if (idle) {
              for (const idleMs of args.idleIntervals)
                for (const variant of args.idleVariants) {
                  if (variant === "idle-different-tenant" && count < 2)
                    throw new Error(
                      "idle-different-tenant requires --tenant-mode=distributed --tenant-count>=2",
                    );
                  result.push({ ...base, idleMs, variant });
                }
              continue;
            }
            if (test === "exam-burst") {
              for (const students of args.examStudents) {
                if (students > args.studentsPerTenant)
                  throw new Error(
                    "exam students exceed --students-per-tenant: reseed a larger dataset",
                  );
                result.push({
                  ...base,
                  tenantCount: 1,
                  tenantMode: "single",
                  suite: "writes",
                  concurrency: students,
                  examStudents: students,
                  loadModel: "burst",
                });
              }
              continue;
            }
            const arrival =
              args.loadModel === "arrival-rate" || suite === "saturation";
            for (const c of arrival ? [args.maxInFlight] : args.concurrencies)
              for (const rps of arrival ? args.targetRps : [null])
                for (const limit of test === "scan" ? args.scanLimits : [null])
                  result.push({
                    ...base,
                    test: limit ? `scan-${limit}` : test,
                    scanLimit: limit,
                    concurrency: c,
                    loadModel: arrival
                      ? "arrival-rate"
                      : args.loadModel === "constant"
                        ? "closed-loop"
                        : args.loadModel,
                    offeredRps: rps,
                  });
          }
        }
      }
    }
  if (!result.length) throw new Error("empty benchmark matrix");
  return result;
}
export function canonicalPlans(args: BenchArgs, full = false): ScenarioPlan[] {
  const result: ScenarioPlan[] = [];
  const tenantCounts = (full ? [10, 50, 100, 500] : [10, 50, 100]).filter(
    (n) => n <= args.tenants,
  );
  if (!tenantCounts.includes(args.tenants)) tenantCounts.push(args.tenants);
  const make = (override: Partial<BenchArgs>) =>
    result.push(...plans({ ...args, ...override }));
  make({
    suites: ["latency"],
    tests: ["point-read"],
    tenantMode: "single",
    concurrencies: [1],
    loadModel: "closed-loop",
  });
  make({
    suites: ["mixed-lms"],
    tenantMode: "single",
    concurrencies: full ? [1, 10, 25, 50, 100, 200] : [1, 10, 50, 100],
    loadModel: "closed-loop",
  });
  make({
    suites: ["writes"],
    tenantMode: "single",
    concurrencies: full ? [1, 10, 50, 100] : [1, 10, 50],
    loadModel: "closed-loop",
  });
  make({
    suites: ["latency"],
    tests: ["exam-burst"],
    tenantMode: "single",
    examStudents: full ? [50, 100, 250, 500] : [50, 100, 250],
  });
  for (const distribution of full
    ? (["uniform", "hotset", "zipf"] as const)
    : (["uniform", "hotset"] as const))
    make({
      suites: ["tenant-churn"],
      tenantMode: "distributed",
      tenantCounts,
      concurrencies: [50],
      loadModel: "closed-loop",
      traffic: { ...args.traffic, distribution },
    });
  make({
    suites: ["saturation"],
    tests: ["lms-balanced"],
    tenantMode: "single",
    targetRps: full ? [10, 25, 50, 100, 200, 500] : [10, 25, 50, 100, 200],
    loadModel: "arrival-rate",
  });
  make({
    suites: ["idle-start"],
    tenantMode: full ? "distributed" : "single",
    tenantCounts: full ? [Math.min(10, args.tenants)] : [1],
    idleIntervals: full ? [30000, 120000, 300000] : [30000],
    idleVariants: full
      ? [
          "idle-same-row",
          "idle-different-row",
          "idle-same-tenant",
          "idle-different-tenant",
        ]
      : ["idle-same-row"],
  });
  return result;
}

/** Interleave comparable scenarios and rotate engine order for each repetition. */
export function roundPlans(
  selection: ScenarioPlan[],
  run: number,
): ScenarioPlan[] {
  const groups = new Map<string, ScenarioPlan[]>();
  for (const scenario of selection) {
    const key = JSON.stringify([
      scenario.mode,
      scenario.suite,
      scenario.test,
      scenario.tenantMode,
      scenario.tenantCount,
      scenario.concurrency,
      scenario.loadModel,
      scenario.offeredRps,
      scenario.scanLimit,
      scenario.distribution,
      scenario.idleMs,
      scenario.variant,
      scenario.examStudents,
    ]);
    (groups.get(key) ?? (groups.set(key, []), groups.get(key)!)).push(scenario);
  }
  return [...groups.values()].flatMap((rows) => {
    const shift = (run - 1) % rows.length;
    return [...rows.slice(shift), ...rows.slice(0, shift)];
  });
}
