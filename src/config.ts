import { EXPLICIT_BACKENDS } from "../shared/topology.ts";
import { DEFAULT_POOL, type PoolConfig } from "../shared/policy.ts";
import type { TrafficOptions } from "./workloads/distribution.ts";
// CLI + env configuration. Env vars take precedence over flags so CI and
// local runs share one code path; empty env values fall through.
export const BACKENDS = [
  ...EXPLICIT_BACKENDS,
  "d1",
  "d1-raw",
  "d1-eeur",
  "turso",
  "turso-raw",
  "turso-reused",
  "tursodb",
  "tursodb-reused",
  "tursodb-concurrent",
  "d1-rr",
  "d1-eeur-rr",
  "d1-drizzle",
  "d1-eeur-drizzle",
  "turso-drizzle",
] as const;

export const TESTS = [
  "cold-start",
  "first-request-after-idle",
  "tenant-churn",
  "exam-burst",
  "order-event",
  "lms-read-heavy",
  "lms-balanced",
  "lms-write-heavy",
  "point-read",
  "scan",
  "scan-100",
  "insert",
  "update",
  "student-dashboard",
  "course-page",
  "lesson-page",
  "quiz-page",
  "submit-quiz-answer",
  "update-progress",
  "enrollment",
  "independent-writes",
  "hot-row-write",
  "mixed",
] as const;

export const MODES = ["edge", "direct"] as const;
export const LOAD_MODELS = [
  "constant",
  "closed-loop",
  "arrival-rate",
  "burst",
] as const;
export const TENANT_MODES = ["single", "distributed"] as const;
export const WORKLOADS = ["kv", "lms"] as const;

export type Backend = (typeof BACKENDS)[number];
export type Test = (typeof TESTS)[number];
export type Mode = (typeof MODES)[number];
export type LoadModel = (typeof LOAD_MODELS)[number];
export type TenantMode = (typeof TENANT_MODES)[number];
export type Workload = (typeof WORKLOADS)[number];

export interface BenchArgs {
  workerUrl: string;
  tursoUrl: string;
  tursoToken: string;
  tursodbUrl: string;
  tursodbToken: string;
  adminToken: string;
  backends: Backend[];
  tests: Test[];
  workload: Workload;
  modes: Mode[];
  iterations: number;
  duration: number; // seconds, 0 = iteration-based
  concurrencies: number[];
  warmup: number;
  seedRows: number;
  scanLimits: number[];
  loadModel: LoadModel;
  timeoutMs: number;
  runs: number;
  writeRetries: number;
  coldSamples: number;
  coldIdleMs: number;
  coldWarmRequests: number;
  tenantMode: TenantMode;
  tenantCounts: number[];
  tenants: number;
  studentsPerTenant: number;
  coursesPerTenant: number;
  lessonsPerCourse: number;
  out: string;
  suites: string[];
  targetRps: number[];
  maxInFlight: number;
  maxStartLagMs: number;
  traffic: TrafficOptions;
  pool: PoolConfig;
  runnerLabel: string;
  runnerRegion: string;
  idleIntervals: number[];
  idleVariants: string[];
  examStudents: number[];
  profileMix: Record<string, number> | null;
  validationTimeoutMs: number;
}

export function getRaw(k: string, d: string): string {
  const env = process.env[k.toUpperCase().replace(/-/g, "_")];
  if (env !== undefined && env !== "") return env;
  const hit = process.argv.find((a) => a.startsWith(`--${k}=`));
  return hit ? hit.slice(k.length + 3) : d;
}

function oneOf<T extends string>(
  k: string,
  d: string,
  allowed: readonly T[],
): T[] {
  const vals = getRaw(k, d)
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s !== "");
  if (vals.length === 0)
    throw new Error(
      `--${k} must be a non-empty list of: ${allowed.join(", ")}`,
    );
  for (const v of vals) {
    if (!(allowed as readonly string[]).includes(v)) {
      throw new Error(
        `--${k}: unknown value ${JSON.stringify(v)} (allowed: ${allowed.join(", ")})`,
      );
    }
  }
  return vals as T[];
}

function oneValue<T extends string>(
  k: string,
  d: string,
  allowed: readonly T[],
): T {
  const raw = getRaw(k, d).trim();
  if (!(allowed as readonly string[]).includes(raw)) {
    throw new Error(
      `--${k} must be one of: ${allowed.join(", ")} (got ${JSON.stringify(raw)})`,
    );
  }
  return raw as T;
}

function int(k: string, d: string, min: number, max?: number): number {
  const raw = getRaw(k, d);
  const n = Number(raw);
  const range = max === undefined ? `>= ${min}` : `${min}..${max}`;
  if (!Number.isInteger(n) || n < min || (max !== undefined && n > max)) {
    throw new Error(
      `--${k} must be an integer ${range} (got ${JSON.stringify(raw)})`,
    );
  }
  return n;
}

function intList(k: string, d: string, min: number): number[] {
  const items = getRaw(k, d).split(",");
  const out: number[] = [];
  for (const item of items) {
    const raw = item.trim();
    const n = Number(raw);
    if (raw === "" || !Number.isInteger(n) || n < min) {
      throw new Error(
        `--${k} must be a comma list of integers >= ${min} (got ${JSON.stringify(item)})`,
      );
    }
    out.push(n);
  }
  return out;
}

export function parseArgs(
  preset: { manhali?: boolean; full?: boolean } = {},
): BenchArgs {
  const timeoutMs = int("timeout-ms", "5000", 100, 60000);
  return {
    workerUrl: getRaw("worker-url", "http://127.0.0.1:8787").replace(/\/$/, ""),
    tursoUrl: getRaw("turso-url", ""),
    tursoToken: getRaw("turso-token", ""),
    tursodbUrl: getRaw("tursodb-url", ""),
    tursodbToken: getRaw("tursodb-token", ""),
    adminToken: getRaw("admin-token", ""),
    backends: oneOf(
      "backends",
      preset.manhali
        ? "d1-per-tenant,turso-per-tenant,tursodb-per-tenant"
        : "d1,d1-eeur,turso,d1-rr,d1-eeur-rr",
      BACKENDS,
    ),
    tests: oneOf("tests", "point-read,scan,insert", TESTS),
    workload: oneValue("workload", "lms", WORKLOADS),
    modes: oneOf("modes", "edge", MODES),
    iterations: int("iterations", preset.manhali ? "1000000" : "2000", 1),
    duration: int(
      "duration",
      preset.manhali ? (preset.full ? "30" : "8") : "0",
      0,
      3600,
    ),
    concurrencies: intList("concurrency", "1,10,50", 1),
    warmup: int("warmup", preset.manhali ? "5" : "100", 0),
    seedRows: int("seed-rows", "10000", 1),
    scanLimits: intList("scan-limits", "100", 1),
    loadModel: oneValue("load-model", "constant", LOAD_MODELS),
    timeoutMs,
    runs: int("runs", preset.full ? "10" : "5", 1, 25),
    writeRetries: int("write-retries", "3", 0, 20),
    coldSamples: int("cold-samples", preset.manhali ? "1" : "5", 1, 1000),
    coldIdleMs: int("cold-idle-ms", "30000", 0, 3600000),
    coldWarmRequests: int("cold-warm-requests", "5", 1, 100),
    tenantMode: oneValue("tenant-mode", "single", TENANT_MODES),
    tenantCounts: intList("tenant-count", "1", 1),
    tenants: int(
      "tenants",
      preset.manhali ? (preset.full ? "500" : "100") : "10",
      1,
      100000,
    ),
    studentsPerTenant: int(
      "students-per-tenant",
      preset.manhali ? "500" : "200",
      1,
      100000,
    ),
    coursesPerTenant: int("courses-per-tenant", "5", 1, 10000),
    lessonsPerCourse: int("lessons-per-course", "10", 1, 10000),
    out: getRaw(
      "out",
      `results-manhali-${new Date().toISOString().replace(/[:.]/g, "-")}-${crypto.randomUUID().slice(0, 8)}.json`,
    ),
    suites: oneOf("suites", "latency", [
      "latency",
      "saturation",
      "reliability",
      "idle-start",
      "tenant-churn",
      "writes",
      "read-heavy",
      "mixed-lms",
    ]),
    targetRps: intList("target-rps", "10,25,50,100,200", 1),
    maxInFlight: int("max-in-flight", "1000", 1, 100000),
    maxStartLagMs: int("max-start-lag-ms", "1000", 0),
    traffic: {
      distribution: oneValue("tenant-distribution", "uniform", [
        "uniform",
        "zipf",
        "hotset",
      ]),
      hotTenantPercent: int("hot-tenant-percent", "10", 1, 99),
      hotTrafficPercent: int("hot-traffic-percent", "80", 1, 99),
      zipfExponent: Number(getRaw("zipf-exponent", "1.1")),
      seed: int("seed", "42", 0),
    },
    pool: {
      ...DEFAULT_POOL,
      operationTimeoutMs: timeoutMs,
      queryTimeoutMs: timeoutMs,
      checkoutTimeoutMs: timeoutMs,
      sizePerTenant: int("turso-pool-size-per-tenant", "8", 1),
      maxActivePools: int("turso-max-active-pools", "100", 1),
      idleTtlMs: int("turso-pool-idle-ttl-ms", "60000", 1),
      maxTotalConnections: int("turso-max-total-connections", "128", 1),
    },
    runnerLabel: getRaw("runner-label", "unspecified"),
    runnerRegion: getRaw("runner-region", "unknown"),
    idleIntervals: getRaw("idle-intervals-ms", "")
      ? intList("idle-intervals-ms", "30000", 0)
      : [int("cold-idle-ms", "30000", 0, 3600000)],
    idleVariants: oneOf("idle-variants", "idle-same-row", [
      "idle-same-row",
      "idle-different-row",
      "idle-same-tenant",
      "idle-different-tenant",
    ]),
    examStudents: intList("exam-students", "50,100,250", 1),
    profileMix: getRaw("lms-mix", "")
      ? (JSON.parse(getRaw("lms-mix", "")) as Record<string, number>)
      : null,
    validationTimeoutMs: int("validation-timeout-ms", "60000", 100),
  };
}
