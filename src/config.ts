// CLI + env configuration. Env vars take precedence over flags so CI and
// local runs share one code path; empty env values fall through.
export const BACKENDS = [
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
export const LOAD_MODELS = ["constant", "burst"] as const;
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
  tenantMode: TenantMode;
  tenantCounts: number[];
  tenants: number;
  studentsPerTenant: number;
  coursesPerTenant: number;
  lessonsPerCourse: number;
  out: string;
}

export function getRaw(k: string, d: string): string {
  const env = process.env[k.toUpperCase().replace(/-/g, "_")];
  if (env !== undefined && env !== "") return env;
  const hit = process.argv.find((a) => a.startsWith(`--${k}=`));
  return hit ? hit.slice(k.length + 3) : d;
}

function oneOf<T extends string>(k: string, d: string, allowed: readonly T[]): T[] {
  const vals = getRaw(k, d)
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s !== "");
  if (vals.length === 0) throw new Error(`--${k} must be a non-empty list of: ${allowed.join(", ")}`);
  for (const v of vals) {
    if (!(allowed as readonly string[]).includes(v)) {
      throw new Error(`--${k}: unknown value ${JSON.stringify(v)} (allowed: ${allowed.join(", ")})`);
    }
  }
  return vals as T[];
}

function oneValue<T extends string>(k: string, d: string, allowed: readonly T[]): T {
  const raw = getRaw(k, d).trim();
  if (!(allowed as readonly string[]).includes(raw)) {
    throw new Error(`--${k} must be one of: ${allowed.join(", ")} (got ${JSON.stringify(raw)})`);
  }
  return raw as T;
}

function int(k: string, d: string, min: number, max?: number): number {
  const raw = getRaw(k, d);
  const n = Number(raw);
  const range = max === undefined ? `>= ${min}` : `${min}..${max}`;
  if (!Number.isInteger(n) || n < min || (max !== undefined && n > max)) {
    throw new Error(`--${k} must be an integer ${range} (got ${JSON.stringify(raw)})`);
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
      throw new Error(`--${k} must be a comma list of integers >= ${min} (got ${JSON.stringify(item)})`);
    }
    out.push(n);
  }
  return out;
}

export function parseArgs(): BenchArgs {
  return {
    workerUrl: getRaw("worker-url", "http://127.0.0.1:8787").replace(/\/$/, ""),
    tursoUrl: getRaw("turso-url", ""),
    tursoToken: getRaw("turso-token", ""),
    tursodbUrl: getRaw("tursodb-url", ""),
    tursodbToken: getRaw("tursodb-token", ""),
    adminToken: getRaw("admin-token", ""),
    backends: oneOf("backends", "d1,d1-eeur,turso,d1-rr,d1-eeur-rr", BACKENDS),
    tests: oneOf("tests", "point-read,scan,insert", TESTS),
    workload: oneValue("workload", "lms", WORKLOADS),
    modes: oneOf("modes", "edge", MODES),
    iterations: int("iterations", "2000", 1),
    duration: int("duration", "0", 0, 3600),
    concurrencies: intList("concurrency", "1,10,50", 1),
    warmup: int("warmup", "100", 0),
    seedRows: int("seed-rows", "10000", 1),
    scanLimits: intList("scan-limits", "100", 1),
    loadModel: oneValue("load-model", "constant", LOAD_MODELS),
    timeoutMs: int("timeout-ms", "5000", 100),
    runs: int("runs", "1", 1, 25),
    writeRetries: int("write-retries", "3", 0, 20),
    tenantMode: oneValue("tenant-mode", "single", TENANT_MODES),
    tenantCounts: intList("tenant-count", "1", 1),
    tenants: int("tenants", "10", 1, 100000),
    studentsPerTenant: int("students-per-tenant", "200", 1, 100000),
    coursesPerTenant: int("courses-per-tenant", "5", 1, 10000),
    lessonsPerCourse: int("lessons-per-course", "10", 1, 10000),
    out: getRaw("out", "results.json"),
  };
}
