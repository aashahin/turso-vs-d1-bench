// Orchestrator: builds the benchmark matrix, runs every scenario `runs` times,
// captures environment metadata, and writes results.json + results.csv plus
// console saturation tables. Tokens are never written to results.
import { makeDirectExec, makeEdgeExec } from "./exec.ts";
import { parseArgs, type Backend, type Test } from "./config.ts";
import { printBests, printDistributed, printRunTable, printSaturation, printWriteComparison } from "./reporters/console.ts";
import { toCsv } from "./reporters/csv.ts";
import { runScenario } from "./runner.ts";
import { median, mergeTx } from "./stats.ts";
import type { EnvInfo, ResultDoc, RunRecord, ScenarioResult } from "./results.ts";
import { tenantFor, type Dims, type OpQuery } from "./workloads/common.ts";
import { kvMixedTest, kvOp, lmsMixedTest, lmsOp, writeOp } from "./workloads/index.ts";
import { CONCURRENCY_TESTS, KV_TESTS, WRITE_TESTS } from "../shared/backend-tests.ts";

const args = parseArgs();

const dims: Dims = {
  seedRows: args.seedRows,
  scanLimits: args.scanLimits,
  tenants: args.tenants,
  studentsPerTenant: args.studentsPerTenant,
  coursesPerTenant: args.coursesPerTenant,
  lessonsPerCourse: args.lessonsPerCourse,
};

function normalizeTest(t: Test): string {
  // Legacy alias from the first benchmark iteration.
  if ((t as string) === "scan-100") return "scan";
  return t;
}

function workloadOf(test: string): string {
  if (test === "mixed") return args.workload;
  if (CONCURRENCY_TESTS[test] === true) return "concurrency";
  return KV_TESTS[test] === true ? "kv" : "lms";
}

function buildOp(test: string, tenantMode: string, tenantCount: number): (index: number) => OpQuery {
  return (index: number) => {
    const tenant = tenantFor(index, tenantMode, tenantCount);
    if (test === "mixed") {
      const atomic = args.workload === "kv" ? kvMixedTest(index) : lmsMixedTest(index);
      return args.workload === "kv" ? kvOp(atomic, index, tenant, dims) : lmsOp(atomic, index, tenant, dims);
    }
    if (CONCURRENCY_TESTS[test] === true) return writeOp(test, index, tenant, dims);
    if (KV_TESTS[test] === true) return kvOp(test, index, tenant, dims);
    return lmsOp(test, index, tenant, dims);
  };
}

async function coloOf(workerUrl: string): Promise<string | null> {
  try {
    const res = await fetch(`${workerUrl}/cdn-cgi/trace`);
    if (!res.ok) return null;
    const text = await res.text();
    return text.match(/^colo=(.*)$/m)?.[1]?.trim() ?? null;
  } catch {
    return null;
  }
}

async function metaOf(workerUrl: string): Promise<{ kv: Record<string, number | null>; lms: Record<string, number | null>; concurrency: Record<string, number | null> }> {
  try {
    const res = await fetch(`${workerUrl}/bench/meta`);
    if (!res.ok) return { kv: {}, lms: {}, concurrency: {} };
    const m = (await res.json()) as Record<string, unknown>;
    const num = (k: string): number | null => (typeof m[k] === "number" ? (m[k] as number) : null);
    const d1Conc = (m.d1_concurrency_rows as Record<string, number | null>) ?? {};
    return {
      kv: { d1_weur: num("d1_weur_kv_rows"), d1_eeur: num("d1_eeur_kv_rows"), turso: num("turso_kv_rows"), tursodb: num("tursodb_kv_rows") },
      lms: ((m.d1_lms as Record<string, number | null>) ?? {}) as Record<string, number | null>,
      concurrency: {
        d1_weur_progress: d1Conc.concurrent_progress ?? null,
        d1_weur_hot: d1Conc.hot_counter ?? null,
        turso_progress: num("turso_progress_rows"),
        tursodb_progress: num("tursodb_progress_rows"),
        tursodb_hot: num("tursodb_hot_rows"),
      },
    };
  } catch {
    return { kv: {}, lms: {}, concurrency: {} };
  }
}

/** Worker-side view of the configured Turso databases (hosts only, never tokens). */
async function identityOf(workerUrl: string): Promise<{ tursoHost: string | null; tursodbHost: string | null; tursodbConfigured: boolean }> {
  try {
    const res = await fetch(`${workerUrl}/bench/identity`);
    if (!res.ok) return { tursoHost: null, tursodbHost: null, tursodbConfigured: false };
    const identity = (await res.json()) as Record<string, unknown>;
    return {
      tursoHost: typeof identity.tursoHost === "string" ? identity.tursoHost : null,
      tursodbHost: typeof identity.tursodbHost === "string" ? identity.tursodbHost : null,
      tursodbConfigured: identity.tursodbConfigured === true,
    };
  } catch {
    return { tursoHost: null, tursodbHost: null, tursodbConfigured: false };
  }
}

async function pkgVersion(name: string): Promise<string | null> {
  try {
    const pkg = (await Bun.file(`node_modules/${name}/package.json`).json()) as { version?: string };
    return pkg.version ?? null;
  } catch {
    return null;
  }
}

// ---- Matrix ----
interface Scenario {
  mode: string;
  backend: Backend;
  test: string;
  scanLimit: number | null;
  tenantMode: string;
  tenantCount: number;
  concurrency: number;
}

const DIRECT_BACKENDS: Readonly<Record<string, true>> = { turso: true, "turso-raw": true, "turso-reused": true, "turso-drizzle": true };

const matrix: Scenario[] = [];
for (const mode of args.modes) {
  for (const backend of args.backends) {
    if (mode === "direct" && DIRECT_BACKENDS[backend] !== true) {
      console.log(`skip direct/${backend}: direct mode only supports the libSQL backends reached over HTTPS from this machine (D1 and Turso Database go through the Worker in edge mode).`);
      continue;
    }
    for (const rawTest of args.tests) {
      const test = normalizeTest(rawTest);
      const needsAdmin = test === "mixed" || WRITE_TESTS[test] === true;
      if (mode === "edge" && needsAdmin && !args.adminToken) {
        throw new Error("edge writes need --admin-token (or ADMIN_TOKEN env): Worker write endpoints require Bearer auth");
      }
      const tenantCounts = args.tenantMode === "distributed" ? args.tenantCounts : [1];
      for (const tenantCount of tenantCounts) {
        for (const c of args.concurrencies) {
          if (test === "scan") {
            for (const limit of args.scanLimits) {
              matrix.push({ mode, backend, test: `scan-${limit}`, scanLimit: limit, tenantMode: args.tenantMode, tenantCount, concurrency: c });
            }
          } else {
            matrix.push({ mode, backend, test, scanLimit: null, tenantMode: args.tenantMode, tenantCount, concurrency: c });
          }
        }
      }
    }
  }
}
if (matrix.length === 0) throw new Error("empty benchmark matrix: check --modes/--backends/--tests");

console.log(
  `modes=${args.modes.join("+")} backends=${args.backends.join(",")} tests=${args.tests.join(",")} workload=${args.workload} ` +
    `iterations=${args.iterations} duration=${args.duration}s concurrency=${args.concurrencies.join(",")} warmup=${args.warmup} ` +
    `load=${args.loadModel} runs=${args.runs} tenant=${args.tenantMode} tenants=${args.tenantCounts.join(",")} timeout=${args.timeoutMs}ms`,
);

// ---- Runs ----
const scenarios: ScenarioResult[] = [];
for (const s of matrix) {
  const atomicForOps = s.scanLimit !== null ? "scan" : s.test;
  // Scan scenarios pin one limit per scenario; other ops builders sample from dims.
  const pinned = s.scanLimit !== null ? { ...dims, scanLimits: [s.scanLimit as number] } : dims;
  const ops = (index: number): OpQuery => {
    const base = s.scanLimit !== null ? kvOp("scan", index, tenantFor(index, s.tenantMode, s.tenantCount), pinned) : buildOp(atomicForOps, s.tenantMode, s.tenantCount)(index);
    // --write-retries is a Worker request param; reads ignore it.
    return { test: base.test, method: base.method, params: { ...base.params, retries: args.writeRetries } };
  };
  const label = `${s.mode}/${s.backend}/${s.test} ${s.tenantMode}/t${s.tenantCount} c=${s.concurrency}`;
  const exec = s.mode === "edge" ? makeEdgeExec(args.workerUrl, args.adminToken, s.backend) : makeDirectExec(args.tursoUrl, args.tursoToken);
  const runs: RunRecord[] = [];
  for (let run = 1; run <= args.runs; run++) {
    const tag = args.runs > 1 ? `${label} run=${run}/${args.runs}` : label;
    const out = await runScenario({
      label: tag,
      ops,
      totalOps: args.iterations,
      durationSec: args.duration,
      concurrency: s.concurrency,
      warmup: args.warmup,
      loadModel: args.loadModel,
      timeoutMs: args.timeoutMs,
      exec,
    });
    runs.push({
      run,
      e2e: out.e2e,
      db: out.db,
      query: out.query,
      success: out.success,
      failed: out.failed,
      errorRate: out.errorRate,
      errorsByClass: out.errorsByClass,
      errorsByStatus: out.errorsByStatus,
      sampleErrors: out.sampleErrors,
      replicaRate: out.replicaRate,
      replicaReads: out.replicaReads,
      primaryReads: out.primaryReads,
      totalReads: out.totalReads,
      regions: out.regions,
      engine: out.engine,
      transactionMode: out.transactionMode,
      tx: out.tx,
      rps: out.rps,
      wallMs: out.wallMs,
    });
  }
  scenarios.push({
    mode: s.mode,
    backend: s.backend,
    engine: runs.find((r) => r.engine !== null)?.engine ?? null,
    transactionMode: runs.find((r) => r.transactionMode !== null)?.transactionMode ?? null,
    test: s.test,
    workload: workloadOf(atomicForOps),
    scanLimit: s.scanLimit,
    tenantMode: s.tenantMode,
    tenantCount: s.tenantCount,
    concurrency: s.concurrency,
    loadModel: args.loadModel,
    durationSec: args.duration,
    iterations: args.iterations,
    timeoutMs: args.timeoutMs,
    runs,
    summary: {
      medianP50: median(runs.map((r) => r.e2e.p50)),
      medianP95: median(runs.map((r) => r.e2e.p95)),
      medianP99: median(runs.map((r) => r.e2e.p99)),
      medianRps: median(runs.map((r) => r.rps)),
      medianDbP50: runs.some((r) => r.db !== null) ? median(runs.map((r) => r.db?.p50 ?? 0)) : null,
      medianErrorRate: median(runs.map((r) => r.errorRate)),
      medianReplicaRate: runs.some((r) => r.replicaRate !== null) ? median(runs.map((r) => r.replicaRate ?? 0)) : null,
      // Counters are summed across runs (rates must stay exact, not medians of rates).
      tx: mergeTx(runs.map((r) => r.tx).filter((tx): tx is NonNullable<typeof tx> => tx !== null)),
    },
  });
}

// ---- Environment ----
const hostOfUrl = (u: string): string | null => {
  if (u === "") return null;
  try {
    return new URL(u.replace(/^(libsql|turso):\/\//, "https://")).host;
  } catch {
    return null;
  }
};

const notes: string[] = [];
if (args.modes.includes("direct")) notes.push("direct mode hits Turso Cloud from this machine; it is a client-to-cloud reference, not comparable with edge-terminated D1 traffic.");
if (args.tenantMode === "distributed") notes.push("distributed requests spread uniformly over tenantCount databases; per-tenant RPS = total RPS / tenantCount. Tenant DBs backed by explicit DB_TENANT_* bindings report tenantDb isolated, otherwise tenant_id scoping in the shared primary (see tenantBinding per response).");
if (args.backends.some((b) => b.startsWith("tursodb"))) notes.push("tursodb backends target the new Turso Database engine (turso db create --tursodb). tursodb-concurrent uses BEGIN CONCURRENT (early-preview MVCC): conflicts are detected at commit, retried up to --write-retries, and every retry is inside the measured latency.");
const environment: EnvInfo = {
  timestamp: new Date().toISOString(),
  workerUrl: args.workerUrl,
  colo: await coloOf(args.workerUrl),
  tursoHost: hostOfUrl(args.tursoUrl),
  tursodbHost: hostOfUrl(args.tursodbUrl),
  tursodbConfigured: args.tursodbUrl !== "" && args.tursodbToken !== "",
  kvRows: {},
  lmsRows: {},
  concurrencyRows: {},
  kvPayloadBytes: 200,
  seedDims: { seedRows: args.seedRows, tenants: args.tenants, studentsPerTenant: args.studentsPerTenant, coursesPerTenant: args.coursesPerTenant, lessonsPerCourse: args.lessonsPerCourse },
  sdk: { tursoServerless: await pkgVersion("@tursodatabase/serverless"), drizzleOrm: await pkgVersion("drizzle-orm") },
  wrangler: await pkgVersion("wrangler"),
  bun: typeof Bun !== "undefined" ? Bun.version : null,
  notes,
};
{
  const meta = await metaOf(args.workerUrl);
  environment.kvRows = meta.kv;
  environment.lmsRows = meta.lms;
  environment.concurrencyRows = meta.concurrency;
  const identity = await identityOf(args.workerUrl);
  // The Worker's view wins: it is the process that actually talks to the DB.
  if (identity.tursoHost !== null) environment.tursoHost = identity.tursoHost;
  if (identity.tursodbHost !== null) environment.tursodbHost = identity.tursodbHost;
  environment.tursodbConfigured = identity.tursodbConfigured;
  if (args.backends.some((b) => b.startsWith("tursodb")) && !identity.tursodbConfigured) {
    throw new Error(
      "tursodb backends requested but the Worker has no TURSODB_URL/TURSODB_TOKEN: run `turso db create --tursodb bench-tursodb`, set the TURSODB_URL var + TURSODB_TOKEN secret (wrangler.jsonc vars / `bun run worker:secret:tursodb`), then reseed.",
    );
  }
  const progress = environment.concurrencyRows.tursodb_progress ?? environment.concurrencyRows.d1_weur_progress;
  if (args.tests.some((t) => t === "independent-writes") && progress !== null && progress !== args.seedRows) {
    notes.push(`concurrency tables hold ${progress} rows but --seed-rows=${args.seedRows}: reseed before comparing independent-writes across backends.`);
  }
}

const doc: ResultDoc = { timestamp: environment.timestamp, environment, scenarios };

// ---- Output ----
printRunTable(scenarios);
printSaturation(scenarios);
printWriteComparison(scenarios);
printDistributed(scenarios);
printBests(scenarios);
await Bun.write(args.out, JSON.stringify(doc, null, 2));
const csvOut = args.out.endsWith(".json") ? `${args.out.slice(0, -".json".length)}.csv` : `${args.out}.csv`;
await Bun.write(csvOut, toCsv(scenarios));
console.log(`wrote ${args.out} + ${csvOut}`);
