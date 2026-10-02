import { createCheckpoint } from "./checkpoint.ts";
// Reproducible orchestrator. Preflight/provisioning are outside every measured window.
import { hostname } from "node:os";
import { parseArgs, type BenchArgs } from "./config.ts";
import { plans, roundPlans, type ScenarioPlan } from "./plan.ts";
import { backendSpec } from "../shared/topology.ts";
import {
  preflight,
  expectedFingerprint,
  type TargetEvidence,
} from "./topology.ts";
import { makeDirectExec, makeEdgeExec } from "./exec.ts";
import { runScenario } from "./runner.ts";
import { runColdStart, coldStartCsv, type ColdStartRun } from "./cold-start.ts";
import {
  summarizeRuns,
  type EnvInfo,
  type ResultDoc,
  type ScenarioResult,
} from "./results.ts";
import { kvOp, lmsOp, writeOp, kvMixedTest } from "./workloads/index.ts";
import type { Dims, OpQuery } from "./workloads/common.ts";
import { tenantSelector } from "./workloads/distribution.ts";
import { examOp, profileMix, operationSelector } from "./workloads/profiles.ts";
import { CONCURRENCY_TESTS, KV_TESTS } from "../shared/backend-tests.ts";
import { manhaliSummary, printManhali } from "./reporters/manhali.ts";
import { toCsv } from "./reporters/csv.ts";
import { serializeResult, writeArtifacts } from "./artifacts.ts";
import { access } from "node:fs/promises";
async function version(name: string): Promise<string | null> {
  try {
    return (
      (await Bun.file(`node_modules/${name}/package.json`).json()) as {
        version: string;
      }
    ).version;
  } catch {
    return null;
  }
}
async function git(...args: string[]): Promise<string> {
  const p = Bun.spawn(["git", ...args], { stdout: "pipe", stderr: "ignore" });
  const text = await new Response(p.stdout).text();
  if ((await p.exited) !== 0) throw new Error("git provenance unavailable");
  return text.trim();
}
export function workload(
  args: BenchArgs,
  s: ScenarioPlan,
  dims: Dims,
): (index: number) => OpQuery {
  const tenant = tenantSelector(s.tenantCount, {
    ...args.traffic,
    distribution: s.distribution,
  });
  const mix = profileMix(s.test, args.workload, args.profileMix);
  const choose = mix ? operationSelector(mix, args.traffic.seed) : null;
  return (index) => {
    const t = s.tenantMode === "single" ? 1 : tenant(index);
    let op: OpQuery;
    if (s.test === "exam-burst") op = examOp(index, t, dims);
    else {
      const test =
        s.test === "tenant-churn"
          ? "point-read"
          : s.scanLimit
            ? "scan"
            : choose
              ? choose(index - args.warmup)
              : s.test === "mixed"
                ? kvMixedTest(index)
                : s.test;
      op = CONCURRENCY_TESTS[test]
        ? writeOp(test, index, t, dims)
        : KV_TESTS[test]
          ? kvOp(test, index, t, {
              ...dims,
              scanLimits: s.scanLimit ? [s.scanLimit] : dims.scanLimits,
            })
          : lmsOp(test, index, t, dims);
    }
    return { ...op, params: { ...op.params, retries: args.writeRetries } };
  };
}
function verifiedExec(
  args: BenchArgs,
  s: ScenarioPlan,
  topology: TargetEvidence[],
) {
  if (s.mode === "direct")
    return makeDirectExec(args.tursoUrl, args.tursoToken, args.timeoutMs);
  const exec = makeEdgeExec(
    args.workerUrl,
    args.adminToken,
    s.backend,
    args.timeoutMs,
  );
  const spec = backendSpec(s.backend);
  return async (...p: Parameters<typeof exec>) => {
    const sample = await exec(...p);
    if (
      sample.tenantIsolation !== spec.tenantIsolation ||
      sample.engine !== spec.engine ||
      sample.requestedTenant !== Number(p[0].params.tenant)
    )
      throw new Error(
        "TOPOLOGY_INVALID: measured request routing differs from validated topology",
      );
    const target = topology.find(
      (t) =>
        t.backend === s.backend &&
        t.requestedTenant ===
          (spec.tenantIsolation === "database-per-tenant"
            ? Number(p[0].params.tenant)
            : 1),
    );
    if (!target)
      throw new Error("TOPOLOGY_INVALID: requested database not validated");
    if (
      sample.deploymentVersion !== target.deploymentVersion ||
      sample.sourceFingerprint !== target.sourceFingerprint
    )
      throw new Error(
        "TOPOLOGY_INVALID: Worker deployment changed after validation",
      );
    const identity =
      spec.engine === "d1" || spec.tenantIsolation === "shared-schema"
        ? target.bindingName
        : target.databaseIdentity;
    if (sample.databaseIdentity !== identity)
      throw new Error("TOPOLOGY_INVALID: measured database identity mismatch");
    return sample;
  };
}
function scenarioResult(
  args: BenchArgs,
  s: ScenarioPlan,
  runs: ScenarioResult["runs"],
  topology: TargetEvidence[],
): ScenarioResult {
  const spec = backendSpec(s.backend);
  const summary = summarizeRuns(runs);
  const warnings = [...(summary.warnings ?? [])];
  if (topology.filter((t) => t.backend === s.backend).some((t) => !t.region))
    warnings.push("BACKEND_REGION_UNKNOWN");
  return {
    ...s,
    backend: s.backend,
    tenantIsolation: spec.tenantIsolation,
    databaseCount:
      spec.tenantIsolation === "database-per-tenant" ? s.tenantCount : 1,
    routingStrategy: spec.routingStrategy,
    accessLayer: spec.accessLayer,
    profileMix: profileMix(s.test, args.workload, args.profileMix),
    engine: runs.find((r) => r.engine)?.engine ?? spec.engine,
    transactionMode:
      runs.find((r) => r.transactionMode)?.transactionMode ?? null,
    workload:
      s.test === "mixed"
        ? args.workload
        : s.test.startsWith("lms-")
          ? "lms"
          : KV_TESTS[s.test]
            ? "kv"
            : "lms",
    durationSec: s.examStudents ? 0 : args.duration,
    iterations: s.examStudents ?? args.iterations,
    timeoutMs: args.timeoutMs,
    runs,
    summary,
    warnings,
  };
}
export async function runBenchmark(
  args: BenchArgs,
  selection = plans(args),
): Promise<ResultDoc> {
  const csv = args.out.endsWith(".json")
    ? args.out.slice(0, -5) + ".csv"
    : args.out + ".csv";
  const comparisonPath = args.out.replace(/\.json$/, "") + "-comparison.json";
  const coldCsv = csv.replace(/\.csv$/, "-cold-start.csv");
  for (const path of [
    args.out,
    csv,
    comparisonPath,
    ...(selection.some((s) => s.idleMs !== undefined) ? [coldCsv] : []),
  ]) {
    let exists = false;
    try {
      await access(path);
      exists = true;
    } catch {}
    if (exists)
      throw new Error(`refusing to overwrite historical result: ${path}`);
  }
  const checkpoint = await createCheckpoint(
    args.out.replace(/\.json$/, "") + "-checkpoint.json",
  );
  await checkpoint({
    status: "validating",
    workerUrl: args.workerUrl,
    backends: [...new Set(selection.map((s) => s.backend))],
    expectedTenantCount: Math.max(...selection.map((s) => s.tenantCount)),
    plannedSteps: selection.length * args.runs,
  });
  const dims: Dims = {
    seedRows: args.seedRows,
    tenants: args.tenants,
    studentsPerTenant: args.studentsPerTenant,
    coursesPerTenant: args.coursesPerTenant,
    lessonsPerCourse: args.lessonsPerCourse,
    scanLimits: args.scanLimits,
  };
  const timestamp = new Date().toISOString();
  const edge = selection.filter((s) => s.mode === "edge");
  console.log(
    `Validating topology before measurement: ${edge.length} scenarios, runs=${args.runs}`,
  );
  const topology = edge.length
    ? await preflight(
        args,
        [...new Set(edge.map((s) => s.backend))],
        Math.max(...edge.map((s) => s.tenantCount)),
      )
    : [];
  const safeArgs = Object.fromEntries(
    Object.entries(args).filter(
      ([k]) =>
        !k.toLowerCase().includes("token") &&
        !["tursoUrl", "tursodbUrl"].includes(k),
    ),
  );
  const environment: EnvInfo = {
    timestamp,
    workerUrl: args.workerUrl,
    gitCommit: await git("rev-parse", "HEAD"),
    gitDirty: (await git("status", "--porcelain")).length > 0,
    deploymentVersion: topology[0]?.deploymentVersion ?? null,
    runnerLabel: args.runnerLabel,
    runnerRegion: args.runnerRegion,
    runnerHost: hostname(),
    schemaFingerprint: await expectedFingerprint(),
    arguments: safeArgs,
    poolConfiguration: args.pool,
    timeoutConfiguration: {
      runnerOperationMs: args.timeoutMs,
      workerOperationMs: args.timeoutMs,
      tursoQueryTimeoutMs: args.timeoutMs,
      tursoPoolCheckoutTimeoutMs: args.timeoutMs,
      transactionMaxMs: args.timeoutMs,
      validationMs: args.validationTimeoutMs,
      d1Cancellation:
        "binding has no caller-controlled cancellation; deadline bounds observation, not server execution",
    },
    topology,
    colo: topology[0]?.colo ?? null,
    tursoHost: null,
    tursodbHost: null,
    tursodbConfigured: topology.some((t) => t.engine === "tursodb"),
    kvRows: {},
    lmsRows: {},
    concurrencyRows: {},
    kvPayloadBytes: 200,
    seedDims: {
      seedRows: args.seedRows,
      tenants: args.tenants,
      studentsPerTenant: args.studentsPerTenant,
      coursesPerTenant: args.coursesPerTenant,
      lessonsPerCourse: args.lessonsPerCourse,
    },
    sdk: {
      tursoServerless: await version("@tursodatabase/serverless"),
      drizzleOrm: await version("drizzle-orm"),
    },
    wrangler: await version("wrangler"),
    bun: Bun.version,
    notes: [
      "Cloudflare Worker clocks advance only after I/O. Routing, allocation, Drizzle composition and serialization CPU timings are unavailable; DB/server clock spans are approximate I/O observations, not CPU profiles.",
      "Topology validation warms infrastructure before the first idle interval. Idle is runner silence, never forced infrastructure cold start.",
      "SDK connection creation is allocation, not TCP/TLS establishment. Query timing includes Worker-to-database transport; it excludes platform time before handler execution.",
      "In-flight counts are runner-observed operations; timed-out writes may still commit. SDK clients are bounded per isolate, not globally.",
      "Drizzle labels measure SQL composition only, not a full ORM adapter. Direct mode is reference-only.",
      "p99 is suppressed in comparison reports below 1,000 successful operations per run. Bootstrap CI resamples independent run metrics; runs share mutable database state.",
    ],
  };
  const scenarios: ScenarioResult[] = [];
  const coldStarts: ColdStartRun[] = [];
  // Rotate backend ordering across run repetitions to reduce time-of-day bias.
  const records = new Map<ScenarioPlan, ScenarioResult["runs"]>();
  let completedSteps = 0;
  const saveProgress = async (
    run: number,
    current: ScenarioPlan,
    status = "running",
  ) =>
    checkpoint({
      status,
      timestamp,
      environment,
      progress: {
        run,
        runs: args.runs,
        completedSteps,
        plannedSteps: selection.length * args.runs,
        currentScenario: current,
      },
      scenarios: [...records].map(([plan, rows]) =>
        scenarioResult(args, plan, rows, topology),
      ),
      coldStarts,
    });
  for (let run = 1; run <= args.runs; run++) {
    const ordered = roundPlans(selection, run);
    for (const s of ordered) {
      const spec = backendSpec(s.backend);
      const exec = verifiedExec(args, s, topology);
      const ops = workload(args, s, dims);
      if (s.idleMs !== undefined) {
        coldStarts.push(
          await runColdStart({
            run,
            backend: s.backend,
            tenantMode: s.tenantMode,
            tenantCount: s.tenantCount,
            samples: args.coldSamples,
            idleMs: s.idleMs,
            warmRequests: args.coldWarmRequests,
            timeoutMs: args.timeoutMs,
            ops: (i) =>
              kvOp(
                "point-read",
                i,
                s.tenantMode === "single"
                  ? 1
                  : tenantSelector(s.tenantCount, args.traffic)(i),
                dims,
              ),
            exec,
            variant: s.variant,
            tenantIsolation: spec.tenantIsolation,
            seedRows: args.seedRows,
          }),
        );
        completedSteps++;
        await saveProgress(run, s);
        continue;
      }
      const totalOps = s.examStudents ?? args.iterations;
      const durationSec = s.examStudents ? 0 : args.duration;
      const outcome = await runScenario({
        label: `${s.suite}/${s.backend}/${s.test} t=${s.tenantCount} c=${s.concurrency} rps=${s.offeredRps ?? "-"} run=${run}`,
        ops,
        totalOps,
        durationSec,
        concurrency: s.concurrency,
        warmup: s.examStudents ? 0 : args.warmup,
        loadModel: s.loadModel,
        timeoutMs: args.timeoutMs,
        exec,
        targetRps: s.offeredRps ?? undefined,
        maxInFlight: args.maxInFlight,
        maxStartLagMs: args.maxStartLagMs,
        tenantCount: s.tenantCount,
        operationMix:
          profileMix(s.test, args.workload, args.profileMix) ?? undefined,
      });
      if (outcome.warnings?.includes("TOPOLOGY_INVALID"))
        throw new Error(
          "TOPOLOGY_INVALID: invalid measured routing; comparison aborted",
        );
      const rows = records.get(s) ?? [];
      rows.push({ run, ...outcome });
      records.set(s, rows);
      completedSteps++;
      await saveProgress(run, s);
    }
  }
  scenarios.push(
    ...[...records].map(([s, runs]) => scenarioResult(args, s, runs, topology)),
  );
  const comparison = manhaliSummary(scenarios, coldStarts);
  const doc: ResultDoc = {
    timestamp,
    environment,
    scenarios,
    coldStarts,
    comparison,
  };
  printManhali(scenarios, coldStarts);
  await writeArtifacts({
    [args.out]: serializeResult(doc),
    [csv]: toCsv(scenarios),
    [comparisonPath]: serializeResult({ timestamp, environment, comparison }),
    ...(coldStarts.length ? { [coldCsv]: coldStartCsv(coldStarts) } : {}),
  });
  await checkpoint({
    status: "complete",
    timestamp,
    environment,
    scenarios,
    coldStarts,
    comparison,
    progress: { completedSteps, plannedSteps: selection.length * args.runs },
  });
  console.log(`wrote ${args.out}, ${csv}, ${comparisonPath}`);
  return doc;
}
if (import.meta.main) await runBenchmark(parseArgs());
