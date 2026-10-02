// A supervisor owns setup, the measured child and the deadline. No provisioning here.
import { open, rename } from "node:fs/promises";
import { basename } from "node:path";
import { parseArgs, getRaw } from "../src/config.ts";
import { runBenchmark } from "../src/bench.ts";
import { limitedPlans, runBudget, budgetExpired, savePartialCheckpoint } from "../src/run-budget.ts";
import { safeError } from "../shared/policy.ts";

const args = {
  ...parseArgs({ manhali: true, full: true }),
  tenants: Number(getRaw("tenants", "30")),
  runs: Number(getRaw("runs", "5")),
  maxRunMs: Number(getRaw("max-run-ms", "21600000")),
};
const budget = runBudget(
  args.maxRunMs,
  Number(process.env.BENCH_JOB_STARTED_AT_MS ?? Date.now()),
);
const selection = limitedPlans(args);

if (process.argv.includes("--benchmark-child")) {
  await runBenchmark({ ...args, ...budget }, selection);
} else {
  const prefix = args.out.replace(/\.json$/, "");
  const seedProgressPrefix = basename(getRaw("seed-progress-prefix", prefix));
  for (const path of [args.out, prefix + "-checkpoint.json"])
    if (await Bun.file(path).exists())
      throw new Error(`refusing to reuse a historical result path: ${path}`);
  const statusPath = prefix + "-run.json";
  const statusHandle = await open(statusPath, "wx", 0o600);
  const base = {
    ...budget,
    pid: process.pid,
    resultPath: args.out,
    tenantCount: args.tenants,
    backends: args.backends,
    runs: args.runs,
    plannedSteps: selection.length * args.runs,
    systemdUnit: process.env.BENCH_SYSTEMD_UNIT ?? null,
    setupResumedFrom: seedProgressPrefix === basename(prefix) ? null : seedProgressPrefix,
  };
  await statusHandle.writeFile(JSON.stringify({ ...base, status: "starting" }));
  await statusHandle.close();
  async function writeStatus(status: string, extra: Record<string, unknown> = {}) {
    const temporary = statusPath + "." + crypto.randomUUID() + ".tmp";
    const handle = await open(temporary, "wx", 0o600);
    await handle.writeFile(JSON.stringify({ ...base, status, updatedAt: new Date().toISOString(), ...extra }, null, 2));
    await handle.close();
    await rename(temporary, statusPath);
  }
  const children = new Set<ReturnType<typeof Bun.spawn>>();
  let stopReason: "budget-exhausted" | "stopped" | null = null;
  let forceKillTimer: ReturnType<typeof setTimeout> | undefined;
  function stop(reason: "budget-exhausted" | "stopped") {
    if (stopReason) return;
    stopReason = reason;
    for (const child of children) child.kill("SIGTERM");
    forceKillTimer = setTimeout(() => {
      for (const child of children) child.kill("SIGKILL");
    }, 2000);
  }
  const onSignal = () => stop("stopped");
  process.on("SIGTERM", onSignal);
  process.on("SIGINT", onSignal);
  const watchdog = setInterval(() => {
    if (budgetExpired(budget)) stop("budget-exhausted");
  }, 1000);
  async function child(command: string[], env: Record<string, string> = {}) {
    if (budgetExpired(budget)) stop("budget-exhausted");
    if (stopReason) throw new Error(stopReason);
    const processChild = Bun.spawn(command, {
      stdout: "inherit", stderr: "inherit", stdin: "ignore",
      env: { ...process.env, ...env, BENCH_JOB_STARTED_AT_MS: String(Date.parse(budget.startedAt)) },
    });
    children.add(processChild);
    const code = await processChild.exited;
    children.delete(processChild);
    if (code !== 0) throw new Error(`benchmark child exited with code ${code}`);
  }
  try {
    if (getRaw("reseed", "false") === "true") {
      if (args.backends.some((b) => !["d1-per-tenant", "turso-per-tenant", "tursodb-per-tenant"].includes(b)))
        throw new Error("reseed supports the three isolated benchmark engines only");
      await writeStatus("seeding");
      const setup = await Promise.allSettled(args.backends.map(async (backend) => {
        const progress = `bench-private-${seedProgressPrefix}-seed-${backend}.json`;
        for (let attempt = 1; attempt <= 3; attempt++) {
          try {
            await child([process.execPath, "scripts/seed-isolated-set.ts"], {
              BACKEND: backend, TENANT_COUNT: String(args.tenants),
              TENANTS: String(args.tenants), SEED_PROGRESS_FILE: progress,
              SEED_CONCURRENCY: backend === "d1-per-tenant" ? "2" : "3",
              WORKER_URL: args.workerUrl, SEED_ROWS: String(args.seedRows), ROWS: String(args.seedRows),
              STUDENTS_PER_TENANT: String(args.studentsPerTenant),
              COURSES_PER_TENANT: String(args.coursesPerTenant),
              LESSONS_PER_COURSE: String(args.lessonsPerCourse),
            });
            return;
          } catch (error) {
            if (stopReason || attempt === 3) throw error;
            console.warn(`Resuming setup ${backend}, attempt ${attempt + 1}`);
          }
        }
      }));
      const failure = setup.find((s) => s.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
    }
    await writeStatus("running");
    await child([process.execPath, "scripts/run-limited-benchmark.ts", ...process.argv.slice(2), "--benchmark-child"], {
      TENANTS: String(args.tenants), RUNS: String(args.runs), MAX_RUN_MS: String(args.maxRunMs),
    });
    // The child may finish just as the deadline fires; a valid final JSON wins.
    await Bun.file(args.out).json();
    await writeStatus("complete");
  } catch (error) {
    const state = stopReason ?? "failed";
    for (const processChild of children) processChild.kill("SIGTERM");
    await Promise.allSettled([...children].map((p) => p.exited));
    const resultPath = await savePartialCheckpoint(args.out, state, budget);
    await writeStatus(state, { resultPath, error: safeError(error).error });
    if (state === "failed") process.exitCode = 1;
  } finally {
    clearInterval(watchdog);
    clearTimeout(forceKillTimer);
    process.off("SIGTERM", onSignal);
    process.off("SIGINT", onSignal);
  }
}
