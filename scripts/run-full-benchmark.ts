// Durable runner entry point: execution is separate from provisioning and seeding.
import { open, readFile, rename } from "node:fs/promises";
import { parseArgs, getRaw } from "../src/config.ts";
import { canonicalPlans } from "../src/plan.ts";
import { runBenchmark } from "../src/bench.ts";
import { safeError } from "../shared/policy.ts";
const args = parseArgs({ manhali: true, full: true });
const statusPath = args.out.replace(/\.json$/, "") + "-run.json";
const status = await open(statusPath, "wx", 0o600);
await status.writeFile(
  JSON.stringify({
    status: "starting",
    pid: process.pid,
    startedAt: new Date().toISOString(),
    resultPath: args.out,
    tenantCount: args.tenants,
    runs: args.runs,
  }),
);
await status.close();
async function writeStatus(state: string, extra: Record<string, unknown> = {}) {
  const temporary = statusPath + ".tmp";
  await Bun.write(
    temporary,
    JSON.stringify(
      {
        status: state,
        pid: process.pid,
        updatedAt: new Date().toISOString(),
        resultPath: args.out,
        tenantCount: args.tenants,
        runs: args.runs,
        ...extra,
      },
      null,
      2,
    ),
  );
  await rename(temporary, statusPath);
}
try {
  const seedFiles = getRaw("seed-ready-files", "").split(",").filter(Boolean);
  if (seedFiles.length) {
    for (;;) {
      const readiness = await Promise.all(
        seedFiles.map(async (file) => {
          try {
            const state = (await Bun.file(file).json()) as {
              completed: number[];
            };
            return { file, completed: state.completed.length };
          } catch {
            return { file, completed: 0 };
          }
        }),
      );
      if (readiness.every((state) => state.completed >= args.tenants)) break;
      await writeStatus("waiting-for-seeding", { readiness });
      await new Promise((resolve) => setTimeout(resolve, 30000));
    }
  }
  await writeStatus("running");
  await runBenchmark(args, canonicalPlans(args, true));
  await writeStatus("complete");
} catch (error) {
  await writeStatus("failed", { error: safeError(error).error });
  const checkpointPath = args.out.replace(/\.json$/, "") + "-checkpoint.json";
  try {
    const data = JSON.parse(await readFile(checkpointPath, "utf8"));
    const temporary = checkpointPath + ".failed.tmp";
    await Bun.write(
      temporary,
      JSON.stringify(
        {
          ...data,
          status: "failed",
          error: safeError(error).error,
          updatedAt: new Date().toISOString(),
        },
        null,
        2,
      ),
    );
    await rename(temporary, checkpointPath);
  } catch {}
  throw error;
}
