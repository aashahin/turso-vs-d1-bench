import { readFile } from "node:fs/promises";
import { serializeResult, writeArtifacts } from "./artifacts.ts";
import type { BenchArgs } from "./config.ts";
import { canonicalPlans } from "./plan.ts";

/** Broad coverage with short idle waits; repetition count remains explicit in args. */
export function limitedPlans(args: BenchArgs) {
  return canonicalPlans(args, true).filter(
    (scenario) => scenario.idleMs === undefined || scenario.idleMs === 30000,
  );
}

export function runBudget(maxRunMs: number, startedAtMs = Date.now()) {
  if (!Number.isInteger(maxRunMs) || maxRunMs < 60000 || maxRunMs > 86400000)
    throw new Error("--max-run-ms must be an integer from 60000 to 86400000");
  return {
    startedAt: new Date(startedAtMs).toISOString(),
    deadlineAt: new Date(startedAtMs + maxRunMs).toISOString(),
    maxRunMs,
    // Reserve time to terminate child requests and export the last checkpoint.
    finishReserveMs: Math.min(30000, Math.floor(maxRunMs / 10)),
  };
}
export type RunBudget = ReturnType<typeof runBudget>;

export function budgetExpired(budget: RunBudget, nowMs = Date.now()) {
  return nowMs >= Date.parse(budget.deadlineAt) - budget.finishReserveMs;
}

/** Preserve completed failures too. Uncheckpointed operations remain explicitly unknown. */
export async function savePartialCheckpoint(
  out: string,
  status: "budget-exhausted" | "stopped" | "failed",
  budget: RunBudget,
) {
  const sourceCheckpoint = out.replace(/\.json$/, "") + "-checkpoint.json";
  let checkpoint: Record<string, unknown> = {};
  try {
    checkpoint = JSON.parse(await readFile(sourceCheckpoint, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const { checkpointOwner: _owner, ...document } = checkpoint;
  const content = serializeResult({
    ...document,
    timestamp: document.timestamp ?? budget.startedAt,
    status,
    partial: true,
    completion: {
      ...budget,
      savedAt: new Date().toISOString(),
      lastMeasurementAt: checkpoint.updatedAt ?? null,
      sourceCheckpoint,
      unfinishedOperationAccounting:
        "Only completed checkpoint records are available. Uncheckpointed attempts have unknown outcomes; reliability metrics describe completed scenario records only.",
    },
    warnings: ["INCOMPLETE_RUN", "EXPLORATORY_SINGLE_OR_FEW_RUNS"],
  });
  try {
    await writeArtifacts({ [out]: content });
    return out;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    // A deadline might interrupt final artifact writing. Preserve that file too.
    const alternative = out.replace(/\.json$/, "") + "-partial.json";
    await writeArtifacts({ [alternative]: content });
    return alternative;
  }
}
