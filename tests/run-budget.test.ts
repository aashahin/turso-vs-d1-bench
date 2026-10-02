import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseArgs } from "../src/config.ts";
import { limitedPlans, runBudget, budgetExpired, savePartialCheckpoint } from "../src/run-budget.ts";

test("six-hour plan covers every engine at 30 tenants without long idle waits", () => {
  const args = { ...parseArgs({ manhali: true, full: true }), tenants: 30, runs: 5, adminToken: "fake" };
  const selection = limitedPlans(args);
  expect(new Set(selection.map((s) => s.backend))).toEqual(new Set(args.backends));
  expect(Math.max(...selection.map((s) => s.tenantCount))).toBe(30);
  expect(new Set(selection.filter((s) => s.test === "tenant-churn").map((s) => s.distribution)))
    .toEqual(new Set(["uniform", "hotset", "zipf"]));
  expect(selection.filter((s) => s.idleMs !== undefined).every((s) => s.idleMs === 30000)).toBe(true);
  expect(new Set(selection.filter((s) => s.test === "exam-burst").map((s) => s.examStudents)))
    .toEqual(new Set([50, 100, 250, 500]));
  expect(selection.some((s) => s.loadModel === "arrival-rate" && s.offeredRps === 500)).toBe(true);
  // The fixed windows leave room for setup, warmup, query drain and checkpoint export.
  const windows = selection.reduce((ms, s) => ms + (s.idleMs ?? (s.examStudents ? 0 : 30000)), 0) * args.runs;
  expect(windows).toBeLessThan(5 * 3600000);
});

test("deadline includes setup and reserves time for checkpoint export", () => {
  const start = Date.parse("2026-10-02T00:00:00Z");
  const budget = runBudget(21600000, start);
  expect(budget.deadlineAt).toBe("2026-10-02T06:00:00.000Z");
  expect(budgetExpired(budget, start + 21600000 - 30001)).toBe(false);
  expect(budgetExpired(budget, start + 21600000 - 30000)).toBe(true);
  // An interrupted runner resuming after the deadline must not start another child.
  expect(budgetExpired(budget, start + 8 * 3600000)).toBe(true);
  expect(() => runBudget(0, start)).toThrow();
});

test("deadline export preserves failures and checkpoint history without claiming completion", async () => {
  const directory = await mkdtemp(join(tmpdir(), "bench-budget-"));
  const out = join(directory, "result.json");
  const checkpoint = out.replace(".json", "-checkpoint.json");
  const original = JSON.stringify({
    checkpointOwner: "original-owner", status: "running", updatedAt: "2026-10-02T05:59:00Z",
    progress: { completedSteps: 1, plannedSteps: 585, run: 1, runs: 5 },
    environment: { arguments: { tenants: 30 } },
    scenarios: [{ backend: "d1-per-tenant", runs: [{ success: 4, failed: 3, errorsByClass: { timeout: 3 } }] }],
    coldStarts: [],
  });
  try {
    await writeFile(checkpoint, original);
    const resultPath = await savePartialCheckpoint(out, "budget-exhausted", runBudget(21600000));
    const saved = JSON.parse(await readFile(resultPath, "utf8"));
    expect(saved).toMatchObject({ status: "budget-exhausted", partial: true });
    expect(saved.scenarios).toEqual(JSON.parse(original).scenarios);
    expect(saved.progress.completedSteps).toBe(1);
    expect(saved.completion.unfinishedOperationAccounting).toContain("unknown outcomes");
    expect(saved.checkpointOwner).toBeUndefined();
    expect(await readFile(checkpoint, "utf8")).toBe(original);
    // Even an interrupted final result must survive fallback export.
    const alternative = await savePartialCheckpoint(out, "stopped", runBudget(21600000));
    expect(alternative).toBe(out.replace(".json", "-partial.json"));
    expect(JSON.parse(await readFile(out, "utf8")).status).toBe("budget-exhausted");
    await expect(savePartialCheckpoint(out, "failed", runBudget(21600000))).rejects.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
