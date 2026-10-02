// CSV export: one row per run, easy to graph. Never includes tokens.
import type { ScenarioResult } from "../results.ts";

const COLUMNS = [
  "suite",
  "tenantIsolation",
  "databaseCount",
  "routingStrategy",
  "accessLayer",
  "distribution",
  "offeredRps",
  "scheduled",
  "started",
  "completed",
  "dropped",
  "lateStarts",
  "queueP95",
  "peakInFlight",
  "poolWaitP50",
  "connectionReuseRate",
  "connectionsCreated",
  "poolEvictions",
  "warnings",
  "backend",
  "engine",
  "transactionMode",
  "test",
  "workload",
  "tenantMode",
  "tenantCount",
  "concurrency",
  "run",
  "success",
  "errors",
  "errorRate",
  "rps",
  "p50",
  "p90",
  "p95",
  "p99",
  "mean",
  "max",
  "dbP50",
  "replicaRate",
  "attempted",
  "attemptsTotal",
  "committed",
  "successAfterRetry",
  "failedAfterRetries",
  "conflicts",
  "conflictRate",
  "retries",
  "retryRate",
  "avgRetries",
  "mode",
  "loadModel",
] as const;

function num(v: number | null): string {
  return v === null ? "" : String(Math.round(v * 100) / 100);
}

export function toCsv(scenarios: ScenarioResult[]): string {
  const lines = [COLUMNS.join(",")];
  for (const s of scenarios) {
    for (const r of s.runs) {
      const tx = r.tx;
      lines.push(
        [
          s.suite ?? "",
          s.tenantIsolation ?? "legacy-unknown",
          s.databaseCount ?? "",
          s.routingStrategy ?? "",
          s.accessLayer ?? "raw",
          s.distribution ?? "",
          s.offeredRps ?? "",
          r.arrival?.scheduled ?? "",
          r.arrival?.started ?? "",
          r.arrival?.completed ?? "",
          r.arrival?.dropped ?? "",
          r.arrival?.lateStarts ?? "",
          r.arrival?.queueDelay.p95 ?? "",
          r.arrival?.peakInFlight ?? "",
          r.pool?.wait?.p50 ?? "",
          r.pool?.reuseRate ?? "",
          r.pool?.events.connectionCreated ?? "",
          r.pool?.events.poolEviction ?? "",
          (s.warnings ?? []).join("|"),
          s.backend,
          s.engine ?? "",
          s.transactionMode ?? "",
          s.test,
          s.workload,
          s.tenantMode,
          String(s.tenantCount),
          String(s.concurrency),
          String(r.run),
          String(r.success),
          String(r.failed),
          r.errorRate.toFixed(4),
          Math.round(r.rps * 100) / 100,
          num(r.e2e.n > 0 ? r.e2e.p50 : null),
          num(r.e2e.n > 0 ? r.e2e.p90 : null),
          num(r.e2e.n > 0 ? r.e2e.p95 : null),
          num(r.e2e.n >= 1000 ? r.e2e.p99 : null),
          num(r.e2e.n > 0 ? r.e2e.mean : null),
          num(r.e2e.n > 0 ? r.e2e.max : null),
          r.db === null ? "" : num(r.db.p50),
          r.replicaRate === null ? "" : r.replicaRate.toFixed(4),
          tx === null ? "" : String(tx.attempted),
          tx === null ? "" : String(tx.attemptsTotal),
          tx === null ? "" : String(tx.committed),
          tx === null ? "" : String(tx.successAfterRetry),
          tx === null ? "" : String(tx.failedAfterRetries),
          tx === null ? "" : String(tx.conflicts),
          tx === null ? "" : tx.conflictRate.toFixed(4),
          tx === null ? "" : String(tx.retries),
          tx === null ? "" : tx.retryRate.toFixed(4),
          tx === null ? "" : tx.avgRetries.toFixed(4),
          s.mode,
          s.loadModel,
        ]
          .map((v) => {
            const text = String(v);
            return /[",\n\r]/.test(text)
              ? `"${text.replace(/"/g, '""')}"`
              : text;
          })
          .join(","),
      );
    }
  }
  return lines.join("\n") + "\n";
}
