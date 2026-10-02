import type { ScenarioResult } from "../results.ts";
import type { ColdStartRun } from "../cold-start.ts";
import { median } from "../stats.ts";
/** Keys contain every comparison dimension except backend/engine. No cross-topology/ORM rankings. */
export function comparisonKey(s: ScenarioResult): string {
  return JSON.stringify([
    s.mode,
    s.suite,
    s.test,
    s.accessLayer ?? "raw",
    s.tenantIsolation ?? "legacy-unknown",
    s.tenantCount,
    s.distribution,
    s.loadModel,
    s.offeredRps,
    s.concurrency,
    s.scanLimit,
    s.profileMix,
    s.timeoutMs,
  ]);
}
export function saturationEvidence(scenarios: ScenarioResult[]) {
  const groups = new Map<string, ScenarioResult[]>();
  for (const s of scenarios.filter(
    (s) => s.suite === "saturation" && s.loadModel === "arrival-rate",
  )) {
    const key = JSON.stringify([
      s.backend,
      s.test,
      s.tenantCount,
      s.distribution,
      s.accessLayer,
      s.tenantIsolation,
    ]);
    (groups.get(key) ?? (groups.set(key, []), groups.get(key)!)).push(s);
  }
  return [...groups].map(([key, rows]) => {
    rows.sort((a, b) => (a.offeredRps ?? 0) - (b.offeredRps ?? 0));
    const baseline = rows[0];
    const points = rows.map((s, i) => {
      const prior = rows[i - 1];
      const reasons: string[] = [];
      if (s.runs.some((r) => r.warnings?.includes("BUDGET_LIMITED_WINDOW")))
        reasons.push("offering window shortened by operation budget");
      if ((s.summary.pooledErrorRate ?? 1) >= 0.01)
        reasons.push("error rate >= 1%");
      if (
        s.runs.some(
          (r) =>
            r.arrival &&
            (r.arrival.dropped > 0 ||
              r.arrival.lateStarts / Math.max(1, r.arrival.started) > 0.01),
        )
      )
        reasons.push("offered load not achieved");
      if (s.runs.some((r) => (r.arrival?.queueDelay.p95 ?? 0) > 10))
        reasons.push("runner scheduling delay > 10ms p95");
      if (
        baseline?.summary.medianP95 &&
        (s.summary.medianP95 ?? 0) > baseline.summary.medianP95 * 2
      )
        reasons.push("p95 > 2x lowest offered-load baseline");
      if (
        baseline?.summary.medianP99 &&
        (s.summary.medianP99 ?? 0) > baseline.summary.medianP99 * 2
      )
        reasons.push("p99 > 2x lowest offered-load baseline");
      if (
        prior &&
        (s.offeredRps ?? 0) > (prior.offeredRps ?? 0) * 1.2 &&
        s.summary.medianRps <= prior.summary.medianRps * 1.05
      )
        reasons.push(
          "throughput plateau: <= 5% gain for > 20% offered-load increase",
        );
      return {
        offeredRps: s.offeredRps,
        rps: s.summary.medianRps,
        p95: s.summary.medianP95,
        p99: s.summary.medianP99,
        errorRate: s.summary.pooledErrorRate,
        reasons,
      };
    });
    const first = points.find((p) => p.reasons.length);
    const sustainable = points.filter((p) => !p.reasons.length);
    return {
      key,
      points,
      firstObservedInstability: first ?? null,
      highestTestedStableRps: sustainable.length
        ? Math.max(...sustainable.map((s) => s.offeredRps ?? 0))
        : null,
      qualification:
        "tested points only; latency thresholds are diagnostic, not a production SLA",
    };
  });
}
export function manhaliSummary(
  scenarios: ScenarioResult[],
  idle: ColdStartRun[],
) {
  const groups = new Map<string, ScenarioResult[]>();
  for (const s of scenarios.filter((s) => s.mode === "edge"))
    (
      groups.get(comparisonKey(s)) ??
      (groups.set(comparisonKey(s), []), groups.get(comparisonKey(s))!)
    ).push(s);
  const signals = [...groups].map(([comparison, rows]) => ({
    comparison,
    signalType:
      rows[0]?.test === "lms-balanced"
        ? "lowest balanced LMS p95"
        : rows[0]?.test === "exam-burst"
          ? "lowest exam burst p95"
          : rows[0]?.suite === "writes"
            ? "lowest write p95"
            : "lowest scenario p95",
    lowestP95:
      rows
        .filter((s) => s.summary.medianP95 !== null)
        .sort((a, b) => a.summary.medianP95! - b.summary.medianP95!)[0]
        ?.backend ?? null,
    lowestErrorRate:
      rows
        .filter((s) => s.runs.some((r) => r.success + r.failed > 0))
        .sort(
          (a, b) =>
            (a.summary.pooledErrorRate ?? 1) - (b.summary.pooledErrorRate ?? 1),
        )[0]?.backend ?? null,
    measurements: rows.map((s) => ({
      backend: s.backend,
      p50: s.summary.medianP50,
      p95: s.summary.medianP95,
      p99: s.summary.medianP99,
      errorRate: s.summary.pooledErrorRate,
      warning: s.warnings ?? s.summary.warnings,
      pool: s.test === "tenant-churn" ? s.runs.map((r) => r.pool) : undefined,
    })),
  }));
  const idleSignals = idle.map((r) => ({
    backend: r.backend,
    tenantIsolation: r.tenantIsolation,
    tenantCount: r.tenantCount,
    variant: r.variant,
    idleMs: r.idleMs,
    p50: r.first.e2e?.p50 ?? null,
    p95: r.first.e2e?.p95 ?? null,
    failures: r.first.failed,
    warnings: r.first.success < 1000 ? ["INSUFFICIENT_SAMPLES"] : [],
  }));
  const idleGroups = new Map<string, typeof idleSignals>();
  for (const row of idleSignals) {
    const key = JSON.stringify([
      row.tenantIsolation,
      row.tenantCount,
      row.variant,
      row.idleMs,
    ]);
    (
      idleGroups.get(key) ?? (idleGroups.set(key, []), idleGroups.get(key)!)
    ).push(row);
  }
  const idleDecisions = [...idleGroups].map(([comparison, rows]) => {
    const backends = [...new Set(rows.map((r) => r.backend))];
    const measured = backends
      .map((backend) => {
        const samples = rows.filter((r) => r.backend === backend);
        return {
          backend,
          p50: median(samples.flatMap((s) => (s.p50 === null ? [] : [s.p50]))),
          p95: median(samples.flatMap((s) => (s.p95 === null ? [] : [s.p95]))),
          successfulRuns: samples.filter((s) => s.p50 !== null).length,
          failures: samples.reduce((n, s) => n + s.failures, 0),
        };
      })
      .filter((r) => r.successfulRuns > 0);
    return {
      comparison,
      lowestIdleP50:
        measured.toSorted((a, b) => a.p50 - b.p50)[0]?.backend ?? null,
      lowestIdleP95:
        measured.toSorted((a, b) => a.p95 - b.p95)[0]?.backend ?? null,
      measurements: measured,
    };
  });
  return {
    signals,
    idleDecisions,
    idle: idleSignals,
    saturation: saturationEvidence(scenarios),
    notes: [
      "Signals compare only identical topology, access layer, workload, source, load and timeout policy. Lower latency does not imply better reliability. No weighted score or overall winner.",
    ],
  };
}
export function printManhali(
  scenarios: ScenarioResult[],
  idle: ColdStartRun[],
): void {
  for (const section of [
    "Latency",
    "Throughput",
    "Reliability",
    "Writes",
    "Tenant switching",
    "Pool behavior",
    "Saturation",
    "Topology",
  ]) {
    console.log(`\n${section}`);
    const selected =
      section === "Writes"
        ? scenarios.filter(
            (s) => s.suite === "writes" || s.test === "exam-burst",
          )
        : section === "Tenant switching" || section === "Pool behavior"
          ? scenarios.filter((s) => s.test === "tenant-churn")
          : section === "Saturation"
            ? scenarios.filter((s) => s.suite === "saturation")
            : scenarios;
    console.log(
      "backend\tengine\tisolation\ttenant/db\tdistribution\tload\tofferedRps\tc\tok/fail\terr%\trps\tp50\tp95\tp99\tdbP50\tpoolWaitP50\treuse\twarnings",
    );
    for (const s of selected) {
      const ok = s.runs.reduce((n, r) => n + r.success, 0),
        fail = s.runs.reduce((n, r) => n + r.failed, 0);
      const waits = s.runs.flatMap((r) =>
          r.pool?.wait ? [r.pool.wait.p50] : [],
        ),
        reuse = s.runs.flatMap((r) =>
          r.pool?.reuseRate != null ? [r.pool.reuseRate] : [],
        );
      console.log(
        [
          s.backend,
          s.engine,
          s.tenantIsolation,
          `${s.tenantCount}/${s.databaseCount}`,
          s.distribution,
          s.loadModel,
          s.offeredRps ?? "-",
          s.concurrency,
          `${ok}/${fail}`,
          100 * (s.summary.pooledErrorRate ?? 0),
          s.summary.medianRps,
          s.summary.medianP50,
          s.summary.medianP95,
          s.summary.medianP99 ?? "insufficient",
          s.summary.medianDbP50,
          waits.length ? median(waits) : "-",
          reuse.length ? median(reuse) : "-",
          (s.warnings ?? []).join("|"),
        ].join("\t"),
      );
    }
  }
  console.log(
    "\nIdle behavior — first request after runner idle; no guaranteed cold infrastructure",
  );
  for (const r of idle)
    console.log(
      `${r.backend} ${r.variant} idle=${r.idleMs} first ok=${r.first.success} fail=${r.first.failed} p50=${r.first.e2e?.p50 ?? "n/a"} p95=${r.first.e2e?.p95 ?? "n/a"} warnings=${r.first.success < 1000 ? "INSUFFICIENT_SAMPLES" : "-"}`,
    );
  console.log(
    "\nManhali decision signals (machine-readable comparison saved alongside results)",
  );
  console.log(JSON.stringify(saturationEvidence(scenarios)));
}
