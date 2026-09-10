// TSV run table + saturation/distributed comparison tables. Reports medians
// across runs; never a single overall "winner" — the right backend depends on
// the workload, concurrency, and tenant layout.
import { fmt, median } from "../stats.ts";
import type { ScenarioResult } from "../results.ts";
import { READ_TESTS } from "../testsets.ts";

export function printRunTable(scenarios: ScenarioResult[]): void {
  const head = ["benchmark", "runs", "ok", "err", "err%", "rps", "p50", "p90", "p95", "p99", "max", "db p50", "rep%"];
  console.log(head.join("\t"));
  for (const s of scenarios) {
    const ok = s.runs.reduce((a, r) => a + r.success, 0);
    const err = s.runs.reduce((a, r) => a + r.failed, 0);
    const rep = s.summary.medianReplicaRate;
    console.log(
      [
        `${s.mode}/${s.backend}/${s.test} ${s.tenantMode}/t${s.tenantCount} c=${s.concurrency}`,
        String(s.runs.length),
        String(ok),
        String(err),
        (s.summary.medianErrorRate * 100).toFixed(1),
        Math.round(s.summary.medianRps).toString(),
        fmt(s.summary.medianP50),
        fmt(median(s.runs.map((r) => r.e2e.p90))),
        fmt(s.summary.medianP95),
        fmt(s.summary.medianP99),
        fmt(median(s.runs.map((r) => r.e2e.max))),
        s.summary.medianDbP50 === null ? "-" : fmt(s.summary.medianDbP50),
        rep === null ? "-" : String(Math.round(rep * 100)),
      ].join("\t"),
    );
  }
}

function cell(v: number | null): string {
  return v === null ? "  -  " : fmt(v).padStart(5, " ");
}

export function printSaturation(scenarios: ScenarioResult[]): void {
  const singles = scenarios.filter((s) => s.tenantMode === "single" && s.mode === "edge");
  if (singles.length === 0) return;
  for (const section of ["READ LATENCY", "WRITE LATENCY"] as const) {
    const rows = singles.filter((s) => (section === "READ LATENCY") === (READ_TESTS[s.test] === true));
    if (rows.length === 0) continue;
    console.log(`\n${section} — SINGLE TENANT SATURATION (server-side db p50 ms, median across runs)`);
    const tests = [...new Set(rows.map((s) => s.test))].sort();
    for (const test of tests) {
      const group = rows.filter((s) => s.test === test);
      const concs = [...new Set(group.map((s) => s.concurrency))].sort((a, b) => a - b);
      const backs = [...new Set(group.map((s) => s.backend))].sort();
      console.log(`\n${test}`);
      console.log(["backend".padEnd(24), ...concs.map((c) => `c=${c}`.padStart(5, " "))].join("  "));
      for (const b of backs) {
        const first = group.find((s) => s.backend === b);
        const line = [first === undefined || first.engine === null ? b.padEnd(24) : `${b} [${first.engine}]`.padEnd(24)];
        for (const c of concs) {
          const hit = group.find((s) => s.backend === b && s.concurrency === c);
          line.push(cell(hit?.summary.medianDbP50 ?? null));
        }
        console.log(line.join("  "));
      }
    }
  }
}

/**
 * Write/transaction comparison: one row per (test, backend, concurrency) with
 * the transaction mode and MVCC counters. Conflict and retry numbers are only
 * meaningful for `tursodb-concurrent` (BEGIN CONCURRENT); every other backend
 * reports "-".
 */
export function printWriteComparison(scenarios: ScenarioResult[]): void {
  const writes = scenarios.filter((s) => s.mode === "edge" && READ_TESTS[s.test] !== true);
  if (writes.length === 0) return;
  console.log("\nWRITE / TRANSACTION COMPARISON (edge only; medians across runs; conflicts/retries are server-side MVCC counters)");
  console.log(
    ["test", "backend", "engine", "txMode", "c", "ok", "err", "err%", "rps", "p50", "p95", "p99", "attempts", "committed", "conflicts", "conflict%", "retries", "retry%", "avgRetries", "okAfterRetry", "failedAfterRetries"].join("\t"),
  );
  const sorted = [...writes].sort(
    (a, b) => a.test.localeCompare(b.test) || a.backend.localeCompare(b.backend) || a.concurrency - b.concurrency,
  );
  for (const s of sorted) {
    const ok = s.runs.reduce((a, r) => a + r.success, 0);
    const err = s.runs.reduce((a, r) => a + r.failed, 0);
    const tx = s.summary.tx;
    console.log(
      [
        s.test,
        s.backend,
        s.engine ?? "-",
        s.transactionMode ?? "-",
        String(s.concurrency),
        String(ok),
        String(err),
        (s.summary.medianErrorRate * 100).toFixed(1),
        Math.round(s.summary.medianRps).toString(),
        fmt(s.summary.medianP50),
        fmt(s.summary.medianP95),
        fmt(s.summary.medianP99),
        tx === null ? "-" : String(tx.attemptsTotal),
        tx === null ? "-" : String(tx.committed),
        tx === null ? "-" : String(tx.conflicts),
        tx === null ? "-" : (tx.conflictRate * 100).toFixed(1),
        tx === null ? "-" : String(tx.retries),
        tx === null ? "-" : (tx.retryRate * 100).toFixed(1),
        tx === null ? "-" : tx.avgRetries.toFixed(3),
        tx === null ? "-" : String(tx.successAfterRetry),
        tx === null ? "-" : String(tx.failedAfterRetries),
      ].join("\t"),
    );
  }
}

export function printDistributed(scenarios: ScenarioResult[]): void {
  const dist = scenarios.filter((s) => s.tenantMode === "distributed" && s.mode === "edge");
  if (dist.length === 0) return;
  console.log("\nDISTRIBUTED TENANTS (median across runs)");
  console.log(["backend", "test", "tenants", "concurrency", "RPS", "per-tenant RPS", "p95", "err%"].join("\t"));
  const sorted = [...dist].sort((a, b) => a.tenantCount - b.tenantCount || a.concurrency - b.concurrency);
  for (const s of sorted) {
    console.log(
      [
        s.backend,
        s.test,
        String(s.tenantCount),
        String(s.concurrency),
        Math.round(s.summary.medianRps).toString(),
        (s.summary.medianRps / Math.max(1, s.tenantCount)).toFixed(1),
        fmt(s.summary.medianP95),
        (s.summary.medianErrorRate * 100).toFixed(1),
      ].join("\t"),
    );
  }
}

export function printBests(scenarios: ScenarioResult[]): void {
  const edge = scenarios.filter((s) => s.mode === "edge");
  if (edge.length === 0) return;
  console.log("\nBEST PER CATEGORY (edge only; direct mode is reference-only and excluded)");
  const keys = [...new Set(edge.map((s) => `${s.test}|${s.tenantMode}|t${s.tenantCount}|c${s.concurrency}`))].sort();
  for (const key of keys) {
    const group = edge.filter((s) => `${s.test}|${s.tenantMode}|t${s.tenantCount}|c${s.concurrency}` === key);
    if (group.length < 2) continue;
    const by = (f: (s: ScenarioResult) => number): ScenarioResult => group.reduce((a, b) => (f(a) <= f(b) ? a : b));
    const byMax = (f: (s: ScenarioResult) => number): ScenarioResult => group.reduce((a, b) => (f(a) >= f(b) ? a : b));
    console.log(
      `${key}: best p50=${by((s) => s.summary.medianP50).backend} best p95=${by((s) => s.summary.medianP95).backend} best throughput=${byMax((s) => s.summary.medianRps).backend} lowest errors=${by((s) => s.summary.medianErrorRate).backend}`,
    );
  }
}
