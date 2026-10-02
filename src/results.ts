import type { RunOutcome } from "./runner.ts";
import type { TargetEvidence } from "./topology.ts";
// Shared result types for the JSON/CSV/console reporters.
import type { ErrorClass, Summary, TxStats } from "./stats.ts";
import { dispersion, median, mergeTx } from "./stats.ts";
import type { ColdStartRun } from "./cold-start.ts";

export interface RunRecord extends RunOutcome {
  run: number;
  e2e: Summary;
  db: Summary | null;
  query: Summary | null;
  success: number;
  failed: number;
  errorRate: number;
  errorsByClass: Record<ErrorClass, number>;
  errorsByStatus: Record<string, number>;
  sampleErrors: string[];
  replicaRate: number | null;
  replicaReads: number;
  primaryReads: number;
  totalReads: number;
  regions: Record<string, number>;
  /** Engine reported by the Worker: d1 | libsql | tursodb. */
  engine: string | null;
  /** Transaction form actually executed: none | transaction | concurrent. */
  transactionMode: string | null;
  /** MVCC/transaction counters (null for backends without transactions). */
  tx: TxStats | null;
  rps: number;
  wallMs: number;
}

export interface ScenarioSummary {
  medianP50: number | null;
  medianP95: number | null;
  medianP99: number | null;
  medianRps: number;
  medianDbP50: number | null;
  medianErrorRate: number;
  medianReplicaRate: number | null;
  /** Counters summed across runs so conflict/retry rates stay exact. */
  tx: TxStats | null;
  variation?: Record<string, ReturnType<typeof dispersion>>;
  pooledErrorRate?: number;
  warnings?: string[];
}

export interface ScenarioResult {
  suite?: string;
  tenantIsolation?: string;
  databaseCount?: number;
  routingStrategy?: string;
  accessLayer?: string;
  distribution?: string;
  offeredRps?: number | null;
  profileMix?: Record<string, number> | null;
  topology?: TargetEvidence[];
  warnings?: string[];
  mode: string;
  backend: string;
  engine: string | null;
  transactionMode: string | null;
  test: string;
  workload: string;
  scanLimit: number | null;
  tenantMode: string;
  tenantCount: number;
  concurrency: number;
  loadModel: string;
  durationSec: number;
  iterations: number;
  timeoutMs: number;
  runs: RunRecord[];
  summary: ScenarioSummary;
}

export interface EnvInfo {
  gitCommit?: string;
  gitDirty?: boolean;
  deploymentVersion?: string | null;
  runnerLabel?: string;
  runnerRegion?: string;
  runnerHost?: string;
  schemaFingerprint?: string;
  arguments?: Record<string, unknown>;
  poolConfiguration?: unknown;
  timeoutConfiguration?: unknown;
  topology?: TargetEvidence[];
  timestamp: string;
  workerUrl: string;
  colo: string | null;
  tursoHost: string | null;
  tursodbHost: string | null;
  tursodbConfigured: boolean;
  kvRows: Record<string, number | null>;
  lmsRows: Record<string, number | null>;
  concurrencyRows: Record<string, number | null>;
  kvPayloadBytes: number;
  seedDims: {
    seedRows: number;
    tenants: number;
    studentsPerTenant: number;
    coursesPerTenant: number;
    lessonsPerCourse: number;
  };
  sdk: { tursoServerless: string | null; drizzleOrm: string | null };
  wrangler: string | null;
  bun: string | null;
  notes: string[];
}

export interface ResultDoc {
  timestamp: string;
  environment: EnvInfo;
  scenarios: ScenarioResult[];
  coldStarts: ColdStartRun[];
  comparison?: unknown;
}

/** Missing latency is not zero latency; failure rates still include every run. */
export function summarizeRuns(runs: RunRecord[]): ScenarioSummary {
  const successful = runs.filter((r) => r.e2e.n > 0);
  const db = runs.flatMap((r) =>
    r.db !== null && r.db.n > 0 ? [r.db.p50] : [],
  );
  const replicas = runs.flatMap((r) =>
    r.replicaRate === null ? [] : [r.replicaRate],
  );
  const variation = {
    p50: dispersion(successful.map((r) => r.e2e.p50)),
    p95: dispersion(successful.map((r) => r.e2e.p95)),
    p99: dispersion(
      successful.filter((r) => r.e2e.n >= 1000).map((r) => r.e2e.p99),
    ),
    rps: dispersion(runs.map((r) => r.rps)),
    errorRate: dispersion(runs.map((r) => r.errorRate)),
  };
  const warnings = [...new Set(runs.flatMap((r) => r.warnings ?? []))];
  if (runs.length < 5) warnings.push("EXPLORATORY_SINGLE_OR_FEW_RUNS");
  if (Object.values(variation).some((v) => v && v.coefficientOfVariation > 0.2))
    warnings.push("HIGH_VARIANCE");
  return {
    variation,
    warnings,
    pooledErrorRate:
      runs.reduce((n, r) => n + r.failed, 0) /
      Math.max(
        1,
        runs.reduce((n, r) => n + r.success + r.failed, 0),
      ),
    medianP50: successful.length
      ? median(successful.map((r) => r.e2e.p50))
      : null,
    medianP95: successful.length
      ? median(successful.map((r) => r.e2e.p95))
      : null,
    medianP99:
      successful.length && successful.every((r) => r.e2e.n >= 1000)
        ? median(successful.map((r) => r.e2e.p99))
        : null,
    medianRps: median(runs.map((r) => r.rps)),
    medianDbP50: db.length ? median(db) : null,
    medianErrorRate: median(runs.map((r) => r.errorRate)),
    medianReplicaRate: replicas.length ? median(replicas) : null,
    tx: mergeTx(runs.flatMap((r) => (r.tx === null ? [] : [r.tx]))),
  };
}
