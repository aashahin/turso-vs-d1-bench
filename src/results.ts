// Shared result types for the JSON/CSV/console reporters.
import type { ErrorClass, Summary, TxStats } from "./stats.ts";

export interface RunRecord {
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
  medianP50: number;
  medianP95: number;
  medianP99: number;
  medianRps: number;
  medianDbP50: number | null;
  medianErrorRate: number;
  medianReplicaRate: number | null;
  /** Counters summed across runs so conflict/retry rates stay exact. */
  tx: TxStats | null;
}

export interface ScenarioResult {
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
  seedDims: { seedRows: number; tenants: number; studentsPerTenant: number; coursesPerTenant: number; lessonsPerCourse: number };
  sdk: { tursoServerless: string | null; drizzleOrm: string | null };
  wrangler: string | null;
  bun: string | null;
  notes: string[];
}

export interface ResultDoc {
  timestamp: string;
  environment: EnvInfo;
  scenarios: ScenarioResult[];
}
