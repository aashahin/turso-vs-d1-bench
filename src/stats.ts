// Latency statistics over successful operations. Failures are counted
// separately and never silently dropped: every run reports success, failed,
// errorRate, and errors grouped by class.
export interface Summary {
  n: number;
  min: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  max: number;
  mean: number;
  stdev: number;
  ops: number; // successful ops/sec over the measured wall time
}

export type ErrorClass = "timeout" | "http" | "rate-limit" | "backend" | "conflict" | "unknown";

export interface ClassifiedError {
  message: string;
  errorClass: ErrorClass;
  httpStatus: number | null;
}

export function classifyError(e: unknown): ClassifiedError {
  const message = e instanceof Error ? e.message : String(e);
  const lower = message.toLowerCase();
  if (e instanceof DOMException && e.name === "TimeoutError") {
    return { message, errorClass: "timeout", httpStatus: null };
  }
  if (lower.includes("aborted") || lower.includes("timeout") || lower.includes("timed out")) {
    return { message, errorClass: "timeout", httpStatus: null };
  }
  const http = message.match(/HTTP\s+(\d{3})/);
  const httpStatus = http?.[1] ? Number(http[1]) : null;
  if (httpStatus === 429 || lower.includes("rate limit") || lower.includes("too many requests") || lower.includes("overload") || lower.includes("d1_write_limit") || lower.includes("too much load")) {
    return { message, errorClass: "rate-limit", httpStatus };
  }
  if (httpStatus !== null) {
    // Conflict markers win over the transport class: an MVCC conflict surfaced
    // through an error response is still a conflict, not a backend failure.
    if (lower.includes("conflict") || lower.includes("sqlite_busy") || lower.includes("busy_snapshot")) {
      return { message, errorClass: "conflict", httpStatus };
    }
    return { message, errorClass: httpStatus >= 500 ? "backend" : "http", httpStatus };
  }
  if (lower.includes("fetch failed") || lower.includes("network") || lower.includes("econn") || lower.includes("socket")) {
    return { message, errorClass: "backend", httpStatus: null };
  }
  if (lower.includes("conflict") || lower.includes("sqlite_busy") || lower.includes("busy_snapshot")) {
    return { message, errorClass: "conflict", httpStatus };
  }
  return { message, errorClass: "unknown", httpStatus: null };
}

export function summarize(samplesMs: number[], wallMs: number): Summary {
  const s = [...samplesMs].sort((a, b) => a - b);
  const n = s.length;
  if (n === 0) {
    return { n: 0, min: 0, p50: 0, p90: 0, p95: 0, p99: 0, max: 0, mean: 0, stdev: 0, ops: 0 };
  }
  // Nearest-rank percentiles over zero-based indices.
  const pick = (p: number): number => s[Math.max(0, Math.ceil(p * n) - 1)] ?? 0;
  const mean = s.reduce((a, b) => a + b, 0) / n;
  const variance = s.reduce((a, b) => a + (b - mean) ** 2, 0) / n;
  return {
    n,
    min: s[0] ?? 0,
    p50: pick(0.5),
    p90: pick(0.9),
    p95: pick(0.95),
    p99: pick(0.99),
    max: s[n - 1] ?? 0,
    mean,
    stdev: Math.sqrt(variance),
    ops: n / (wallMs / 1000),
  };
}

export function median(values: number[]): number {
  if (values.length === 0) return 0;
  const s = [...values].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 === 1 ? (s[mid] ?? 0) : ((s[mid - 1] ?? 0) + (s[mid] ?? 0)) / 2;
}

export const fmt = (v: number): string => (v < 10 ? v.toFixed(2) : v < 100 ? v.toFixed(1) : Math.round(v).toString());

// ---- MVCC / transaction statistics -----------------------------------------
// Raw counters (attempted/committed/conflicts/retries) come from the Worker's
// server-side transaction loop, so retries and backoff are inside the measured
// latency. Rates are derived here so every reporter prints the same numbers.

export interface TxCounts {
  /** Operations that entered the retry loop. */
  attempted: number;
  /** Transaction attempts issued, including retries. */
  attemptsTotal: number;
  committed: number;
  successAfterRetry: number;
  failedAfterRetries: number;
  conflicts: number;
  retries: number;
  error: string | null;
}

export interface TxStats extends TxCounts {
  /** Conflicts as a share of attempts (attempts that hit a conflict). */
  conflictRate: number;
  /** Retries as a share of attempts. */
  retryRate: number;
  /** Average retries per operation that was attempted. */
  avgRetries: number;
}

export function txRates(counts: TxCounts): TxStats {
  const attempts = Math.max(1, counts.attemptsTotal);
  const attempted = Math.max(1, counts.attempted);
  return { ...counts, conflictRate: counts.conflicts / attempts, retryRate: counts.retries / attempts, avgRetries: counts.retries / attempted };
}

export function mergeTx(counts: readonly TxCounts[]): TxStats | null {
  if (counts.length === 0) return null;
  const zero: TxCounts = {
    attempted: 0,
    attemptsTotal: 0,
    committed: 0,
    successAfterRetry: 0,
    failedAfterRetries: 0,
    conflicts: 0,
    retries: 0,
    error: null,
  };
  const merged = counts.reduce<TxCounts>(
    (a, c) => ({
      attempted: a.attempted + c.attempted,
      attemptsTotal: a.attemptsTotal + c.attemptsTotal,
      committed: a.committed + c.committed,
      successAfterRetry: a.successAfterRetry + c.successAfterRetry,
      failedAfterRetries: a.failedAfterRetries + c.failedAfterRetries,
      conflicts: a.conflicts + c.conflicts,
      retries: a.retries + c.retries,
      error: a.error ?? c.error,
    }),
    zero,
  );
  return txRates(merged);
}
