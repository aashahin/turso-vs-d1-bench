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

export type ErrorClass =
  | "timeout"
  | "http"
  | "rate-limit"
  | "backend"
  | "conflict"
  | "unknown"
  | "connection"
  | "constraint"
  | "application";

export interface ClassifiedError {
  message: string;
  errorClass: ErrorClass;
  httpStatus: number | null;
  timeoutClass?: string | null;
  evidence?: Record<string, unknown>;
}

export function classifyError(e: unknown): ClassifiedError {
  const structured = e as {
    errorClass?: ErrorClass;
    timeoutClass?: string | null;
    message?: string;
    evidence?: Record<string, unknown>;
    httpStatus?: number | null;
  };
  if (
    structured?.errorClass &&
    [
      "timeout",
      "http",
      "rate-limit",
      "backend",
      "conflict",
      "unknown",
      "connection",
      "constraint",
      "application",
    ].includes(structured.errorClass)
  )
    return {
      message: structured.message ?? "backend error",
      errorClass: structured.errorClass,
      httpStatus: structured.httpStatus ?? null,
      timeoutClass: structured.timeoutClass,
      evidence: structured.evidence,
    };
  const rawMessage = e instanceof Error ? e.message : String(e);
  const message = rawMessage.includes("TOPOLOGY_INVALID")
    ? "TOPOLOGY_INVALID"
    : "operation failed (raw error text withheld)";
  const lower = rawMessage.toLowerCase();
  // MVCC conflicts surface as SQLITE_BUSY / SQLITE_BUSY_SNAPSHOT or a
  // "Write-write conflict" message; they are neither transport nor app errors.
  const looksLikeConflict =
    lower.includes("conflict") ||
    lower.includes("sqlite_busy") ||
    lower.includes("busy_snapshot");
  if (e instanceof DOMException && e.name === "TimeoutError") {
    return {
      message,
      errorClass: "timeout",
      httpStatus: null,
      timeoutClass: "runner-operation",
    };
  }
  if (
    lower.includes("aborted") ||
    lower.includes("timeout") ||
    lower.includes("timed out")
  ) {
    return { message, errorClass: "timeout", httpStatus: null };
  }
  const http = rawMessage.match(/HTTP[^0-9]*(\d{3})/i);
  const httpStatus = http?.[1] ? Number(http[1]) : null;
  if (
    httpStatus === 429 ||
    lower.includes("rate limit") ||
    lower.includes("too many requests") ||
    lower.includes("overload") ||
    lower.includes("d1_write_limit") ||
    lower.includes("too much load")
  ) {
    return { message, errorClass: "rate-limit", httpStatus };
  }
  if (lower.includes("constraint"))
    return { message, errorClass: "constraint", httpStatus };
  if (httpStatus !== null) {
    // Conflict markers win over the transport class: an MVCC conflict surfaced
    // through an error response is still a conflict, not a backend failure.
    if (looksLikeConflict)
      return { message, errorClass: "conflict", httpStatus };
    return {
      message,
      errorClass: httpStatus >= 500 ? "backend" : "http",
      httpStatus,
    };
  }
  if (
    lower.includes("fetch failed") ||
    lower.includes("network") ||
    lower.includes("econn") ||
    lower.includes("socket")
  ) {
    return { message, errorClass: "backend", httpStatus: null };
  }
  if (looksLikeConflict) return { message, errorClass: "conflict", httpStatus };
  return { message, errorClass: "unknown", httpStatus: null };
}

export function summarize(samplesMs: number[], wallMs: number): Summary {
  const s = [...samplesMs].sort((a, b) => a - b);
  const n = s.length;
  if (n === 0) {
    return {
      n: 0,
      min: 0,
      p50: 0,
      p90: 0,
      p95: 0,
      p99: 0,
      max: 0,
      mean: 0,
      stdev: 0,
      ops: 0,
    };
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
    ops: wallMs > 0 ? n / (wallMs / 1000) : 0,
  };
}

export function median(values: number[]): number {
  if (values.length === 0) return 0;
  const s = [...values].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 === 1
    ? (s[mid] ?? 0)
    : ((s[mid - 1] ?? 0) + (s[mid] ?? 0)) / 2;
}

export const fmt = (v: number | null): string =>
  v === null
    ? "-"
    : v < 10
      ? v.toFixed(2)
      : v < 100
        ? v.toFixed(1)
        : Math.round(v).toString();

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
  return {
    ...counts,
    conflictRate: counts.conflicts / attempts,
    retryRate: counts.retries / attempts,
    avgRetries: counts.retries / attempted,
  };
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

export function dispersion(values: number[], seed = 42) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const q = (p: number) =>
    sorted[Math.max(0, Math.ceil(p * sorted.length) - 1)]!;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  let state = seed >>> 0;
  const random = () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 2 ** 32;
  };
  const boot =
    values.length >= 5
      ? Array.from({ length: 2000 }, () =>
          median(
            Array.from(
              { length: values.length },
              () => values[Math.floor(random() * values.length)]!,
            ),
          ),
        ).sort((a, b) => a - b)
      : [];
  return {
    n: values.length,
    median: median(values),
    min: sorted[0]!,
    max: sorted.at(-1)!,
    iqr: q(0.75) - q(0.25),
    coefficientOfVariation: mean
      ? Math.sqrt(
          values.reduce((a, b) => a + (b - mean) ** 2, 0) / values.length,
        ) / mean
      : 0,
    medianBootstrap95: boot.length ? [boot[49]!, boot[1949]!] : null,
  };
}
