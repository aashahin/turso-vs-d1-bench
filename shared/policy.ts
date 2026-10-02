export class BenchError extends Error {
  evidence?: Record<string, unknown>;
  httpStatus?: number | null;
  constructor(
    public readonly errorClass: string,
    message: string,
    public readonly timeoutClass: string | null = null,
  ) {
    super(message);
  }
}
/** Do not expose SDK/server error text: it may contain URLs, tokens or SQL values. */
export function safeError(error: unknown): {
  error: string;
  errorClass: string;
  timeoutClass: string | null;
} {
  if (error instanceof BenchError)
    return {
      error: error.message,
      errorClass: error.errorClass,
      timeoutClass: error.timeoutClass,
    };
  const text =
    error instanceof Error
      ? `${error.name} ${error.message}`.toLowerCase()
      : "";
  const httpStatus =
    Number(text.match(/http[^0-9]*([45][0-9]{2})/)?.[1]) || null;
  const errorClass = /timed out|timeout|aborted/.test(text)
    ? "timeout"
    : /constraint/.test(text)
      ? "constraint"
      : /conflict|busy|locked/.test(text)
        ? "conflict"
        : /429|rate limit|overload|connections limit/.test(text)
          ? "rate-limit"
          : httpStatus !== null
            ? httpStatus >= 500
              ? "backend"
              : "http"
            : /fetch|network|socket|connection/.test(text)
              ? "connection"
              : "application";
  return {
    error: `backend ${errorClass} error`,
    errorClass,
    timeoutClass: errorClass === "timeout" ? "query" : null,
  };
}
export async function bounded<T>(
  task: () => Promise<T>,
  ms: number,
  phase: string,
  signal?: AbortSignal,
): Promise<T> {
  if (signal?.aborted)
    throw signal.reason ?? new DOMException("request aborted", "AbortError");
  let timer!: ReturnType<typeof setTimeout>;
  let abort!: () => void;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new BenchError("timeout", `${phase} timeout`, phase)),
      ms,
    );
    abort = () =>
      reject(
        signal?.reason ?? new DOMException("request aborted", "AbortError"),
      );
    signal?.addEventListener("abort", abort, { once: true });
  });
  try {
    return await Promise.race([task(), deadline]);
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
  }
}
export interface PoolConfig {
  sizePerTenant: number;
  maxActivePools: number;
  idleTtlMs: number;
  maxTotalConnections: number;
  checkoutTimeoutMs: number;
  operationTimeoutMs: number;
  queryTimeoutMs: number;
}
export const DEFAULT_POOL: PoolConfig = {
  sizePerTenant: 8,
  maxActivePools: 100,
  idleTtlMs: 60000,
  maxTotalConnections: 128,
  checkoutTimeoutMs: 5000,
  operationTimeoutMs: 5000,
  queryTimeoutMs: 5000,
};
export function validatePool(config: PoolConfig): void {
  for (const [k, v] of Object.entries(config))
    if (!Number.isInteger(v) || v < 1)
      throw new BenchError("application", `invalid pool option ${k}`);
  if (
    config.checkoutTimeoutMs > config.operationTimeoutMs ||
    config.queryTimeoutMs > config.operationTimeoutMs
  )
    throw new BenchError(
      "application",
      "phase timeout exceeds operation timeout",
    );
}
