/** Deployed Workers freeze clocks between I/O events; zero is not CPU cost. */
export function workerTimingEvidence(
  extra: Record<string, unknown>,
): Record<string, unknown> {
  return {
    ...extra,
    timingQuality: {
      clock: "cloudflare-io-gated",
      synchronousPhaseTimers: "unavailable",
    },
    pool:
      extra.pool && typeof extra.pool === "object"
        ? { ...extra.pool, connectionCreateMs: null }
        : undefined,
    poolMetrics:
      extra.poolMetrics && typeof extra.poolMetrics === "object"
        ? { ...extra.poolMetrics, connectionCreateMs: null }
        : undefined,
  };
}
