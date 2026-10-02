import { hash32 } from "./common.ts";
export type Distribution = "uniform" | "zipf" | "hotset";
export interface TrafficOptions {
  distribution: Distribution;
  hotTenantPercent: number;
  hotTrafficPercent: number;
  zipfExponent: number;
  seed: number;
}
export const DEFAULT_TRAFFIC: TrafficOptions = {
  distribution: "uniform",
  hotTenantPercent: 10,
  hotTrafficPercent: 80,
  zipfExponent: 1.1,
  seed: 42,
};
export function tenantSelector(
  count: number,
  o: TrafficOptions,
): (index: number) => number {
  if (
    !Number.isInteger(count) ||
    count < 1 ||
    o.hotTenantPercent <= 0 ||
    o.hotTenantPercent >= 100 ||
    o.hotTrafficPercent <= 0 ||
    o.hotTrafficPercent >= 100 ||
    !Number.isFinite(o.zipfExponent) ||
    o.zipfExponent <= 0
  )
    throw new Error("invalid tenant distribution");
  const cdf: number[] = [];
  let sum = 0;
  if (o.distribution === "zipf") {
    for (let t = 1; t <= count; t++) {
      sum += 1 / t ** o.zipfExponent;
      cdf.push(sum);
    }
    for (let i = 0; i < cdf.length; i++) cdf[i] = cdf[i]! / sum;
  }
  const hot = Math.min(
    count,
    Math.max(1, Math.ceil((count * o.hotTenantPercent) / 100)),
  );
  return (index) => {
    const u = hash32(index ^ o.seed ^ 0x74e4a7) / 2 ** 32;
    if (o.distribution === "uniform") return 1 + Math.floor(u * count);
    if (o.distribution === "hotset") {
      const v = hash32(index ^ o.seed ^ 0x8871) / 2 ** 32;
      return hot === count || u < o.hotTrafficPercent / 100
        ? 1 + Math.floor(v * hot)
        : 1 + hot + Math.floor(v * (count - hot));
    }
    let lo = 0,
      hi = cdf.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (u <= cdf[mid]!) hi = mid;
      else lo = mid + 1;
    }
    return lo + 1;
  };
}
export function concentration(
  counts: Record<string, number>,
  tenantCount: number,
) {
  const values = Object.values(counts).sort((a, b) => b - a);
  const total = values.reduce((a, b) => a + b, 0);
  const hot = Math.max(1, Math.ceil(tenantCount * 0.1));
  return {
    counts,
    total,
    tenantsObserved: values.length,
    top10PercentShare: total
      ? values.slice(0, hot).reduce((a, b) => a + b, 0) / total
      : 0,
    maxTenantShare: total ? (values[0] ?? 0) / total : 0,
  };
}
