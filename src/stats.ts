export interface Summary {
  n: number;
  min: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
  mean: number;
  stdev: number;
  ops: number; // ops/sec over the measured wall time
}

export function summarize(samplesMs: number[], wallMs: number): Summary {
  const s = [...samplesMs].sort((a, b) => a - b);
  const n = s.length;
  if (n === 0) throw new Error("summarize: no samples");
  // Nearest-rank percentiles over zero-based indices.
  const pick = (p: number) => s[Math.max(0, Math.ceil(p * n) - 1)];
  const mean = s.reduce((a, b) => a + b, 0) / Math.max(1, n);
  const variance = s.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(1, n);
  return {
    n,
    min: s[0] ?? 0,
    p50: pick(0.5),
    p95: pick(0.95),
    p99: pick(0.99),
    max: s[n - 1] ?? 0,
    mean,
    stdev: Math.sqrt(variance),
    ops: n / (wallMs / 1000),
  };
}

const fmt = (v: number) => (v < 10 ? v.toFixed(2) : v < 100 ? v.toFixed(1) : Math.round(v).toString());

export function printTable(rows: { label: string; e2e: Summary; db: Summary | null; replicaRate: number | null }[]) {
  const head = ["benchmark", "n", "min", "p50", "p95", "p99", "max", "mean", "ops/s", "db p50", "rep%"];
  console.log(head.join("\t"));
  for (const r of rows) {
    console.log(
      [
        r.label,
        String(r.e2e.n),
        fmt(r.e2e.min),
        fmt(r.e2e.p50),
        fmt(r.e2e.p95),
        fmt(r.e2e.p99),
        fmt(r.e2e.max),
        fmt(r.e2e.mean),
        Math.round(r.e2e.ops).toString(),
        r.db ? fmt(r.db.p50) : "-",
        r.replicaRate === null ? "-" : `${Math.round(r.replicaRate * 100)}`,
      ].join("\t"),
    );
  }
}
