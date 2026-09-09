export const BACKENDS = ["d1", "d1-eeur", "turso", "d1-rr", "d1-eeur-rr"] as const;
export const TESTS = ["point-read", "scan-100", "insert"] as const;
export const MODES = ["edge", "direct"] as const;
export type Backend = (typeof BACKENDS)[number];
export type Test = (typeof TESTS)[number];
export type Mode = (typeof MODES)[number];

export interface BenchArgs {
  workerUrl: string;
  tursoUrl: string;
  tursoToken: string;
  adminToken: string;
  backends: Backend[];
  tests: Test[];
  modes: Mode[];
  iterations: number;
  concurrencies: number[];
  warmup: number;
  seedRows: number;
  out: string;
}

export function parseArgs(): BenchArgs {
  // Env wins over CLI flags so a published repo stays runnable via environment
  // without editing code; empty env values fall through to flag/default.
  const g = (k: string, d: string) => {
    const env = process.env[k.toUpperCase().replace(/-/g, "_")];
    if (env !== undefined && env !== "") return env;
    const hit = process.argv.find((a) => a.startsWith(`--${k}=`));
    return hit ? hit.slice(k.length + 3) : d;
  };

  const oneOf = <T extends string>(k: string, d: string, allowed: readonly T[]): T[] => {
    const vals = g(k, d)
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s !== "");
    if (vals.length === 0) throw new Error(`--${k} must be a non-empty list of: ${allowed.join(", ")}`);
    for (const v of vals) {
      if (!(allowed as readonly string[]).includes(v)) {
        throw new Error(`--${k}: unknown value ${JSON.stringify(v)} (allowed: ${allowed.join(", ")})`);
      }
    }
    return vals as T[];
  };

  const int = (k: string, d: string, min: number, max?: number): number => {
    const raw = g(k, d);
    const n = Number(raw);
    const range = max === undefined ? `>= ${min}` : `${min}..${max}`;
    if (!Number.isInteger(n) || n < min || (max !== undefined && n > max)) {
      throw new Error(`--${k} must be an integer ${range} (got ${JSON.stringify(raw)})`);
    }
    return n;
  };

  const intList = (k: string, d: string, min: number): number[] => {
    const items = g(k, d).split(",");
    const out: number[] = [];
    for (const item of items) {
      const raw = item.trim();
      const n = Number(raw);
      if (raw === "" || !Number.isInteger(n) || n < min) {
        throw new Error(`--${k} must be a comma list of integers >= ${min} (got ${JSON.stringify(item)})`);
      }
      out.push(n);
    }
    return out;
  };

  return {
    workerUrl: g("worker-url", "http://127.0.0.1:8787").replace(/\/$/, ""),
    tursoUrl: g("turso-url", ""),
    tursoToken: g("turso-token", ""),
    adminToken: g("admin-token", ""),
    backends: oneOf("backends", "d1,d1-eeur,turso,d1-rr,d1-eeur-rr", BACKENDS),
    tests: oneOf("tests", "point-read,scan-100,insert", TESTS),
    modes: oneOf("modes", "edge", MODES),
    iterations: int("iterations", "100", 1),
    concurrencies: intList("concurrency", "1,10,50", 1),
    warmup: int("warmup", "10", 0),
    seedRows: int("seed-rows", "10000", 1),
    out: g("out", "results.json"),
  };
}
