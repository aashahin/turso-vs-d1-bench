// Deterministic per-op parameter derivation. Every ID is a pure function of
// the op index, so all backends replay the identical access pattern for a
// scenario: same rows, same payload sizes, same tenant spread. Concurrency
// cannot skew the pattern because there is no shared RNG state.

export function hash32(n: number): number {
  let h = (n >>> 0) ^ 0x9e3779b9;
  h = Math.imul(h ^ (h >>> 16), 0x21f0aaad);
  h = Math.imul(h ^ (h >>> 15), 0x735a2d97);
  h ^= h >>> 15;
  return h >>> 0;
}

/** Deterministic fixed-size payload (hex chars), identical for every backend. */
export function payloadFor(opIndex: number, size = 72): string {
  let out = "";
  let k = opIndex;
  while (out.length < size) {
    out += hash32(k).toString(16).padStart(8, "0");
    k = (k + 0x9e3779b9) >>> 0;
  }
  return out.slice(0, size);
}

export interface Dims {
  seedRows: number;
  scanLimits: number[];
  tenants: number;
  studentsPerTenant: number;
  coursesPerTenant: number;
  lessonsPerCourse: number;
}

export interface OpQuery {
  test: string;
  params: Record<string, number | string>;
  method: "GET" | "POST";
}

export function tenantFor(opIndex: number, tenantMode: string, tenantCount: number): number {
  if (tenantMode !== "distributed") return 1;
  return 1 + (hash32(opIndex ^ 0x74e4a7) % Math.max(1, tenantCount));
}
