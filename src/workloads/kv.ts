// Trivial KV operations. Scan uses a random valid start ID per request
// (WHERE id >= ? ORDER BY id LIMIT ?) so it is not cache-friendly.
import { hash32, payloadFor, type Dims, type OpQuery } from "./common.ts";

export function kvOp(test: string, opIndex: number, tenant: number, dims: Dims): OpQuery {
  const h = (salt: number): number => hash32((opIndex ^ salt) >>> 0);
  if (test === "point-read") {
    return { test, method: "GET", params: { id: 1 + (h(0x11) % dims.seedRows), tenant } };
  }
  if (test === "scan") {
    const limit = dims.scanLimits[h(0x22) % dims.scanLimits.length] ?? 100;
    const maxStart = Math.max(1, dims.seedRows - limit + 1);
    return { test, method: "GET", params: { startId: 1 + (h(0x33) % maxStart), limit, tenant } };
  }
  if (test === "insert") {
    return { test, method: "POST", params: { tenant, payload: payloadFor(opIndex) } };
  }
  // update: rewrite the payload of an existing kv row (same byte size).
  return { test: "update", method: "POST", params: { id: 1 + (h(0x44) % dims.seedRows), tenant, payload: payloadFor(opIndex) } };
}
