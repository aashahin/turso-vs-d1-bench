// Concurrency-focused write workloads for the Turso Database MVCC benchmarks.
//
// `independent-writes` targets a different `concurrent_progress` row per op
// (row-level MVCC should let unrelated writers commit in parallel).
// `hot-row-write` targets the same `hot_counter` row from every op (genuine
// write/write conflict: MVCC cannot help, only retries can).
//
// Both are read-then-write transactions; the transaction form depends on the
// backend (D1 batch, BEGIN IMMEDIATE, or BEGIN CONCURRENT).
import { type Dims, type OpQuery } from "./common.ts";

export function writeOp(test: string, opIndex: number, tenant: number, dims: Dims): OpQuery {
  if (test === "independent-writes") {
    // Distinct student row per op index, uniform over the seeded range.
    // A hash modulo the row count collides before exhausting the row set.
    // Cycle without replacement; wrapping still requires enough seeded rows
    // for the requested concurrency and is documented in the benchmark notes.
    const student = 1 + (opIndex % dims.seedRows);
    return { test, method: "POST", params: { tenant, student } };
  }
  if (test === "hot-row-write") {
    // Single shared row (id = 1); no per-op parameter.
    return { test, method: "POST", params: { tenant } };
  }
  throw new Error(`writeOp: unknown test ${JSON.stringify(test)}`);
}
