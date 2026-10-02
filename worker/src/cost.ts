export interface QueryCounters {
  queries: number;
  rowsRead: number | null;
  rowsWritten: number | null;
}
/** D1 all/run expose row counters; first() hides them, so select the first all() row. */
export function meteredD1(db: D1Database, counters: QueryCounters): D1Database {
  const record = (result: D1Result) => {
    counters.queries++;
    if (typeof result.meta?.rows_read === "number")
      counters.rowsRead = (counters.rowsRead ?? 0) + result.meta.rows_read;
    if (typeof result.meta?.rows_written === "number")
      counters.rowsWritten =
        (counters.rowsWritten ?? 0) + result.meta.rows_written;
  };
  const wrap = (stmt: D1PreparedStatement): D1PreparedStatement => {
    const wrapped: D1PreparedStatement = {
      bind: (...p) => wrap(stmt.bind(...p)),
      first: async <T = Record<string, unknown>>() => {
        if (typeof stmt.all !== "function") {
          counters.queries++;
          return stmt.first<T>();
        }
        const result = await stmt.all<T>();
        record(result as D1Result);
        return result.results[0] ?? null;
      },
      all: async <T = Record<string, unknown>>() => {
        const result = await stmt.all<T>();
        record(result as D1Result);
        return result;
      },
      run: async () => {
        const result = await stmt.run();
        record(result);
        return result;
      },
    };
    unwrap.set(wrapped, stmt);
    return wrapped;
  };
  return {
    prepare: (sql) => wrap(db.prepare(sql)),
    exec: (sql) => db.exec(sql),
    batch: async (statements) => {
      const results = await db.batch(statements.map((s) => unwrap.get(s) ?? s));
      for (const result of results) record(result);
      return results;
    },
    withSession: () => meteredD1(db.withSession(), counters),
  };
  // Bindings need original statement handles for batch, not wrapper objects.
}
const unwrap = new WeakMap<D1PreparedStatement, D1PreparedStatement>();

import type { Connection } from "@tursodatabase/serverless";
/** Statement attempts, including resolution and retry attempts; not vendor billing units. */
export function meteredTurso(
  conn: Connection,
  counters: QueryCounters,
): Connection {
  return new Proxy(conn, {
    get(target, key) {
      const member = Reflect.get(target, key, target) as unknown;
      if (typeof member !== "function") return member;
      if (!["get", "all", "run", "batch"].includes(String(key)))
        return member.bind(target);
      return async (...args: unknown[]) => {
        counters.queries +=
          key === "batch" && Array.isArray(args[0]) ? args[0].length : 1;
        const result: unknown = await Reflect.apply(member, target, args);
        if (
          key === "run" &&
          result &&
          typeof (result as { changes?: number }).changes === "number"
        )
          counters.rowsWritten =
            (counters.rowsWritten ?? 0) +
            (result as { changes: number }).changes;
        if (key === "batch" && Array.isArray(result))
          for (const row of result) {
            if (typeof row?.rowsAffected === "number")
              counters.rowsWritten =
                (counters.rowsWritten ?? 0) + row.rowsAffected;
          }
        return result;
      };
    },
  });
}
