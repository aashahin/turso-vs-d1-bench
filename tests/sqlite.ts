import { Database, type SQLQueryBindings } from "bun:sqlite";
export function localD1(sqlite = new Database(":memory:")) {
  interface Statement extends D1PreparedStatement {
    sql: string;
    args: SQLQueryBindings[];
  }
  const execute = (s: Statement): D1Result => {
    if (/^\s*(SELECT|PRAGMA)/i.test(s.sql)) {
      const rows = sqlite.query(s.sql).all(...s.args) as Record<
        string,
        unknown
      >[];
      return {
        results: rows,
        meta: { last_row_id: null, rows_read: rows.length, rows_written: 0 },
      };
    }
    const change = sqlite.query(s.sql).run(...s.args);
    return {
      results: [],
      meta: {
        last_row_id: Number(change.lastInsertRowid),
        rows_read: 0,
        rows_written: change.changes,
      },
    };
  };
  const prepare = (sql: string): Statement => ({
    sql,
    args: [],
    bind(...args: unknown[]) {
      this.args = args as SQLQueryBindings[];
      return this;
    },
    async first<T = Record<string, unknown>>() {
      return (execute(this).results[0] ?? null) as T | null;
    },
    async all<T>() {
      return execute(this) as D1Result<T>;
    },
    async run() {
      return execute(this);
    },
  });
  const db: D1Database = {
    prepare,
    exec: async (sql) => sqlite.exec(sql),
    batch: async (statements) =>
      sqlite.transaction(() =>
        statements.map((s) => execute(s as Statement)),
      )(),
    withSession: () => db,
  };
  return { db, sqlite };
}
