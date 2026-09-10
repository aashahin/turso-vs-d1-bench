// Minimal Cloudflare Workers + D1 typings so `bun run typecheck` passes
// without pulling @cloudflare/workers-types. If that package is added later,
// delete this file and extend `Env` from the generated `Env` instead.

interface D1ResultMeta {
  last_row_id: number | string | null;
  rows_read: number;
  rows_written: number;
  served_by_primary?: boolean | null;
  served_by_region?: string | null;
  [key: string]: unknown;
}

interface D1Result<T = Record<string, unknown>> {
  results: T[];
  meta: D1ResultMeta;
}

interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T = Record<string, unknown>>(): Promise<T | null>;
  all<T = Record<string, unknown>>(): Promise<D1Result<T>>;
  run(): Promise<D1Result>;
}

interface D1Database {
  prepare(query: string): D1PreparedStatement;
  batch(statements: D1PreparedStatement[]): Promise<D1Result[]>;
  exec(query: string): Promise<unknown>;
  withSession(): D1Database;
}
