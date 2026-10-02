// Backend name normalization, engine classification, and client pools.
//
// Three engines are benchmarked: Cloudflare D1 (Cloudflare's SQLite), libSQL
// (`turso*` on the existing Turso Cloud database), and the new Turso Database
// engine (`tursodb*`, `turso db create --tursodb`, MVCC/concurrent writes).
//
// The SDK documents Connection as single-stream: concurrent calls on one
// Connection are serialized by the SDK's internal execLock. A single shared
// client would therefore be safe but would serialize all concurrent requests,
// so the reused backends keep a small pool (the SDK's own recommended
// pattern): concurrent requests run on different Connections, sequential
// reuses skip connect()/close(). connect() itself does no I/O (pure object
// alloc); close() sends an extra request whenever a baton is held, which is
// the overhead the fresh-vs-reused comparison isolates.
//
// Concurrency: an atomic `batch(stmts, mode)` transaction is one HTTP request
// on the connection's own stream, so `tursodb-concurrent` runs BEGIN
// CONCURRENT transactions through the same configurable pool model as
// `turso-reused` — bounded, not serialized, and the only reuse model Turso
// Cloud accepts at benchmark concurrency. (`transactionAsync()` gives every
// transaction its own server session and is rejected with "Database
// connections limit exceeded" once concurrency climbs.)
import {
  connect,
  type Connection as TursoClient,
} from "@tursodatabase/serverless";
import { EXPLICIT_BACKENDS, backendSpec } from "../../shared/topology.ts";
import { DEFAULT_POOL, type PoolConfig } from "../../shared/policy.ts";
import { TenantPoolManager } from "./pool.ts";
import { TX_WRITE_TESTS, WRITE_TESTS } from "../../shared/backend-tests.ts";

export type { TursoClient };

export type BackendName =
  | (typeof EXPLICIT_BACKENDS)[number]
  | "d1"
  | "d1-eeur"
  | "d1-rr"
  | "d1-eeur-rr"
  | "turso"
  | "turso-reused"
  | "tursodb-reused"
  | "tursodb-concurrent"
  | "d1-drizzle"
  | "d1-eeur-drizzle"
  | "turso-drizzle";

type EngineName = "d1" | "libsql" | "tursodb";

/** How a benchmark op reaches the database engine for a (backend, test) pair. */
type TxMode = "none" | "transaction" | "concurrent";

export interface Env {
  DB: D1Database;
  DB_EEUR: D1Database;
  TURSO_URL: string;
  TURSO_TOKEN: string;
  TURSODB_URL?: string;
  TURSODB_TOKEN?: string;
  ADMIN_TOKEN: string;
  [key: string]: unknown;
}

const BACKEND_ALIASES: Record<string, BackendName> = {
  d1: "d1",
  "d1-raw": "d1",
  "d1-eeur": "d1-eeur",
  "d1-eeur-raw": "d1-eeur",
  "d1-rr": "d1-rr",
  "d1-eeur-rr": "d1-eeur-rr",
  turso: "turso",
  "turso-raw": "turso",
  "turso-request-client": "turso",
  "turso-reused": "turso-reused",
  "turso-reused-client": "turso-reused",
  tursodb: "tursodb-reused",
  "tursodb-raw": "tursodb-reused",
  "tursodb-reused": "tursodb-reused",
  "tursodb-reused-client": "tursodb-reused",
  "tursodb-concurrent": "tursodb-concurrent",
  "tursodb-mvcc": "tursodb-concurrent",
  "d1-drizzle": "d1-drizzle",
  "d1-eeur-drizzle": "d1-eeur-drizzle",
  "turso-drizzle": "turso-drizzle",
};

export function normalizeBackend(raw: string): BackendName | null {
  return (EXPLICIT_BACKENDS as readonly string[]).includes(raw)
    ? (raw as BackendName)
    : (BACKEND_ALIASES[raw] ?? null);
}

export function isLibsqlBackend(b: BackendName): boolean {
  return backendSpec(b).engine === "libsql";
}

export function isTursoDbBackend(b: BackendName): boolean {
  return backendSpec(b).engine === "tursodb";
}

/** Any backend served by an external Turso (libSQL or Turso Database) database. */
export function isTursoBackend(b: BackendName): boolean {
  return isLibsqlBackend(b) || isTursoDbBackend(b);
}

export function usesSessions(b: BackendName): boolean {
  return b === "d1-rr" || b === "d1-eeur-rr";
}

export function engineOf(b: BackendName): EngineName {
  return backendSpec(b).engine;
}

/**
 * Transaction form used for a (backend, test) pair. Reported per scenario so
 * engine differences (libSQL vs Turso Database) and transaction-mode
 * differences (normal vs BEGIN CONCURRENT) are never conflated.
 */
export function txModeOf(b: BackendName, test: string): TxMode {
  if (WRITE_TESTS[test] !== true) return "none";
  if (b === "tursodb-concurrent") return "concurrent";
  if (TX_WRITE_TESTS[test] === true) return "transaction";
  return "none";
}

export function tursoDbConfigured(env: Env): boolean {
  return (
    typeof env.TURSODB_URL === "string" &&
    env.TURSODB_URL !== "" &&
    typeof env.TURSODB_TOKEN === "string" &&
    env.TURSODB_TOKEN !== ""
  );
}

function requireTursoDb(env: Env): { url: string; token: string } {
  if (!tursoDbConfigured(env)) {
    throw new Error(
      "TURSODB_URL/TURSODB_TOKEN are not configured on the Worker: create a Turso Database (turso db create --tursodb bench-tursodb), then set the TURSODB_URL var and TURSODB_TOKEN secret.",
    );
  }
  return { url: env.TURSODB_URL as string, token: env.TURSODB_TOKEN as string };
}

// Compatibility helpers use the same bounded manager. Live runOp supplies the
// common operation budget; these helpers retain the old 15s regression deadline.
export function withPooledTurso<T>(
  env: Env,
  fn: (conn: TursoClient) => Promise<T>,
  signal?: AbortSignal,
) {
  const config = {
    ...DEFAULT_POOL,
    operationTimeoutMs: 15000,
    queryTimeoutMs: 10000,
    checkoutTimeoutMs: 15000,
  };
  return tenantManager("legacy-libsql", config).use(
    "shared",
    JSON.stringify({ url: env.TURSO_URL, token: env.TURSO_TOKEN }),
    fn,
    signal,
  );
}
export function withPooledTursoDb<T>(
  env: Env,
  fn: (conn: TursoClient) => Promise<T>,
  signal?: AbortSignal,
) {
  const { url, token } = requireTursoDb(env);
  const config = {
    ...DEFAULT_POOL,
    operationTimeoutMs: 15000,
    queryTimeoutMs: 10000,
    checkoutTimeoutMs: 15000,
  };
  return tenantManager("legacy-tursodb", config).use(
    "shared",
    JSON.stringify({ url, token }),
    fn,
    signal,
  );
}
const TURSO_QUERY_TIMEOUT_MS = 5000;
export function freshTursoConn(
  env: Env,
  timeoutMs = TURSO_QUERY_TIMEOUT_MS,
): TursoClient {
  return connect({
    url: env.TURSO_URL,
    authToken: env.TURSO_TOKEN,
    defaultQueryTimeout: timeoutMs,
  });
}

export function freshTursoDbConn(env: Env): TursoClient {
  const { url, token } = requireTursoDb(env);
  return connect({
    url,
    authToken: token,
    defaultQueryTimeout: TURSO_QUERY_TIMEOUT_MS,
  });
}

// Per-tenant managers use one bounded cache per engine and configuration in each isolate.
// Config changes require a fresh deployment/isolate; do not accumulate caches per run.
const tenantManagers = new Map<
  string,
  { config: string; manager: TenantPoolManager<TursoClient> }
>();
export function tenantManager(
  engine: string,
  config: PoolConfig,
): TenantPoolManager<TursoClient> {
  const key = JSON.stringify(config);
  const old = tenantManagers.get(engine);
  if (old && old.config !== key)
    throw new Error(
      "pool config changed in a live isolate; deploy a fresh Worker for a new config",
    );
  if (old) return old.manager;
  const manager = new TenantPoolManager(config, (identity) => {
    const { url, token } = JSON.parse(identity) as {
      url: string;
      token: string;
    };
    return connect({
      url,
      authToken: token,
      defaultQueryTimeout: config.queryTimeoutMs,
    });
  });
  tenantManagers.set(engine, { config: key, manager });
  return manager;
}
export function poolConfig(env: Env, timeoutMs = 5000): PoolConfig {
  const raw =
    typeof env.TURSO_POOL_CONFIG === "string"
      ? (JSON.parse(env.TURSO_POOL_CONFIG) as Partial<PoolConfig>)
      : {};
  return {
    ...DEFAULT_POOL,
    ...raw,
    operationTimeoutMs: timeoutMs,
    queryTimeoutMs: Math.min(raw.queryTimeoutMs ?? timeoutMs, timeoutMs),
    checkoutTimeoutMs: Math.min(raw.checkoutTimeoutMs ?? timeoutMs, timeoutMs),
  };
}
