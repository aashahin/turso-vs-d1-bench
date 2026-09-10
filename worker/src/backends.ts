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
// CONCURRENT transactions through the same 8-connection pool as
// `turso-reused` — bounded, not serialized, and the only reuse model Turso
// Cloud accepts at benchmark concurrency. (`transactionAsync()` gives every
// transaction its own server session and is rejected with "Database
// connections limit exceeded" once concurrency climbs.)
import { connect, type Connection as TursoClient } from "@tursodatabase/serverless";
import { TX_WRITE_TESTS, WRITE_TESTS } from "../../shared/backend-tests.ts";

export type { TursoClient };

export type BackendName =
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
  "d1": "d1",
  "d1-raw": "d1",
  "d1-eeur": "d1-eeur",
  "d1-eeur-raw": "d1-eeur",
  "d1-rr": "d1-rr",
  "d1-eeur-rr": "d1-eeur-rr",
  "turso": "turso",
  "turso-raw": "turso",
  "turso-request-client": "turso",
  "turso-reused": "turso-reused",
  "turso-reused-client": "turso-reused",
  "tursodb": "tursodb-reused",
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
  return BACKEND_ALIASES[raw] ?? null;
}

export function isLibsqlBackend(b: BackendName): boolean {
  return b === "turso" || b === "turso-reused" || b === "turso-drizzle";
}

export function isTursoDbBackend(b: BackendName): boolean {
  return b === "tursodb-reused" || b === "tursodb-concurrent";
}

/** Any backend served by an external Turso (libSQL or Turso Database) database. */
export function isTursoBackend(b: BackendName): boolean {
  return isLibsqlBackend(b) || isTursoDbBackend(b);
}

export function usesSessions(b: BackendName): boolean {
  return b === "d1-rr" || b === "d1-eeur-rr";
}

export function engineOf(b: BackendName): EngineName {
  if (isTursoDbBackend(b)) return "tursodb";
  if (isLibsqlBackend(b)) return "libsql";
  return "d1";
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
  return typeof env.TURSODB_URL === "string" && env.TURSODB_URL !== "" && typeof env.TURSODB_TOKEN === "string" && env.TURSODB_TOKEN !== "";
}

function requireTursoDb(env: Env): { url: string; token: string } {
  if (!tursoDbConfigured(env)) {
    throw new Error(
      "TURSODB_URL/TURSODB_TOKEN are not configured on the Worker: create a Turso Database (turso db create --tursodb bench-tursodb), then set the TURSODB_URL var and TURSODB_TOKEN secret.",
    );
  }
  return { url: env.TURSODB_URL as string, token: env.TURSODB_TOKEN as string };
}

// ---- Client pools -----------------------------------------------------------
// Pool of long-lived Connections. The per-slot queue is belt-and-braces: the
// SDK already serializes concurrent calls per Connection via its execLock, but
// holding one Connection per in-flight request preserves parallelism instead
// of funneling everything through a single lock.
//
// Two failure modes matter on the edge, and both are handled here:
//  * a hung fetch (aborted client, stalled socket) must not own a slot for the
//    life of the isolate — a watchdog recycles the connection and frees the
//    slot (POOL_SLOT_DEADLINE_MS);
//  * work for a client that already timed out is pure waste and saturates the
//    database — checkout (and each phase before it) aborts with the request
//    signal instead of running queries nobody will read.

const POOL_SIZE = 8;
/** Max time one pooled connection may be held by a single request. */
const POOL_SLOT_DEADLINE_MS = 15000;
/**
 * Per-call SDK timeout (`defaultQueryTimeout` -> `AbortSignal.timeout` on the
 * SDK's own fetch). Without it a stalled Turso call never settles, the Worker
 * handler never returns, and the Workers runtime cancels the request as hung
 * (HTTP 500 "error code: 1101"). Bounded above the client's own op timeout
 * (5s by default) so the server side always settles and frees the pool slot.
 */
const TURSO_QUERY_TIMEOUT_MS = 10000;

interface Slot {
  conn: TursoClient;
  queue: Promise<void>;
}

interface ClientPool {
  slots: Slot[];
  key: string;
  cursor: number;
}

function newPool(): ClientPool {
  return { slots: [], key: "", cursor: 0 };
}

const libsqlPool = newPool();
const tursoDbPool = newPool();

function poolFor(pool: ClientPool, url: string, token: string): ClientPool {
  const key = `${url}\0${token}`;
  if (pool.key !== key) {
    pool.slots = Array.from({ length: POOL_SIZE }, () => ({ conn: connect({ url, authToken: token, defaultQueryTimeout: TURSO_QUERY_TIMEOUT_MS }), queue: Promise.resolve() }));
    pool.key = key;
  }
  return pool;
}

/** Awaits the slot queue but gives up as soon as the client is gone. */
async function waitForSlot(prev: Promise<void>, signal: AbortSignal | undefined): Promise<void> {
  if (signal === undefined) {
    await prev;
    return;
  }
  const abortError = (): DOMException => new DOMException("bench request aborted by the client", "AbortError");
  if (signal.aborted) throw abortError();
  let onAbort!: () => void;
  const abortSignal = new Promise<never>((_, reject) => {
    onAbort = () => reject(abortError());
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    await Promise.race([prev, abortSignal]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

async function withPool<T>(
  pool: ClientPool,
  url: string,
  token: string,
  fn: (conn: TursoClient) => Promise<T>,
  signal?: AbortSignal,
): Promise<{ out: T; checkoutMs: number }> {
  const t0 = performance.now();
  poolFor(pool, url, token);
  const idx = (pool.cursor = (pool.cursor + 1) % POOL_SIZE);
  const slot = pool.slots[idx] as Slot;
  const prev = slot.queue;
  let release!: () => void;
  const gate = new Promise<void>((res) => {
    release = res;
  });
  slot.queue = prev.then(() => gate);
  let released = false;
  const releaseOnce = (): void => {
    if (released) return;
    released = true;
    release();
  };
  try {
    await waitForSlot(prev, signal);
  } catch (e) {
    // Abandoned checkout: free the gate we just installed. Without this the
    // slot stays gated forever (nobody will ever resolve it), every later
    // request on it hangs, and the Workers runtime cancels hung requests with
    // an HTTP 500.
    releaseOnce();
    throw e;
  }
  const checkoutMs = performance.now() - t0;
  const watchdog = setTimeout(() => {
    if (!released) {
      // Drop the possibly-hung connection and let queued requests through on a
      // fresh one; the orphaned call can no longer block the pool.
      slot.conn = connect({ url, authToken: token, defaultQueryTimeout: TURSO_QUERY_TIMEOUT_MS });
      releaseOnce();
    }
  }, POOL_SLOT_DEADLINE_MS);
  let deadlineTimer!: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_, reject) => {
    deadlineTimer = setTimeout(() => reject(new Error(`pooled Turso call exceeded ${POOL_SLOT_DEADLINE_MS}ms; connection recycled`)), POOL_SLOT_DEADLINE_MS);
  });
  try {
    const out = await Promise.race([fn(slot.conn), deadline]);
    return { out, checkoutMs };
  } finally {
    clearTimeout(watchdog);
    clearTimeout(deadlineTimer);
    releaseOnce();
  }
}

/** libSQL (`turso-reused`): pooled Clients on the existing Turso Cloud database. */
export function withPooledTurso<T>(env: Env, fn: (conn: TursoClient) => Promise<T>, signal?: AbortSignal): Promise<{ out: T; checkoutMs: number }> {
  return withPool(libsqlPool, env.TURSO_URL, env.TURSO_TOKEN, fn, signal);
}

/** Turso Database (`tursodb-reused`): pooled Clients on the tursodb database. */
export function withPooledTursoDb<T>(env: Env, fn: (conn: TursoClient) => Promise<T>, signal?: AbortSignal): Promise<{ out: T; checkoutMs: number }> {
  const { url, token } = requireTursoDb(env);
  return withPool(tursoDbPool, url, token, fn, signal);
}

export function freshTursoConn(env: Env): TursoClient {
  return connect({ url: env.TURSO_URL, authToken: env.TURSO_TOKEN, defaultQueryTimeout: TURSO_QUERY_TIMEOUT_MS });
}

export function freshTursoDbConn(env: Env): TursoClient {
  const { url, token } = requireTursoDb(env);
  return connect({ url, authToken: token, defaultQueryTimeout: TURSO_QUERY_TIMEOUT_MS });
}
