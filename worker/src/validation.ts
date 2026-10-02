import { connect, type Connection } from "@tursodatabase/serverless";
import {
  SCHEMA_VERSION,
  schemaFingerprint,
  type SchemaEntry,
} from "../../shared/schema.ts";
import { backendSpec } from "../../shared/topology.ts";
import { resolveTenantDb } from "./tenant.ts";
import type { BackendName, Env } from "./backends.ts";
export interface SqlReader {
  all(sql: string, ...params: unknown[]): Promise<unknown[]>;
  get(sql: string, ...params: unknown[]): Promise<unknown>;
}
export function d1Reader(db: D1Database): SqlReader {
  return {
    all: async (s, ...p) =>
      (
        await db
          .prepare(s)
          .bind(...p)
          .all()
      ).results,
    get: (s, ...p) =>
      db
        .prepare(s)
        .bind(...p)
        .first(),
  };
}
export async function readEvidence(reader: SqlReader, tenant: number | null, engine?: string) {
  const identity = (await reader.get(
    "SELECT * FROM bench_identity WHERE singleton=1",
  )) as {
    database_id: string;
    engine: string;
    schema_version: number;
    seed_dims: string;
    tenant: number | null;
  } | null;
  if (!identity || identity.schema_version !== SCHEMA_VERSION)
    throw new Error(
      "TOPOLOGY_INVALID: database is not seeded with the current schema version",
    );
  const entries = (await reader.all(
    "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'",
  )) as SchemaEntry[];
  const rows: Record<string, number> = {};
  for (const table of [
    "kv",
    "concurrent_progress",
    "hot_counter",
    "users",
    "courses",
    "lessons",
    "enrollments",
    "quizzes",
    "quiz_questions",
    "lesson_progress",
    "quiz_attempts",
    "quiz_answers",
    "orders",
    "bench_writes",
  ]) {
    const scoped =
      tenant !== null &&
      [
        "users",
        "courses",
        "lessons",
        "enrollments",
        "quizzes",
        "quiz_questions",
        "lesson_progress",
        "quiz_attempts",
        "quiz_answers",
        "orders",
      ].includes(table);
    const row = (await reader.get(
      `SELECT COUNT(*) AS n FROM ${table}${scoped ? " WHERE tenant_id = ?" : ""}`,
      ...(scoped ? [tenant] : []),
    )) as { n: number };
    rows[table] = Number(row.n);
  }
  const others =
    tenant === null
      ? 0
      : Number(
          (
            (await reader.get(
              "SELECT COUNT(*) AS n FROM users WHERE tenant_id != ?",
              tenant,
            )) as { n: number }
          ).n,
        );
  let compatibilityVersion: string | null = null;
  try {
    compatibilityVersion = (
      (await reader.get("SELECT sqlite_version() AS version")) as {
        version: string;
      }
    ).version;
  } catch (error) {
    const text = error instanceof Error ? error.message : String(error);
    if (
      engine !== "d1" ||
      !text.includes("not authorized to use function") ||
      !text.includes("sqlite_version")
    )
      throw error;
  }
  return {
    databaseIdentity: identity.database_id,
    engine: identity.engine,
    schemaVersion: identity.schema_version,
    seedDims: JSON.parse(identity.seed_dims) as Record<string, number>,
    seedTenant: identity.tenant,
    schemaFingerprint: await schemaFingerprint(entries),
    schema: entries,
    rows,
    otherTenantUsers: others,
    engineVersion: null,
    sqliteCompatibilityVersion: compatibilityVersion,
    engineEvidence: "seed-provisioning-attestation",
  };
}
export function externalConfig(env: Env, backend: BackendName, tenant: number) {
  const route = resolveTenantDb(env, backend, tenant);
  const spec = backendSpec(backend);
  const url =
    route.mapping?.url ??
    (spec.engine === "libsql" ? env.TURSO_URL : env.TURSODB_URL);
  const token =
    route.mapping?.token ??
    (spec.engine === "libsql" ? env.TURSO_TOKEN : env.TURSODB_TOKEN);
  if (!url || !token)
    throw new Error("TOPOLOGY_INVALID: backend credentials missing");
  return { route, url, token };
}
export async function validateTarget(
  env: Env,
  backend: BackendName,
  tenant: number,
) {
  const route = resolveTenantDb(env, backend, tenant);
  const spec = backendSpec(backend);
  let conn: Connection | undefined;
  try {
    const reader = route.db
      ? d1Reader(route.db)
      : (conn = connect({
          ...externalConfig(env, backend, tenant),
          authToken: externalConfig(env, backend, tenant).token,
          defaultQueryTimeout: 5000,
        }));
    const evidence = await readEvidence(
      reader,
      spec.tenantIsolation === "database-per-tenant" ? tenant : null,
      spec.engine,
    );
    if (
      evidence.engine !== spec.engine ||
      (route.mapping && evidence.databaseIdentity !== route.mapping.databaseId)
    )
      throw new Error(
        "TOPOLOGY_INVALID: database identity/engine does not match routing",
      );
    const d1Region = route.db
      ? (await route.db.prepare("SELECT 1 AS probe").all()).meta
          ?.served_by_region
      : null;
    const configuredRegion = route.mapping?.region;
    const region =
      configuredRegion && configuredRegion !== "unknown"
        ? configuredRegion
        : typeof d1Region === "string"
          ? d1Region
          : null;
    return {
      ...evidence,
      backend,
      requestedTenant: tenant,
      tenantIsolation: spec.tenantIsolation,
      bindingName: route.binding,
      region,
      regionSource:
        configuredRegion && configuredRegion !== "unknown"
          ? "manifest-attestation"
          : region
            ? "D1-query-serving-region"
            : null,
      routingStrategy: spec.routingStrategy,
    };
  } finally {
    await conn?.close();
  }
}
// Minimal adapter reuses the exact deterministic D1 seeder for every engine.
export function tursoSeedAdapter(conn: Connection): D1Database {
  interface Statement extends D1PreparedStatement {
    text: string;
    params: unknown[];
  }
  const prepare = (text: string): Statement => ({
    text,
    params: [],
    bind(...params: unknown[]) {
      this.params = params;
      return this;
    },
    async first<T>() {
      return (await conn.get(text, ...this.params)) as T | null;
    },
    async all<T>() {
      return {
        results: (await conn.all(text, ...this.params)) as T[],
        meta: { last_row_id: null, rows_read: 0, rows_written: 0 },
      };
    },
    async run() {
      const r = await conn.run(text, ...this.params);
      return {
        results: [],
        meta: {
          last_row_id: r.lastInsertRowid,
          rows_read: 0,
          rows_written: r.changes,
        },
      };
    },
  });
  const db: D1Database = {
    prepare,
    exec: async (sql) => {
      for (const s of sql
        .split(";")
        .map((s) => s.trim())
        .filter(Boolean))
        await conn.exec(s);
    },
    batch: async (stmts) => {
      const groups = new Map<string, Statement[]>();
      for (const statement of stmts) {
        const s = statement as Statement;
        (
          groups.get(s.text) ?? (groups.set(s.text, []), groups.get(s.text)!)
        ).push(s);
      }
      for (const [sql, group] of groups) {
        const insert = sql.match(/^(INSERT INTO .+ VALUES )(\(.+\))$/);
        if (insert)
          await conn.run(
            insert[1] + group.map(() => insert[2]).join(","),
            ...group.flatMap((s) => s.params),
          );
        else
          await conn.batch(
            group.map((s) => ({ sql: s.text, args: s.params })),
            "immediate",
          );
      }
      return [];
    },
    withSession: () => db,
  };
  return db;
}
