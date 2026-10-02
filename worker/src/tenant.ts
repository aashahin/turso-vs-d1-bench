// Explicit topology. Missing isolated mappings always fail closed.
import {
  isLibsqlBackend,
  isTursoDbBackend,
  type BackendName,
  type Env,
} from "./backends.ts";
import {
  backendSpec,
  parseTenantMappings,
  type TenantMapping,
} from "../../shared/topology.ts";
export type TenantDbKind = "isolated" | "shared";
export function resolveTenantDb(
  env: Env,
  backend: BackendName,
  tenant: number,
): {
  db: D1Database | null;
  kind: TenantDbKind;
  binding: string;
  mapping?: TenantMapping;
} {
  const spec = backendSpec(backend);
  const isolated = spec.tenantIsolation === "database-per-tenant";
  if (isLibsqlBackend(backend) || isTursoDbBackend(backend)) {
    if (!isolated)
      return {
        db: null,
        kind: "shared",
        binding: `${spec.engine}-shared-schema`,
      };
    const mappings = cachedMappings(env, spec.engine);
    const mapping = mappings[String(tenant)];
    if (!mapping)
      throw new Error("TOPOLOGY_INVALID: missing tenant database mapping");
    return {
      db: null,
      kind: "isolated",
      binding: `${spec.engine}-tenant-${tenant}`,
      mapping,
    };
  }
  const eeur = backend.includes("eeur");
  const binding = isolated
    ? `${eeur ? "DB_EEUR" : "DB"}_TENANT_${tenant}`
    : eeur
      ? "DB_EEUR"
      : "DB";
  const db = env[binding] as D1Database | undefined;
  if (!db || typeof db.prepare !== "function")
    throw new Error("TOPOLOGY_INVALID: missing tenant binding");
  return { db, kind: isolated ? "isolated" : "shared", binding };
}
export function allSeedTargets(
  env: Env,
): { name: string; db: D1Database; tenant?: number }[] {
  const targets: { name: string; db: D1Database; tenant?: number }[] = [];
  for (const [key, value] of Object.entries(env)) {
    const match = key.match(/^DB(?:_EEUR)?_TENANT_(\d+)$/);
    if (key === "DB" || key === "DB_EEUR" || match) {
      const db = value as D1Database;
      if (db && typeof db.prepare === "function")
        targets.push({
          name: key,
          db,
          tenant: match ? Number(match[1]) : undefined,
        });
    }
  }
  return targets;
}

const mappingCache = new Map<
  string,
  { raw: unknown; mappings: ReturnType<typeof parseTenantMappings> }
>();
function cachedMappings(env: Env, engine: "d1" | "libsql" | "tursodb") {
  const prefix = engine === "libsql" ? "TURSO_TENANTS" : "TURSODB_TENANTS";
  const single = env[prefix];
  const chunks = Object.entries(env)
    .filter(([key]) => new RegExp(`^${prefix}_\\d+$`).test(key))
    .sort(
      ([a], [b]) =>
        Number(a.slice(prefix.length + 1)) - Number(b.slice(prefix.length + 1)),
    );
  const signature =
    typeof single === "string" && single !== ""
      ? single
      : JSON.stringify(chunks);
  const cached = mappingCache.get(engine);
  if (cached?.raw === signature) return cached.mappings;
  let raw: unknown = single;
  if ((single === undefined || single === "") && chunks.length) {
    const merged: Record<string, TenantMapping> = {};
    for (let index = 0; index < chunks.length; index++) {
      const [key, contents] = chunks[index]!;
      if (key !== `${prefix}_${index}`)
        throw new Error("TOPOLOGY_INVALID: missing tenant manifest chunk");
      const part = parseTenantMappings(contents, engine);
      for (const [tenant, mapping] of Object.entries(part)) {
        if (merged[tenant])
          throw new Error(
            "TOPOLOGY_INVALID: duplicate tenant across manifest chunks",
          );
        merged[tenant] = mapping;
      }
    }
    raw = merged;
  }
  const mappings = parseTenantMappings(raw, engine);
  mappingCache.set(engine, { raw: signature, mappings });
  return mappings;
}
