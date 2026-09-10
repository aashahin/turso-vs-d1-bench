// Tenant routing for the distributed multi-tenant benchmark.
// Manhali architecture: tenant N -> isolated D1 database N. The Worker looks
// for explicit per-tenant bindings (DB_TENANT_<n> for the WEUR family,
// DB_EEUR_TENANT_<n> for EEUR). When the binding is absent it falls back to
// tenant_id scoping inside the shared primary and reports tenantDb:"shared"
// so results can never be mistaken for isolated-DB scaling numbers.
// Turso always scopes via tenant_id (single Turso database): "shared".
import { isLibsqlBackend, isTursoDbBackend, type BackendName, type Env } from "./backends.ts";

export type TenantDbKind = "isolated" | "shared";

export function resolveTenantDb(env: Env, backend: BackendName, tenant: number): { db: D1Database | null; kind: TenantDbKind; binding: string } {
  if (isTursoDbBackend(backend)) return { db: null, kind: "shared", binding: "tursodb-shared-schema" };
  if (isLibsqlBackend(backend)) return { db: null, kind: "shared", binding: "turso-shared-schema" };
  const eeur = backend === "d1-eeur" || backend === "d1-eeur-rr" || backend === "d1-eeur-drizzle";
  const specific = eeur ? `DB_EEUR_TENANT_${tenant}` : `DB_TENANT_${tenant}`;
  const candidate = env[specific] as D1Database | undefined;
  if (candidate && typeof candidate.prepare === "function") {
    return { db: candidate, kind: "isolated", binding: specific };
  }
  return { db: eeur ? env.DB_EEUR : env.DB, kind: "shared", binding: eeur ? "DB_EEUR" : "DB" };
}

/** All D1 databases that exist for seeding (primaries + tenant bindings). */
export function allSeedTargets(env: Env): { name: string; db: D1Database }[] {
  const out: { name: string; db: D1Database }[] = [
    { name: "weur", db: env.DB },
    { name: "eeur", db: env.DB_EEUR },
  ];
  for (const [key, value] of Object.entries(env)) {
    if (/^DB(_EEUR)?_TENANT_\d+$/.test(key) && value !== null && typeof value === "object") {
      const db = value as D1Database;
      if (typeof db.prepare === "function") out.push({ name: key, db });
    }
  }
  return out;
}
