export type Isolation = "shared-schema" | "database-per-tenant";
export type Engine = "d1" | "libsql" | "tursodb";
export interface BackendSpec {
  engine: Engine;
  tenantIsolation: Isolation;
  accessLayer: "raw" | "drizzle";
  execution: string;
  routingStrategy: string;
}
export const EXPLICIT_BACKENDS = [
  "d1-shared",
  "d1-eeur-shared",
  "d1-per-tenant",
  "d1-eeur-per-tenant",
  "turso-shared",
  "turso-per-tenant",
  "tursodb-shared",
  "tursodb-per-tenant",
  "d1-per-tenant-drizzle",
  "d1-eeur-per-tenant-drizzle",
  "turso-per-tenant-drizzle",
  "tursodb-per-tenant-concurrent",
  "d1-per-tenant-rr",
  "d1-eeur-per-tenant-rr",
] as const;
export function backendSpec(backend: string): BackendSpec {
  const engine = backend.startsWith("tursodb")
    ? "tursodb"
    : backend.startsWith("turso")
      ? "libsql"
      : "d1";
  const per = backend.includes("per-tenant");
  const accessLayer = backend.endsWith("drizzle") ? "drizzle" : "raw";
  const execution =
    engine === "d1"
      ? `d1${backend.includes("eeur") ? "-eeur" : ""}${accessLayer === "drizzle" ? "-drizzle" : backend.endsWith("rr") ? "-rr" : ""}`
      : engine === "tursodb"
        ? backend.endsWith("concurrent")
          ? "tursodb-concurrent"
          : "tursodb-reused"
        : accessLayer === "drizzle"
          ? "turso-drizzle"
          : backend === "turso" || backend === "turso-raw"
            ? "turso"
            : "turso-reused";
  return {
    engine,
    tenantIsolation: per ? "database-per-tenant" : "shared-schema",
    accessLayer,
    execution,
    routingStrategy: per
      ? engine === "d1"
        ? "explicit-worker-binding"
        : "tenant-database-manifest"
      : "shared-schema-tenant-id",
  };
}
export interface TenantMapping {
  databaseId: string;
  url: string;
  token: string;
  engine: Engine;
  region?: string;
}
export function parseTenantMappings(
  raw: unknown,
  engine: Engine,
): Record<string, TenantMapping> {
  let data: unknown;
  try {
    data = typeof raw === "string" ? JSON.parse(raw) : raw;
  } catch {
    throw new Error("TOPOLOGY_INVALID: malformed tenant manifest");
  }
  if (!data || typeof data !== "object" || Array.isArray(data))
    throw new Error("TOPOLOGY_INVALID: missing tenant manifest");
  const result: Record<string, TenantMapping> = {};
  const urls = new Set<string>();
  const ids = new Set<string>();
  for (const [tenant, value] of Object.entries(data)) {
    const v = value as Partial<TenantMapping>;
    if (
      !/^\d+$/.test(tenant) ||
      Number(tenant) < 1 ||
      !v ||
      typeof v.url !== "string" ||
      typeof v.token !== "string" ||
      !v.token ||
      typeof v.databaseId !== "string" ||
      !v.databaseId ||
      !/^[-a-zA-Z0-9_:]{1,128}$/.test(v.databaseId) ||
      v.engine !== engine
    )
      throw new Error("TOPOLOGY_INVALID: incomplete tenant mapping");
    let url: URL;
    try {
      url = new URL(v.url.replace(/^(libsql|turso):\/\//, "https://"));
    } catch {
      throw new Error("TOPOLOGY_INVALID: invalid database URL");
    }
    if (
      url.protocol !== "https:" ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    )
      throw new Error("TOPOLOGY_INVALID: unsafe database URL");
    const origin = url.origin;
    if (urls.has(origin) || ids.has(v.databaseId))
      throw new Error("TOPOLOGY_INVALID: tenants share a database");
    urls.add(origin);
    ids.add(v.databaseId);
    result[tenant] = {
      databaseId: v.databaseId,
      url: origin,
      token: v.token,
      engine,
      region: v.region,
    };
  }
  return result;
}
