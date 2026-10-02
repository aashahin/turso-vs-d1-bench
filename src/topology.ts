import { Database } from "bun:sqlite";
import {
  SCHEMA_SQL,
  SCHEMA_VERSION,
  schemaFingerprint,
  type SchemaEntry,
} from "../shared/schema.ts";
import { backendSpec } from "../shared/topology.ts";
import type { BenchArgs } from "./config.ts";
export interface TargetEvidence {
  sourceFingerprint?: string;
  backend: string;
  requestedTenant: number;
  tenantIsolation: string;
  databaseIdentity: string;
  bindingName: string;
  routingStrategy: string;
  engine: string;
  engineVersion: string | null;
  sqliteCompatibilityVersion?: string | null;
  engineEvidence: string;
  schemaVersion: number;
  schemaFingerprint: string;
  seedDims: Record<string, number>;
  seedTenant: number | null;
  rows: Record<string, number>;
  otherTenantUsers: number;
  region: string | null;
  deploymentVersion: string | null;
  gitCommit: string | null;
  sdkVersions: Record<string, string>;
  colo: string | null;
  poolConfiguration: Record<string, number>;
}
export async function expectedFingerprint(): Promise<string> {
  const db = new Database(":memory:");
  try {
    db.exec(SCHEMA_SQL);
    return await schemaFingerprint(
      db
        .query(
          "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'",
        )
        .all() as SchemaEntry[],
    );
  } finally {
    db.close();
  }
}
export function validateEvidence(
  targets: TargetEvidence[],
  args: Pick<
    BenchArgs,
    | "seedRows"
    | "tenants"
    | "studentsPerTenant"
    | "coursesPerTenant"
    | "lessonsPerCourse"
  >,
  fingerprint: string,
): void {
  const ids = new Map<string, Set<string>>();
  for (const t of targets) {
    const spec = backendSpec(t.backend);
    const isolated = spec.tenantIsolation === "database-per-tenant";
    const tenants = isolated ? 1 : args.tenants;
    const fail = (why: string) => {
      throw new Error(
        `TOPOLOGY_INVALID: ${t.backend}/tenant-${t.requestedTenant}: ${why}`,
      );
    };
    if (t.tenantIsolation !== spec.tenantIsolation || t.engine !== spec.engine)
      fail("engine/isolation mismatch");
    if (
      !t.databaseIdentity ||
      t.schemaVersion !== SCHEMA_VERSION ||
      t.schemaFingerprint !== fingerprint
    )
      fail("identity/schema/index mismatch");
    for (const [key, value] of Object.entries({
      seedRows: args.seedRows,
      tenants: args.tenants,
      studentsPerTenant: args.studentsPerTenant,
      coursesPerTenant: args.coursesPerTenant,
      lessonsPerCourse: args.lessonsPerCourse,
    }))
      if (t.seedDims[key] !== value) fail(`seed dimension ${key} mismatch`);
    if (
      isolated &&
      (t.seedTenant !== t.requestedTenant || t.otherTenantUsers !== 0)
    )
      fail("seed contains another tenant or wrong tenant");
    const expected = {
      kv: args.seedRows,
      concurrent_progress: args.seedRows,
      hot_counter: 1,
      users: tenants * args.studentsPerTenant,
      courses: tenants * args.coursesPerTenant,
      lessons: tenants * args.coursesPerTenant * args.lessonsPerCourse,
      quizzes: tenants * args.coursesPerTenant,
      quiz_questions: tenants * args.coursesPerTenant * 5,
    };
    for (const [key, value] of Object.entries(expected))
      if (t.rows[key] !== value)
        fail(`row count ${key} expected ${value}, observed ${t.rows[key]}`);
    // Enrollment writes can grow this table; at least the deterministic seed must exist.
    if (
      (t.rows.enrollments ?? 0) <
      tenants * args.studentsPerTenant * Math.min(2, args.coursesPerTenant)
    )
      fail("enrollment seed incomplete");
    const seen = ids.get(t.backend) ?? new Set<string>();
    if (isolated && seen.has(t.databaseIdentity))
      fail("two tenants reach the same physical database");
    seen.add(t.databaseIdentity);
    ids.set(t.backend, seen);
    if (
      !t.sourceFingerprint ||
      !t.deploymentVersion ||
      !t.gitCommit ||
      !t.sdkVersions?.serverless ||
      !t.sdkVersions?.drizzle
    )
      fail("deployment provenance missing");
  }
}
/** Different mutable starting states can change LMS query cost before load starts. */
export function validateComparableState(targets: TargetEvidence[]): void {
  const groups = new Map<string, string>();
  for (const target of targets) {
    const key = JSON.stringify([
      target.tenantIsolation,
      target.requestedTenant,
    ]);
    const counts = JSON.stringify(
      Object.entries(target.rows).sort(([a], [b]) => a.localeCompare(b)),
    );
    const previous = groups.get(key);
    if (previous !== undefined && previous !== counts)
      throw new Error(
        "TOPOLOGY_INVALID: selected backends have different starting row counts; reseed before comparing",
      );
    groups.set(key, counts);
  }
}
export async function preflight(
  args: BenchArgs,
  backends: string[],
  count: number,
): Promise<TargetEvidence[]> {
  if (!args.adminToken)
    throw new Error("Topology validation requires ADMIN_TOKEN");
  const targets: TargetEvidence[] = [];
  for (const backend of backends) {
    const n =
      backendSpec(backend).tenantIsolation === "database-per-tenant"
        ? count
        : 1;
    for (let tenant = 1; tenant <= n; tenant++) {
      const qs = new URLSearchParams({ backend, tenant: String(tenant) });
      const res = await fetch(`${args.workerUrl}/admin/validate-target?${qs}`, {
        headers: { authorization: `Bearer ${args.adminToken}` },
        signal: AbortSignal.timeout(args.validationTimeoutMs),
      });
      if (!res.ok)
        throw new Error(
          `TOPOLOGY_INVALID: validation failed for ${backend}/tenant-${tenant}: HTTP ${res.status}`,
        );
      const t = (await res.json()) as TargetEvidence;
      targets.push(t);
      if (backendSpec(backend).engine !== "d1")
        for (const [key, value] of Object.entries({
          sizePerTenant: args.pool.sizePerTenant,
          maxActivePools: args.pool.maxActivePools,
          idleTtlMs: args.pool.idleTtlMs,
          maxTotalConnections: args.pool.maxTotalConnections,
        }))
          if (t.poolConfiguration?.[key] !== value)
            throw new Error(
              `TOPOLOGY_INVALID: deployed pool ${key} differs from requested value; update TURSO_POOL_CONFIG and deploy`,
            );
    }
  }
  if (
    new Set(targets.map((t) => t.deploymentVersion)).size > 1 ||
    new Set(targets.map((t) => t.sourceFingerprint)).size > 1
  )
    throw new Error(
      "TOPOLOGY_INVALID: mixed Worker deployments during preflight",
    );
  validateEvidence(targets, args, await expectedFingerprint());
  validateComparableState(targets);
  return targets;
}
