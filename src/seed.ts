// One exact seed implementation runs on the Worker for D1/libSQL/Turso Database.
// Provision/configure targets first. Seeding is destructive only to benchmark tables.
import { parseArgs, getRaw } from "./config.ts";
import { backendSpec } from "../shared/topology.ts";
const args = parseArgs();
const backends = getRaw(
  "backends",
  "d1-shared,d1-eeur-shared,turso-shared,tursodb-shared",
).split(",");
const dims = {
  seedRows: Number(getRaw("rows", String(args.seedRows))),
  tenants: args.tenants,
  studentsPerTenant: args.studentsPerTenant,
  coursesPerTenant: args.coursesPerTenant,
  lessonsPerCourse: args.lessonsPerCourse,
};
if (!args.adminToken) throw new Error("ADMIN_TOKEN required to seed");
for (const backend of backends) {
  const count =
    backendSpec(backend).tenantIsolation === "database-per-tenant"
      ? Math.max(...args.tenantCounts)
      : 1;
  for (let tenant = 1; tenant <= count; tenant++) {
    console.log(
      `seeding ${backend}/tenant-${tenant} (outside benchmark timing)`,
    );
    const res = await fetch(
      `${args.workerUrl}/admin/seed-target?${new URLSearchParams({ backend, tenant: String(tenant) })}`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${args.adminToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(dims),
        signal: AbortSignal.timeout(300000),
      },
    );
    if (!res.ok)
      throw new Error(
        `seed failed: ${backend}/tenant-${tenant}: HTTP ${res.status}`,
      );
    const result = (await res.json()) as {
      ok: boolean;
      databaseIdentity: string;
      analyzed: boolean;
    };
    console.log({ backend, tenant, ...result });
  }
}
console.log(
  "Seed complete. The benchmark independently validates every selected database before measuring.",
);
