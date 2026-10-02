import { describe, expect, test } from "bun:test";
import { SCHEMA_SQL, SCHEMA_VERSION, normalizeSql } from "../shared/schema.ts";
import { parseTenantMappings } from "../shared/topology.ts";
import { resolveTenantDb } from "../worker/src/tenant.ts";
import { seedKv, seedLms } from "../worker/src/seed.ts";
import { readEvidence, d1Reader } from "../worker/src/validation.ts";
import {
  validateEvidence,
  validateComparableState,
  expectedFingerprint,
  type TargetEvidence,
} from "../src/topology.ts";
import { runOp } from "../worker/src/queries.ts";
import { localD1 } from "./sqlite.ts";
import { dims } from "./helpers.ts";
const seedDims = {
  seedRows: dims.seedRows,
  tenants: dims.tenants,
  studentsPerTenant: dims.studentsPerTenant,
  coursesPerTenant: dims.coursesPerTenant,
  lessonsPerCourse: dims.lessonsPerCourse,
};
async function seeded(tenant: number, id = `db-${tenant}`) {
  const { db, sqlite } = localD1();
  sqlite.exec(SCHEMA_SQL);
  await seedKv(db, dims.seedRows);
  await seedLms(db, dims, tenant);
  await db
    .prepare("INSERT INTO bench_identity VALUES (1,?,?,?,?,?)")
    .bind(id, "d1", SCHEMA_VERSION, JSON.stringify(seedDims), tenant)
    .run();
  return { db, sqlite };
}
async function evidence(tenant: number): Promise<TargetEvidence> {
  const local = await seeded(tenant);
  try {
    return {
      ...(await readEvidence(d1Reader(local.db), tenant)),
      backend: "d1-per-tenant",
      requestedTenant: tenant,
      tenantIsolation: "database-per-tenant",
      bindingName: `DB_TENANT_${tenant}`,
      routingStrategy: "explicit-worker-binding",
      region: null,
      deploymentVersion: "test-deploy",
      sourceFingerprint: "test-source",
      gitCommit: "test-commit",
      sdkVersions: { serverless: "1.4.0", drizzle: "0.44.7" },
      colo: "TEST",
      poolConfiguration: {},
    };
  } finally {
    local.sqlite.close();
  }
}
describe("physical tenant topology", () => {
  test("isolated D1 A and B route to different databases; missing binding fails closed", () => {
    const a = localD1(),
      b = localD1();
    try {
      const env = {
        DB: a.db,
        DB_EEUR: a.db,
        DB_TENANT_1: a.db,
        DB_TENANT_2: b.db,
      } as never;
      expect(resolveTenantDb(env, "d1-per-tenant", 1).db).not.toBe(
        resolveTenantDb(env, "d1-per-tenant", 2).db,
      );
      expect(() => resolveTenantDb(env, "d1-per-tenant", 3)).toThrow(
        "TOPOLOGY_INVALID",
      );
      expect(resolveTenantDb(env, "d1-shared", 3).db).toBe(a.db);
    } finally {
      a.sqlite.close();
      b.sqlite.close();
    }
  });
  test("Turso tenant manifest routes to distinct URLs without leaking secrets on invalid mappings", () => {
    const manifest = {
      "1": {
        databaseId: "a",
        url: "https://a.invalid",
        token: "secret-a",
        engine: "libsql",
      },
      "2": {
        databaseId: "b",
        url: "https://b.invalid",
        token: "secret-b",
        engine: "libsql",
      },
    };
    const env = { TURSO_TENANTS: JSON.stringify(manifest) } as never;
    expect(
      resolveTenantDb(env, "turso-per-tenant", 2).mapping?.databaseId,
    ).toBe("b");
    expect(() => resolveTenantDb(env, "turso-per-tenant", 3)).toThrow(
      "missing",
    );
    expect(() =>
      parseTenantMappings(
        { ...manifest, "2": { ...manifest["2"], url: "https://a.invalid" } },
        "libsql",
      ),
    ).toThrow("share");
    expect(() => parseTenantMappings(manifest, "tursodb")).toThrow(
      "incomplete",
    );
  });
  test("preflight rejects shared physical identities despite different binding labels", async () => {
    const targets = await Promise.all([evidence(1), evidence(2)]);
    targets[1]!.databaseIdentity = targets[0]!.databaseIdentity;
    expect(() =>
      validateEvidence(targets, seedDims, targets[0]!.schemaFingerprint),
    ).toThrow("same physical database");
  });
  test.each(["schema", "rows", "engine", "tenant", "provenance"] as const)(
    "preflight rejects invalid %s before measurement",
    async (kind) => {
      const target = await evidence(1);
      if (kind === "schema") target.schemaFingerprint = "wrong";
      if (kind === "rows") target.rows.lessons = 0;
      if (kind === "engine") target.engine = "libsql";
      if (kind === "tenant") target.otherTenantUsers = 1;
      if (kind === "provenance") target.deploymentVersion = null;
      const fingerprint = await expectedFingerprint();
      expect(() => validateEvidence([target], seedDims, fingerprint)).toThrow(
        "TOPOLOGY_INVALID",
      );
    },
  );
});
let awaitFingerprint = "";
test("canonical schema SQL and Worker DDL stay identical, indexes and constraints included", async () => {
  const file = (await Bun.file("schema.sql").text())
    .split("\n")
    .filter((l) => !l.trimStart().startsWith("--"))
    .join("\n");
  expect(normalizeSql(file)).toBe(normalizeSql(SCHEMA_SQL));
  awaitFingerprint = await expectedFingerprint();
  const target = await evidence(2);
  expect(target.schemaFingerprint).toBe(awaitFingerprint);
  expect(() =>
    validateEvidence([target], seedDims, awaitFingerprint),
  ).not.toThrow();
});
describe("real SQLite seed and LMS writes", () => {
  test("isolated seed contains only the requested tenant and fixed timestamps; C=1 has no duplicate enrollment", async () => {
    const a = localD1();
    try {
      await seedLms(a.db, { ...dims, coursesPerTenant: 1 }, 2);
      expect(
        await a.db
          .prepare("SELECT COUNT(*) AS n FROM users WHERE tenant_id != 2")
          .first<{ n: number }>(),
      ).toEqual({ n: 0 });
      expect(
        await a.db
          .prepare("SELECT COUNT(*) AS n FROM enrollments")
          .first<{ n: number }>(),
      ).toEqual({ n: dims.studentsPerTenant });
      expect(
        await a.db
          .prepare("SELECT DISTINCT created_at FROM enrollments")
          .first<{ created_at: string }>(),
      ).toEqual({ created_at: "2026-01-01T00:00:00.000Z" });
    } finally {
      a.sqlite.close();
    }
  });
  test("progress computes a percent and exam commits independent answer/progress writes", async () => {
    const a = await seeded(1);
    try {
      const env = { DB: a.db, DB_EEUR: a.db } as never;
      const p = {
        id: 1,
        startId: 1,
        limit: 10,
        tenant: 1,
        student: 2,
        course: 1,
        lesson: 1,
        quiz: 1,
        question: 1,
        attempt: 0,
        position: 1,
        payload: "test",
        answer: "B",
        completed: 1,
        retries: 0,
      };
      await runOp("d1-per-tenant", a.db, env, "exam-submit", p, "isolated");
      expect(
        await a.db
          .prepare(
            "SELECT progress_pct FROM enrollments WHERE student_id=2 AND course_id=1",
          )
          .first<{ progress_pct: number }>(),
      ).toEqual({ progress_pct: 10 });
      await runOp(
        "d1-per-tenant",
        a.db,
        env,
        "update-progress",
        { ...p, student: 3, course: 3, lesson: 21 },
        "isolated",
      );
      expect(
        await a.db
          .prepare(
            "SELECT progress_pct FROM enrollments WHERE student_id=3 AND course_id=3",
          )
          .first<{ progress_pct: number }>(),
      ).toEqual({ progress_pct: 10 });
      expect(
        await a.db
          .prepare("SELECT COUNT(*) AS n FROM quiz_answers")
          .first<{ n: number }>(),
      ).toEqual({ n: 1 });
    } finally {
      a.sqlite.close();
    }
  });
});

test("different starting mutable table counts invalidate a live engine comparison", async () => {
  const first = await evidence(1);
  const second = {
    ...first,
    backend: "turso-per-tenant",
    engine: "libsql",
    rows: { ...first.rows, quiz_answers: (first.rows.quiz_answers ?? 0) + 1 },
  };
  expect(() => validateComparableState([first, second])).toThrow(
    "starting row counts",
  );
});

test("chunked secret manifests preserve isolation and reject gaps/duplicates", () => {
  const first = {
    "1": {
      databaseId: "chunk-a",
      url: "https://chunk-a.invalid",
      token: "test-a",
      engine: "libsql",
    },
  };
  const second = {
    "2": {
      databaseId: "chunk-b",
      url: "https://chunk-b.invalid",
      token: "test-b",
      engine: "libsql",
    },
  };
  const env = {
    TURSO_TENANTS_0: JSON.stringify(first),
    TURSO_TENANTS_1: JSON.stringify(second),
  } as never;
  expect(resolveTenantDb(env, "turso-per-tenant", 2).mapping?.databaseId).toBe(
    "chunk-b",
  );
  expect(() =>
    resolveTenantDb(
      {
        TURSO_TENANTS_0: JSON.stringify(first),
        TURSO_TENANTS_2: JSON.stringify(second),
      } as never,
      "turso-per-tenant",
      2,
    ),
  ).toThrow("missing tenant manifest chunk");
  expect(() =>
    resolveTenantDb(
      {
        TURSO_TENANTS_0: JSON.stringify(first),
        TURSO_TENANTS_1: JSON.stringify(first),
      } as never,
      "turso-per-tenant",
      1,
    ),
  ).toThrow("duplicate tenant");
});
