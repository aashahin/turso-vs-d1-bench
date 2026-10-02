import { workerTimingEvidence } from "./timing.ts";
import { BUILD_INFO } from "../../shared/build-info.ts";
import { connect } from "@tursodatabase/serverless";
import { backendSpec } from "../../shared/topology.ts";
import { BenchError, bounded, safeError } from "../../shared/policy.ts";
import { SCHEMA_VERSION } from "../../shared/schema.ts";
import { poolConfig } from "./backends.ts";
import {
  validateTarget,
  externalConfig,
  tursoSeedAdapter,
} from "./validation.ts";
import { isReadTest, WRITE_TESTS } from "../../shared/backend-tests.ts";
import {
  engineOf,
  freshTursoConn,
  freshTursoDbConn,
  normalizeBackend,
  tursoDbConfigured,
  usesSessions,
  type Env,
  type TursoClient,
} from "./backends.ts";
import { runOp, type OpParams } from "./queries.ts";
import { allSeedTargets, resolveTenantDb } from "./tenant.ts";
import {
  KV_DDL,
  LMS_DDL,
  analyze,
  seedKv,
  seedLms,
  type SeedDims,
} from "./seed.ts";
import { createLifecycleTracker } from "./lifecycle.ts";

const trackRequest = createLifecycleTracker();

const MAX_SEED_ROWS = 50000;
const MAX_SCAN_LIMIT = 1000;
const MAX_SEED_TENANTS = 200;
const MAX_STUDENTS_PER_TENANT = 2000;
const MAX_COURSES_PER_TENANT = 50;
const MAX_LESSONS_PER_COURSE = 100;

const json = (data: unknown, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
      pragma: "no-cache",
    },
  });

const unauthorized = () =>
  json({ error: "unauthorized: valid Bearer ADMIN_TOKEN required" }, 401);
const bad = (error: string) => json({ error }, 400);
const methodNotAllowed = (allow: string) =>
  new Response(JSON.stringify({ error: `method not allowed, use ${allow}` }), {
    status: 405,
    headers: { "content-type": "application/json", allow },
  });

// Fail closed: no token configured => nothing is authorized.
function isAdmin(req: Request, env: Env): boolean {
  if (!env.ADMIN_TOKEN) return false;
  const h = req.headers.get("authorization") ?? "";
  return h === `Bearer ${env.ADMIN_TOKEN}`;
}

function numParam(
  url: URL,
  name: string,
  dflt: number,
  min: number,
  max: number,
): number | null {
  const raw = url.searchParams.get(name);
  if (raw === null) return dflt;
  const n = Number(raw);
  return Number.isInteger(n) && n >= min && n <= max ? n : null;
}

// Shared insert-body boundary so both backends store the same value.
// Missing/empty body => random payload; malformed JSON or non-string => 400.
async function parseJsonBody(
  req: Request,
): Promise<{ body: Record<string, unknown> } | Response> {
  const text = await req.text();
  if (text === "") return { body: {} };
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return bad("invalid JSON body");
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
    return bad("body must be a JSON object");
  return { body: parsed as Record<string, unknown> };
}

function strField(
  body: Record<string, unknown>,
  name: string,
): string | undefined {
  const v = body[name];
  return typeof v === "string" ? v : undefined;
}

export default {
  async fetch(
    req: Request,
    env: Env,
    ctx?: { waitUntil(promise: Promise<unknown>): void },
  ): Promise<Response> {
    // Count every route, including health/meta/errors: any can warm this isolate.
    // Capture before awaiting so concurrent completions cannot swap identities.
    const handler0 = performance.now();
    const lifecycle = trackRequest();
    let requestBackend: string | null = null;
    let requestTenant: number | undefined;
    try {
      const url = new URL(req.url);

      if (url.pathname === "/health")
        return json({ ok: true, ts: new Date().toISOString() });

      if (url.pathname === "/bench/identity") {
        const hostOf = (u: string | undefined): string | null => {
          if (!u) return null;
          try {
            return new URL(u).host;
          } catch {
            return null;
          }
        };
        return json({
          tursoHost: hostOf(env.TURSO_URL),
          tursodbHost: hostOf(env.TURSODB_URL),
          tursodbConfigured: tursoDbConfigured(env),
        });
      }

      if (
        url.pathname === "/admin/validate-target" ||
        url.pathname === "/admin/seed-target"
      ) {
        if (!isAdmin(req, env)) return unauthorized();
        const backend = normalizeBackend(url.searchParams.get("backend") ?? "");
        const tenant = numParam(url, "tenant", 1, 1, 100000);
        if (!backend || tenant === null)
          return bad("invalid validation target");
        if (url.pathname === "/admin/validate-target") {
          if (req.method !== "GET") return methodNotAllowed("GET");
          const target = await bounded(
            () => validateTarget(env, backend, tenant),
            60000,
            "topology-validation",
            req.signal,
          );
          return json({
            ...target,
            deploymentVersion:
              (env.CF_VERSION_METADATA as { id?: string } | undefined)?.id ??
              BUILD_INFO.buildId,
            deploymentVersionSource: env.CF_VERSION_METADATA
              ? "cloudflare-version-binding"
              : "embedded-build-id",
            gitCommit: BUILD_INFO.gitCommit,
            gitDirty: BUILD_INFO.gitDirty,
            sourceFingerprint: BUILD_INFO.sourceFingerprint,
            sdkVersions: BUILD_INFO.sdkVersions,
            colo:
              (req as Request & { cf?: { colo?: string } }).cf?.colo ?? null,
            poolConfiguration: poolConfig(
              env,
              numParam(url, "timeoutMs", 5000, 100, 60000) ?? 5000,
            ),
          });
        }
        if (req.method !== "POST") return methodNotAllowed("POST");
        const parsed = await parseJsonBody(req);
        if (parsed instanceof Response) return parsed;
        const dims = parsed.body as unknown as SeedDims & { seedRows: number };
        if (
          ![
            dims.tenants,
            dims.studentsPerTenant,
            dims.coursesPerTenant,
            dims.lessonsPerCourse,
            dims.seedRows,
          ].every((v) => Number.isInteger(v) && v > 0) ||
          dims.tenants > 100000 ||
          dims.studentsPerTenant > 2000 ||
          dims.coursesPerTenant > 50 ||
          dims.lessonsPerCourse > 100 ||
          dims.seedRows > 50000
        )
          return bad("invalid seed dimensions");
        const route = resolveTenantDb(env, backend, tenant);
        const spec = backendSpec(backend);
        let conn: ReturnType<typeof connect> | undefined;
        try {
          let db = route.db;
          if (!db) {
            const c = externalConfig(env, backend, tenant);
            conn = connect({
              url: c.url,
              authToken: c.token,
              defaultQueryTimeout: 10000,
            });
            db = tursoSeedAdapter(conn);
          }
          await seedKv(db, dims.seedRows);
          await seedLms(
            db,
            dims,
            spec.tenantIsolation === "database-per-tenant" ? tenant : undefined,
          );
          const existing = await db
            .prepare("SELECT database_id FROM bench_identity WHERE singleton=1")
            .first<{ database_id: string }>();
          const identity =
            existing?.database_id ??
            route.mapping?.databaseId ??
            crypto.randomUUID();
          if (route.mapping && identity !== route.mapping.databaseId)
            throw new Error(
              "TOPOLOGY_INVALID: refusing to change physical database identity",
            );
          await db
            .prepare(
              "INSERT OR REPLACE INTO bench_identity (singleton,database_id,engine,schema_version,seed_dims,tenant) VALUES (1,?,?,?,?,?)",
            )
            .bind(
              identity,
              spec.engine,
              SCHEMA_VERSION,
              JSON.stringify(dims),
              spec.tenantIsolation === "database-per-tenant" ? tenant : null,
            )
            .run();
          return json({
            ok: true,
            databaseIdentity: identity,
            analyzed: await analyze(db),
          });
        } finally {
          await conn?.close();
        }
      }

      if (url.pathname === "/admin/seed-d1") {
        if (req.method !== "POST") return methodNotAllowed("POST");
        if (!isAdmin(req, env)) return unauthorized();
        const rows = numParam(url, "rows", 10000, 1, MAX_SEED_ROWS);
        if (rows === null)
          return bad(`rows must be an integer 1..${MAX_SEED_ROWS}`);
        const t0 = performance.now();
        const ms: Record<string, number> = {};
        const analyzed: Record<string, boolean> = {};
        for (const { name, db } of allSeedTargets(env)) {
          const s0 = performance.now();
          await db.exec(KV_DDL + LMS_DDL);
          await seedKv(db, rows);
          analyzed[name] = await analyze(db);
          ms[name] = performance.now() - s0;
        }
        return json({
          ok: true,
          rows,
          ms,
          analyzed,
          totalMs: performance.now() - t0,
        });
      }

      if (url.pathname === "/admin/seed-lms") {
        if (req.method !== "POST") return methodNotAllowed("POST");
        if (!isAdmin(req, env)) return unauthorized();
        const dims: SeedDims = {
          tenants: numParam(url, "tenants", 10, 1, MAX_SEED_TENANTS) ?? -1,
          studentsPerTenant:
            numParam(
              url,
              "students-per-tenant",
              200,
              1,
              MAX_STUDENTS_PER_TENANT,
            ) ?? -1,
          coursesPerTenant:
            numParam(url, "courses-per-tenant", 5, 1, MAX_COURSES_PER_TENANT) ??
            -1,
          lessonsPerCourse:
            numParam(
              url,
              "lessons-per-course",
              10,
              1,
              MAX_LESSONS_PER_COURSE,
            ) ?? -1,
        };
        if (
          dims.tenants < 0 ||
          dims.studentsPerTenant < 0 ||
          dims.coursesPerTenant < 0 ||
          dims.lessonsPerCourse < 0
        ) {
          return bad(
            "seed-lms params out of range (tenants, students-per-tenant, courses-per-tenant, lessons-per-course)",
          );
        }
        const t0 = performance.now();
        const perDb: Record<string, unknown> = {};
        for (const { name, db } of allSeedTargets(env)) {
          const s0 = performance.now();
          await db.exec(KV_DDL + LMS_DDL);
          const target = allSeedTargets(env).find((t) => t.name === name);
          const counts = await seedLms(db, dims, target?.tenant);
          const analyzed = await analyze(db);
          perDb[name] = { ...counts, analyzed, ms: performance.now() - s0 };
        }
        return json({ ok: true, dims, perDb, totalMs: performance.now() - t0 });
      }

      const m = url.pathname.match(
        /^\/bench\/([A-Za-z0-9_-]+)\/([A-Za-z0-9_-]+)$/,
      );
      if (m) {
        const backend = normalizeBackend(m[1] ?? "");
        // Legacy alias: scan-100 => scan&limit=100.
        const rawTest = m[2] ?? "";
        const test = rawTest === "scan-100" ? "scan" : rawTest;
        if (!backend) return bad(`unknown backend ${JSON.stringify(m[1])}`);
        if (WRITE_TESTS[test] !== true && !isReadTest(test))
          return bad(`unknown test ${JSON.stringify(rawTest)}`);
        const isWrite = WRITE_TESTS[test] === true;
        if (isWrite) {
          if (req.method !== "POST") return methodNotAllowed("POST");
          if (!isAdmin(req, env)) return unauthorized();
        } else if (req.method !== "GET") return methodNotAllowed("GET");

        const tenant = numParam(url, "tenant", 1, 1, MAX_SEED_TENANT_INDEX);
        const id = numParam(url, "id", 1, 1, 2_000_000_000);
        const startId = numParam(
          url,
          "startId",
          numParam(url, "start-id", 1, 1, 2_000_000_000) ?? 1,
          1,
          2_000_000_000,
        );
        const limit = numParam(
          url,
          "limit",
          test === "scan" ? 100 : 100,
          1,
          MAX_SCAN_LIMIT,
        );
        if (
          tenant === null ||
          id === null ||
          startId === null ||
          limit === null
        )
          return bad("invalid numeric query param");
        let body: Record<string, unknown> = {};
        if (isWrite) {
          const parsed = await parseJsonBody(req);
          if (parsed instanceof Response) return parsed;
          body = parsed.body;
        }
        const num = (name: string, dflt: number): number | null => {
          const fromBody = body[name];
          if (fromBody !== undefined) {
            if (
              typeof fromBody !== "number" ||
              !Number.isInteger(fromBody) ||
              fromBody < 0
            )
              return null;
            return fromBody;
          }
          const fromQuery = url.searchParams.get(name);
          if (fromQuery === null) return dflt;
          const n = Number(fromQuery);
          return Number.isInteger(n) && n >= 0 ? n : null;
        };
        const student = num("student", 1);
        const course = num("course", 1);
        const lesson = num("lesson", 1);
        const quiz = num("quiz", 1);
        const question = num("question", 1);
        const attempt = num("attempt", 1);
        const completed = num("completed", 1);
        const position = num("position", 1);
        const retries = num("retries", 3);
        if (
          student === null ||
          course === null ||
          lesson === null ||
          quiz === null ||
          question === null ||
          attempt === null ||
          completed === null ||
          position === null ||
          retries === null
        ) {
          return bad(
            "invalid numeric param (student/course/lesson/quiz/question/attempt/completed/position/retries)",
          );
        }
        if (retries > 20) return bad("retries must be 0..20");
        const payload =
          strField(body, "payload") ??
          crypto.randomUUID() + crypto.randomUUID();
        const answer = strField(body, "answer") ?? "B";
        const params: OpParams = {
          id: id ?? 1,
          startId: startId ?? 1,
          limit: limit ?? 100,
          tenant,
          student,
          course,
          lesson,
          quiz,
          question,
          attempt,
          position,
          payload,
          answer,
          completed,
          retries,
        };
        requestBackend = backend;
        requestTenant = tenant;
        const { db, kind, binding, mapping } = resolveTenantDb(
          env,
          backend,
          tenant,
        );
        const timeoutMs = numParam(url, "timeoutMs", 5000, 100, 60000);
        if (timeoutMs === null) return bad("invalid operation timeout");
        const controller = new AbortController();
        const onAbort = () => controller.abort(req.signal.reason);
        req.signal.addEventListener("abort", onAbort, { once: true });
        const handlerTimer = setTimeout(
          () =>
            controller.abort(
              new BenchError(
                "timeout",
                "worker operation timeout",
                "worker-operation",
              ),
            ),
          timeoutMs,
        );
        let outcome: Awaited<ReturnType<typeof runOp>>;
        try {
          const operation = runOp(backend, db, env, test, params, kind, {
            signal: controller.signal,
            timeoutMs,
            waitUntil: ctx ? (p) => ctx.waitUntil(p) : undefined,
          });
          // Observing the deadline does not cancel existing I/O; keep cleanup owned
          // by this invocation during the runtime's background grace period.
          ctx?.waitUntil(
            operation.then(
              () => {},
              () => {},
            ),
          );
          outcome = await bounded(
            () => operation,
            timeoutMs,
            "worker-operation",
            controller.signal,
          );
        } finally {
          clearTimeout(handlerTimer);
          req.signal.removeEventListener("abort", onAbort);
        }
        const { t, extra } = outcome;
        const response = json({
          deploymentVersion:
            (env.CF_VERSION_METADATA as { id?: string } | undefined)?.id ??
            BUILD_INFO.buildId,
          sourceFingerprint: BUILD_INFO.sourceFingerprint,
          workerHandlerMs: performance.now() - handler0,
          tenantRoutingMs: null,
          dbExecutionMs: t.queryMs,
          poolCheckoutMs: t.checkoutMs,
          connectionCreationMs: null,
          requestedTenant: tenant,
          databaseIdentity: mapping?.databaseId ?? binding,
          databaseIdentitySource: mapping ? "manifest" : "validated-binding",
          tenantIsolation: backendSpec(backend).tenantIsolation,
          accessLayer: backendSpec(backend).accessLayer,
          colo: (req as Request & { cf?: { colo?: string } }).cf?.colo ?? null,
          backend,
          test,
          engine: engineOf(backend),
          ...t,
          buildMs: null,
          clientCreationMs: null,
          dbMs: t.dbMs,
          tenant,
          tenantBinding: binding,
          sessions: usesSessions(backend),
          ...workerTimingEvidence(extra),
          lifecycle,
        });
        const serializationEnd = performance.now();
        response.headers.set(
          "Server-Timing",
          `handler;dur=${serializationEnd - handler0},db;dur=${t.queryMs}`,
        );
        response.headers.set(
          "X-Bench-Total-Server-Ms",
          String(serializationEnd - handler0),
        );
        return response;
      }

      if (url.pathname === "/bench/meta") {
        const weur = await env.DB.prepare(
          "SELECT COUNT(*) AS n FROM kv",
        ).first<{ n: number }>();
        const eeur = await env.DB_EEUR.prepare(
          "SELECT COUNT(*) AS n FROM kv",
        ).first<{ n: number }>();
        let lms: Record<string, number | null> = {};
        let concurrency: Record<string, number | null> = {};
        try {
          const users = await env.DB.prepare(
            "SELECT COUNT(*) AS n FROM users",
          ).first<{ n: number }>();
          const courses = await env.DB.prepare(
            "SELECT COUNT(*) AS n FROM courses",
          ).first<{ n: number }>();
          const lessons = await env.DB.prepare(
            "SELECT COUNT(*) AS n FROM lessons",
          ).first<{ n: number }>();
          const enrollments = await env.DB.prepare(
            "SELECT COUNT(*) AS n FROM enrollments",
          ).first<{ n: number }>();
          lms = {
            users: users?.n ?? null,
            courses: courses?.n ?? null,
            lessons: lessons?.n ?? null,
            enrollments: enrollments?.n ?? null,
          };
          const progress = await env.DB.prepare(
            "SELECT COUNT(*) AS n FROM concurrent_progress",
          ).first<{ n: number }>();
          const hot = await env.DB.prepare(
            "SELECT COUNT(*) AS n FROM hot_counter",
          ).first<{ n: number }>();
          concurrency = {
            concurrent_progress: progress?.n ?? null,
            hot_counter: hot?.n ?? null,
          };
        } catch {
          lms = {};
          concurrency = {};
        }
        const countOf = async (
          conn: TursoClient,
          sql: string,
        ): Promise<number | null> => {
          try {
            const row = (await conn.get(sql)) as { n: number } | undefined;
            return row?.n ?? null;
          } catch {
            return null;
          }
        };
        let tursoRows: number | null = null;
        let tursoError: string | null = null;
        let tursoProgress: number | null = null;
        try {
          const conn = freshTursoConn(env);
          try {
            tursoRows = await countOf(conn, "SELECT COUNT(*) AS n FROM kv");
            tursoProgress = await countOf(
              conn,
              "SELECT COUNT(*) AS n FROM concurrent_progress",
            );
          } finally {
            await conn.close();
          }
        } catch (e) {
          tursoError = safeError(e).error;
        }
        let tursodbRows: number | null = null;
        let tursodbProgress: number | null = null;
        let tursodbHot: number | null = null;
        let tursodbError: string | null = null;
        if (tursoDbConfigured(env)) {
          try {
            const conn = freshTursoDbConn(env);
            try {
              tursodbRows = await countOf(conn, "SELECT COUNT(*) AS n FROM kv");
              tursodbProgress = await countOf(
                conn,
                "SELECT COUNT(*) AS n FROM concurrent_progress",
              );
              tursodbHot = await countOf(
                conn,
                "SELECT COUNT(*) AS n FROM hot_counter",
              );
            } finally {
              await conn.close();
            }
          } catch (e) {
            tursodbError = safeError(e).error;
          }
        } else {
          tursodbError = "TURSODB_URL/TURSODB_TOKEN not configured";
        }
        return json({
          d1_kv_rows: weur?.n,
          d1_weur_kv_rows: weur?.n,
          d1_eeur_kv_rows: eeur?.n,
          turso_kv_rows: tursoRows,
          turso_error: tursoError,
          tursodb_kv_rows: tursodbRows,
          tursodb_progress_rows: tursodbProgress,
          tursodb_hot_rows: tursodbHot,
          tursodb_error: tursodbError,
          d1_lms: lms,
          d1_concurrency_rows: concurrency,
          turso_progress_rows: tursoProgress,
        });
      }

      return json(
        {
          error: "not found",
          routes: [
            "GET /health",
            "GET /bench/identity",
            "POST /admin/seed-d1?rows=N",
            "POST /admin/seed-lms?tenants=N&students-per-tenant=N&courses-per-tenant=N&lessons-per-course=N",
            "GET /bench/:backend/:test (reads)",
            "POST /bench/:backend/:test (writes)",
            "GET /bench/meta",
          ],
        },
        404,
      );
    } catch (e) {
      const error = String(e).includes("TOPOLOGY_INVALID")
        ? {
            error: "TOPOLOGY_INVALID",
            errorClass: "application",
            timeoutClass: null,
          }
        : safeError(e);
      const details = (e as { evidence?: Record<string, unknown> })?.evidence;
      return json(
        {
          ...error,
          evidence: details
            ? {
                ...workerTimingEvidence(details),
                lifecycle,
                engine: requestBackend
                  ? engineOf(requestBackend as Parameters<typeof engineOf>[0])
                  : null,
                requestedTenant: requestTenant,
              }
            : undefined,
        },
        error.errorClass === "timeout" ? 504 : 500,
      );
    }
  },
};

const MAX_SEED_TENANT_INDEX = 100000;
