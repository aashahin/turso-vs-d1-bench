import { connect } from "@tursodatabase/serverless";

interface Env {
  DB: D1Database;
  DB_EEUR: D1Database;
  TURSO_URL: string;
  TURSO_TOKEN: string;
  ADMIN_TOKEN: string;
}

type Test = "point-read" | "scan-100" | "insert";
type Backend = "d1" | "d1-eeur" | "turso" | "d1-rr" | "d1-eeur-rr";

// d1*,d1-eeur* hit the WEUR/EEUR primary directly; *-rr go through the
// Sessions API (replica when RR is enabled, else primary).
function pickDb(env: Env, backend: Backend): { db: D1Database; sessions: boolean } {
  if (backend === "d1-eeur" || backend === "d1-eeur-rr") return { db: env.DB_EEUR, sessions: backend.endsWith("-rr") };
  return { db: env.DB, sessions: backend.endsWith("-rr") };
}

const MAX_SEED_ROWS = 50000;
const MAX_SCAN_LIMIT = 1000;

const json = (data: unknown, status = 200) =>
  new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json" },
  });

const unauthorized = () => json({ error: "unauthorized: valid Bearer ADMIN_TOKEN required" }, 401);
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

function tursoConn(env: Env) {
  return connect({ url: env.TURSO_URL, authToken: env.TURSO_TOKEN });
}

function parseId(url: URL): number | null {
  const n = Number(url.searchParams.get("id") ?? "1");
  return Number.isInteger(n) && n >= 1 ? n : null;
}

function parseLimit(url: URL): number | null {
  const n = Number(url.searchParams.get("limit") ?? "100");
  return Number.isInteger(n) && n >= 1 && n <= MAX_SCAN_LIMIT ? n : null;
}

function parseSeedRows(url: URL): number | null {
  const n = Number(url.searchParams.get("rows") ?? "10000");
  return Number.isInteger(n) && n >= 1 && n <= MAX_SEED_ROWS ? n : null;
}

// Shared insert-body boundary so both backends store the same value.
// Missing/empty body => random payload; malformed JSON or non-string => Response(400).
async function parseInsertBody(req: Request): Promise<{ payload: string } | Response> {
  const text = await req.text();
  if (text === "") return { payload: crypto.randomUUID() + crypto.randomUUID() };
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return bad("invalid JSON body");
  }
  if (typeof body !== "object" || body === null || Array.isArray(body)) return bad("body must be a JSON object");
  const p = (body as Record<string, unknown>).payload;
  if (p === undefined) return { payload: crypto.randomUUID() + crypto.randomUUID() };
  if (typeof p !== "string") return bad("payload must be a string");
  return { payload: p };
}

async function runD1(db: D1Database, test: Test, id: number, limit: number, payload?: string) {
  const t0 = performance.now();
  let extra: Record<string, unknown> = {};
  if (test === "point-read") {
    const row = await db.prepare("SELECT id, payload FROM kv WHERE id = ?").bind(id).first();
    extra = { row };
  } else if (test === "scan-100") {
    const { results } = await db.prepare("SELECT id, payload FROM kv ORDER BY id LIMIT ?").bind(limit).all();
    extra = { rows: results.length };
  } else {
    const res = await db.prepare("INSERT INTO bench_writes (payload) VALUES (?)")
      .bind(payload ?? crypto.randomUUID())
      .run();
    extra = { id: res.meta.last_row_id };
  }
  return { dbMs: performance.now() - t0, ...extra };
}

async function runTurso(env: Env, test: Test, id: number, limit: number, payload?: string) {
  const conn = tursoConn(env);
  try {
    const t0 = performance.now();
    let extra: Record<string, unknown> = {};
    if (test === "point-read") {
      const row = await conn.get("SELECT id, payload FROM kv WHERE id = ?", id);
      extra = { row: row ?? null };
    } else if (test === "scan-100") {
      const rows = await conn.all("SELECT id, payload FROM kv ORDER BY id LIMIT ?", limit);
      extra = { rows: rows.length };
    } else {
      const info = await conn.run("INSERT INTO bench_writes (payload) VALUES (?)", payload ?? crypto.randomUUID());
      extra = { id: info.lastInsertRowid };
    }
    return { dbMs: performance.now() - t0, ...extra };
  } finally {
    await conn.close();
  }
}

// D1 with global read replication: unconstrained session routes reads to the
// nearest read replica (or primary); writes still go to the primary.
// servedBy* tracks where each query actually executed.
async function runD1RR(db: D1Database, test: Test, id: number, limit: number, payload?: string) {
  const session = db.withSession();
  const t0 = performance.now();
  let extra: Record<string, unknown> = {};
  let servedByPrimary: boolean | null = null;
  let servedByRegion: string | null = null;
  if (test === "point-read") {
    const rs = await session.prepare("SELECT id, payload FROM kv WHERE id = ?").bind(id).all();
    servedByPrimary = (rs.meta as unknown as Record<string, unknown>).served_by_primary as boolean | null;
    servedByRegion = (rs.meta as unknown as Record<string, unknown>).served_by_region as string | null;
    extra = { row: rs.results[0] ?? null };
  } else if (test === "scan-100") {
    const rs = await session.prepare("SELECT id, payload FROM kv ORDER BY id LIMIT ?").bind(limit).all();
    servedByPrimary = (rs.meta as unknown as Record<string, unknown>).served_by_primary as boolean | null;
    servedByRegion = (rs.meta as unknown as Record<string, unknown>).served_by_region as string | null;
    extra = { rows: rs.results.length };
  } else {
    const rs = await session.prepare("INSERT INTO bench_writes (payload) VALUES (?)").bind(payload ?? crypto.randomUUID()).run();
    servedByPrimary = (rs.meta as unknown as Record<string, unknown>).served_by_primary as boolean | null;
    servedByRegion = (rs.meta as unknown as Record<string, unknown>).served_by_region as string | null;
    extra = { id: rs.meta.last_row_id };
  }
  return { dbMs: performance.now() - t0, servedByPrimary, servedByRegion, ...extra };
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    try {
      const url = new URL(req.url);

      if (url.pathname === "/health") return json({ ok: true, ts: new Date().toISOString() });

      if (url.pathname === "/bench/identity") {
        let tursoHost: string | null = null;
        try {
          tursoHost = new URL(env.TURSO_URL).host;
        } catch {
          tursoHost = null;
        }
        return json({ tursoHost });
      }

      if (url.pathname === "/admin/seed-d1") {
        if (req.method !== "POST") return methodNotAllowed("POST");
        if (!isAdmin(req, env)) return unauthorized();
        const rows = parseSeedRows(url);
        if (rows === null) return bad(`rows must be an integer 1..${MAX_SEED_ROWS}`);
        const t0 = performance.now();
        const ms: Record<string, number> = {};
        for (const [name, db] of [["weur", env.DB], ["eeur", env.DB_EEUR]] as const) {
          const s0 = performance.now();
          await db.exec(
            "CREATE TABLE IF NOT EXISTS kv (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);" +
              "CREATE TABLE IF NOT EXISTS bench_writes (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')));" +
              "DELETE FROM kv; DELETE FROM bench_writes;",
          );
          const payload = "x".repeat(200);
          let batch: D1PreparedStatement[] = [];
          for (let i = 1; i <= rows; i++) {
            batch.push(db.prepare("INSERT INTO kv (id, payload) VALUES (?, ?)").bind(i, `${i}:${payload}`));
            if (batch.length === 100 || i === rows) {
              await db.batch(batch);
              batch = [];
            }
          }
          ms[name] = performance.now() - s0;
        }
        return json({ ok: true, rows, ms, totalMs: performance.now() - t0 });
      }

      const m = url.pathname.match(/^\/bench\/(d1|d1-eeur|turso|d1-rr|d1-eeur-rr)\/(point-read|scan-100|insert)$/);
      if (m) {
        const [, backend, test] = m as unknown as [string, Backend, Test];
        if (test === "insert") {
          if (req.method !== "POST") return methodNotAllowed("POST");
          if (!isAdmin(req, env)) return unauthorized();
          const parsed = await parseInsertBody(req);
          if (parsed instanceof Response) return parsed;
          const { db, sessions } = pickDb(env, backend);
          const r = backend === "turso" ? await runTurso(env, test, 0, 0, parsed.payload) : sessions ? await runD1RR(db, test, 0, 0, parsed.payload) : await runD1(db, test, 0, 0, parsed.payload);
          return json({ backend, test, ...r });
        }
        if (req.method !== "GET") return methodNotAllowed("GET");
        const id = parseId(url);
        if (id === null) return bad("id must be an integer >= 1");
        const limit = parseLimit(url);
        if (limit === null) return bad(`limit must be an integer 1..${MAX_SCAN_LIMIT}`);
        const { db, sessions } = pickDb(env, backend);
        const r = backend === "turso" ? await runTurso(env, test, id, limit) : sessions ? await runD1RR(db, test, id, limit) : await runD1(db, test, id, limit);
        return json({ backend, test, ...r });
      }

      if (url.pathname === "/bench/meta") {
        const weur = await env.DB.prepare("SELECT COUNT(*) AS n FROM kv").first<{ n: number }>();
        const eeur = await env.DB_EEUR.prepare("SELECT COUNT(*) AS n FROM kv").first<{ n: number }>();
        const conn = tursoConn(env);
        try {
          const row = (await conn.get("SELECT COUNT(*) AS n FROM kv")) as { n: number } | undefined;
          return json({ d1_kv_rows: weur?.n, d1_weur_kv_rows: weur?.n, d1_eeur_kv_rows: eeur?.n, turso_kv_rows: row?.n });
        } finally {
          await conn.close();
        }
      }

      return json({ error: "not found", routes: ["GET /health", "GET /bench/identity", "POST /admin/seed-d1?rows=N", "GET /bench/:backend/:test (reads)", "POST /bench/:backend/insert", "GET /bench/meta"] }, 404);
    } catch (e) {
      console.error(JSON.stringify({ msg: "request_failed", err: String(e) }));
      return json({ error: String(e) }, 500);
    }
  },
};
