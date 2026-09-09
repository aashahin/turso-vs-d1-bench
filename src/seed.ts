import { connect } from "@tursodatabase/serverless";

const MAX_SEED_ROWS = 50000;

// Seeds Turso directly + D1 via the Worker. Usage:
//   bun src/seed.ts --worker-url=http://127.0.0.1:8787 --rows=10000
// Env vars (WORKER_URL, TURSO_URL, TURSO_TOKEN, ADMIN_TOKEN, ROWS) take
// precedence over flags.
const g = (k: string, d: string) => {
  const env = process.env[k.toUpperCase().replace(/-/g, "_")];
  if (env !== undefined && env !== "") return env;
  const hit = process.argv.find((a) => a.startsWith(`--${k}=`));
  return hit ? hit.slice(k.length + 3) : d;
};

const workerUrl = g("worker-url", "http://127.0.0.1:8787").replace(/\/$/, "");
const rowsRaw = g("rows", "10000");
const rows = Number(rowsRaw);
if (!Number.isInteger(rows) || rows < 1 || rows > MAX_SEED_ROWS) {
  throw new Error(`--rows must be an integer 1..${MAX_SEED_ROWS} (got ${JSON.stringify(rowsRaw)})`);
}
const tursoUrl = g("turso-url", "").replace(/^libsql:\/\//, "https://");
const tursoToken = g("turso-token", "");
const adminToken = g("admin-token", "");
if (!tursoUrl || !tursoToken) throw new Error("need --turso-url and --turso-token (or TURSO_URL/TURSO_TOKEN env)");
if (!adminToken) throw new Error("need --admin-token (or ADMIN_TOKEN env) for Worker admin endpoints");
const adminHeaders = { authorization: `Bearer ${adminToken}` };

// 0. Preflight: the Worker must target the same Turso database the client seeds.
// Nothing is mutated before this passes.
const identRes = await fetch(`${workerUrl}/bench/identity`);
if (!identRes.ok) throw new Error(`worker identity check failed: HTTP ${identRes.status} ${await identRes.text()}`);
const ident = (await identRes.json()) as { tursoHost: string | null };
const clientHost = new URL(tursoUrl).host;
if (ident.tursoHost !== clientHost) {
  throw new Error(`worker targets Turso host ${JSON.stringify(ident.tursoHost)}, client targets ${JSON.stringify(clientHost)}; refusing to seed`);
}

const schema = await Bun.file("schema.sql").text();

// 1. Turso
console.log(`seeding turso ${rows} rows…`);
const conn = connect({ url: tursoUrl, authToken: tursoToken });
for (const stmt of schema.split(";").map((s) => s.trim()).filter(Boolean)) await conn.exec(stmt);
await conn.exec("DELETE FROM kv; DELETE FROM bench_writes;");
{
  const payload = "x".repeat(200);
  const t0 = performance.now();
  const CHUNK = 500; // one batch() per chunk keeps Hrana happy
  for (let start = 1; start <= rows; start += CHUNK) {
    const end = Math.min(start + CHUNK - 1, rows);
    const batch: string[] = [];
    for (let i = start; i <= end; i++) batch.push(`INSERT INTO kv (id, payload) VALUES (${i}, '${i}:${payload}')`);
    await conn.batch(batch);
    process.stdout.write(`\rturso: ${end}/${rows}`);
  }
  console.log(`\nturso seeded in ${Math.round(performance.now() - t0)}ms`);
}
// Verify through our own connection: the DB we just wrote must hold the rows.
const cnt = (await conn.get("SELECT COUNT(*) AS n FROM kv")) as { n: number } | undefined;
await conn.close();
if (cnt?.n !== rows) throw new Error(`turso verification failed: expected ${rows} rows, found ${cnt?.n}`);

// 2. D1 via worker (seeds WEUR + EEUR primaries)
console.log(`seeding d1 ${rows} rows…`);
const res = await fetch(`${workerUrl}/admin/seed-d1?rows=${rows}`, { method: "POST", headers: adminHeaders });
if (!res.ok) throw new Error(`seed-d1 failed: HTTP ${res.status} ${await res.text()}`);
console.log("d1:", await res.json());

// 3. Verify all three sides agree
const metaRes = await fetch(`${workerUrl}/bench/meta`);
if (!metaRes.ok) throw new Error(`meta check failed: HTTP ${metaRes.status} ${await metaRes.text()}`);
const meta = (await metaRes.json()) as { d1_weur_kv_rows: number; d1_eeur_kv_rows: number; turso_kv_rows: number };
console.log("meta:", meta);
if (meta.d1_weur_kv_rows !== rows || meta.d1_eeur_kv_rows !== rows || meta.turso_kv_rows !== rows) {
  throw new Error(`row count mismatch, expected ${rows}: ${JSON.stringify(meta)}`);
}
console.log("seed ok: identical row counts on all sides");
