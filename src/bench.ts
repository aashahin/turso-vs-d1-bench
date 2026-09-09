import { connect } from "@tursodatabase/serverless";
import { parseArgs, type Backend, type Test } from "./config.ts";
import { summarize, printTable, type Summary } from "./stats.ts";

interface OpResult {
  e2eMs: number;
  dbMs: number | null;
  // true = served by read replica, false = primary, null = not applicable/unknown
  replica: boolean | null;
}

async function edgeOp(
  workerUrl: string,
  adminToken: string,
  backend: Backend,
  test: Test,
  seedRows: number,
): Promise<OpResult> {
  const id = 1 + Math.floor(Math.random() * seedRows);
  const t0 = performance.now();
  let res: Response;
  if (test === "insert") {
    res = await fetch(`${workerUrl}/bench/${backend}/${test}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({ payload: crypto.randomUUID() + crypto.randomUUID() }),
    });
  } else if (test === "point-read") {
    res = await fetch(`${workerUrl}/bench/${backend}/${test}?id=${id}`);
  } else {
    res = await fetch(`${workerUrl}/bench/${backend}/${test}?limit=100`);
  }
  if (!res.ok) throw new Error(`edge ${backend}/${test}: HTTP ${res.status} ${await res.text()}`);
  const body = (await res.json()) as { dbMs: number; servedByPrimary?: boolean | null };
  const replica = body.servedByPrimary === false ? true : body.servedByPrimary === true ? false : null;
  return { e2eMs: performance.now() - t0, dbMs: body.dbMs, replica };
}

function directTursoConn(tursoUrl: string, tursoToken: string) {
  // serverless SDK speaks HTTPS Hrana; turso CLI prints libsql:// — convert.
  const url = tursoUrl.replace(/^libsql:\/\//, "https://");
  return connect({ url, authToken: tursoToken });
}

async function directTursoOp(tursoUrl: string, tursoToken: string, test: Test, seedRows: number): Promise<OpResult> {
  // Timer spans connect through close so direct e2e is comparable with edge
  // e2e (the Worker awaits Turso close before responding).
  const t0 = performance.now();
  const conn = directTursoConn(tursoUrl, tursoToken);
  try {
    const id = 1 + Math.floor(Math.random() * seedRows);
    if (test === "point-read") {
      await conn.get("SELECT id, payload FROM kv WHERE id = ?", id);
    } else if (test === "scan-100") {
      await conn.all("SELECT id, payload FROM kv ORDER BY id LIMIT 100");
    } else {
      await conn.run("INSERT INTO bench_writes (payload) VALUES (?)", crypto.randomUUID() + crypto.randomUUID());
    }
  } finally {
    await conn.close();
  }
  return { e2eMs: performance.now() - t0, dbMs: null, replica: null };
}

async function runCombo(
  label: string,
  iterations: number,
  concurrency: number,
  warmup: number,
  fn: () => Promise<OpResult>,
): Promise<{ label: string; e2e: Summary; db: Summary | null; concurrency: number; replicaRate: number | null }> {
  for (let i = 0; i < warmup; i++) await fn();
  if (globalThis.gc) globalThis.gc();

  const e2e: number[] = [];
  const db: number[] = [];
  let replicaHits = 0;
  let replicaKnown = 0;
  const wall0 = performance.now();
  const step = Math.max(1, Math.floor(iterations / 10 / concurrency) * concurrency);
  for (let i = 0; i < iterations; i += concurrency) {
    const batch = Array.from({ length: Math.min(concurrency, iterations - i) }, () => fn());
    const results = await Promise.all(batch);
    for (const r of results) {
      e2e.push(r.e2eMs);
      if (r.dbMs !== null) db.push(r.dbMs);
      if (r.replica !== null) {
        replicaKnown++;
        if (r.replica) replicaHits++;
      }
    }
    const done = Math.min(i + concurrency, iterations);
    if (done === iterations || done % step === 0) console.log(`${label} c=${concurrency}: ${done}/${iterations}`);
  }
  const wallMs = performance.now() - wall0;
  return {
    label: `${label} c=${concurrency}`,
    e2e: summarize(e2e, wallMs),
    db: db.length ? summarize(db, wallMs) : null,
    concurrency,
    replicaRate: replicaKnown ? replicaHits / replicaKnown : null,
  };
}

const args = parseArgs();
console.log(
  `mode=${args.modes.join("+")} backends=${args.backends.join(",")} tests=${args.tests.join(",")} iterations=${args.iterations} concurrency=${args.concurrencies.join(",")} warmup=${args.warmup}`,
);

const all: { mode: string; backend: string; test: string; concurrency: number; e2e: Summary; db: Summary | null; replicaRate: number | null }[] = [];

for (const mode of args.modes) {
  for (const backend of args.backends) {
    if (mode === "direct" && backend !== "turso") {
      console.log(`skip direct/${backend}: D1 has no public HTTP endpoint; access goes through the Worker (edge mode).`);
      continue;
    }
    if (mode === "direct" && (!args.tursoUrl || !args.tursoToken)) {
      throw new Error("direct mode needs --turso-url and --turso-token (or TURSO_URL/TURSO_TOKEN env)");
    }
    if (mode === "edge" && args.tests.includes("insert") && !args.adminToken) {
      throw new Error("edge insert needs --admin-token (or ADMIN_TOKEN env): Worker insert endpoints require Bearer auth");
    }
    for (const test of args.tests) {
      for (const c of args.concurrencies) {
        const label = `${mode}/${backend}/${test}`;
        const fn =
          mode === "edge"
            ? () => edgeOp(args.workerUrl, args.adminToken, backend, test, args.seedRows)
            : () => directTursoOp(args.tursoUrl, args.tursoToken, test, args.seedRows);
        const r = await runCombo(label, args.iterations, c, args.warmup, fn);
        all.push({ mode, backend, test, concurrency: c, e2e: r.e2e, db: r.db, replicaRate: r.replicaRate });
      }
    }
  }
}

printTable(all.map((r) => ({ label: `${r.mode}/${r.backend}/${r.test} c=${r.concurrency}`, e2e: r.e2e, db: r.db, replicaRate: r.replicaRate })));
await Bun.write(args.out, JSON.stringify({ ts: new Date().toISOString(), args: { ...args, tursoToken: "<redacted>", adminToken: "<redacted>" }, results: all }, null, 2));
console.log(`wrote ${args.out}`);
