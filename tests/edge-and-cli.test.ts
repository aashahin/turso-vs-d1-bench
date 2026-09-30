import { expect, spyOn, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { makeEdgeExec } from "../src/exec.ts";
import type { ResultDoc } from "../src/results.ts";
import worker from "../worker/src/index.ts";
import { createLifecycleTracker } from "../worker/src/lifecycle.ts";
import type { Env } from "../worker/src/backends.ts";

test("Worker health checks warm the isolate and edge execution preserves lifecycle evidence", async () => {
  const reads: unknown[][] = [];
  const db = {
    prepare(sql: string) {
      return { bind(...params: unknown[]) { return { async first() {
        reads.push([sql, ...params]);
        return { id: params[0], payload: "seeded" };
      } }; } };
    },
  } as D1Database;
  const env: Env = { DB: db, DB_EEUR: db, TURSO_URL: "https://unused.invalid", TURSO_TOKEN: "test", ADMIN_TOKEN: "test" };
  const health = await worker.fetch(new Request("https://worker.invalid/health"), env);
  expect(health.status).toBe(200);
  const fetchWorker = Object.assign(async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const request = input instanceof Request ? new Request(input, init) : new Request(String(input), init);
    return worker.fetch(request, env);
  }, { preconnect: globalThis.fetch.preconnect });
  const fakeFetch = spyOn(globalThis, "fetch").mockImplementation(fetchWorker);
  try {
    const exec = makeEdgeExec("https://worker.invalid", "", "d1");
    const op = { test: "point-read", method: "GET" as const, params: { id: 42, tenant: 1 } };
    const first = await exec(op, new AbortController().signal);
    const following = await exec(op, new AbortController().signal);
    expect(first.lifecycle?.firstRequest).toBe(false);
    expect(following.lifecycle?.isolateId).toBe(first.lifecycle?.isolateId);
    expect(following.lifecycle?.requestNumber).toBe(first.lifecycle!.requestNumber + 1);
    expect(first.dbMs).toBeGreaterThanOrEqual(0);
    expect(reads).toEqual(Array(2).fill(["SELECT id, payload FROM kv WHERE id = ?", 42]));
  } finally { fakeFetch.mockRestore(); }
});

test.each([
  { dbMs: -1 }, { dbMs: "10" }, { dbMs: null }, {},
])("edge executor rejects invalid timing rather than counting success: %j", async (body) => {
  const fakeFetch = spyOn(globalThis, "fetch").mockResolvedValue(Response.json(body));
  try {
    await expect(makeEdgeExec("https://worker.invalid", "", "d1")({ test: "point-read", method: "GET", params: {} }, new AbortController().signal)).rejects.toThrow("bad response");
  } finally { fakeFetch.mockRestore(); }
});

test("CLI runs cold observations first, once per run, and exports separate results", async () => {
  const directory = await mkdtemp(join(tmpdir(), "bench-cold-start-"));
  const paths: string[] = [];
  const track = createLifecycleTracker();
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch(req) {
    const url = new URL(req.url);
    paths.push(url.pathname);
    const lifecycle = track();
    if (url.pathname === "/cdn-cgi/trace") return new Response("colo=TEST\n");
    if (url.pathname === "/bench/meta") return Response.json({ d1_weur_kv_rows: 100 });
    if (url.pathname === "/bench/identity") return Response.json({ tursodbConfigured: false });
    return Response.json({ dbMs: 7, queryMs: 5, engine: "d1", transactionMode: "none", lifecycle });
  } });
  const out = join(directory, "results.json");
  const env = {
    ...process.env,
    WORKER_URL: server.url.href.replace(/\/$/, ""), BACKENDS: "d1", TESTS: "point-read,cold-start", MODES: "edge",
    COLD_SAMPLES: "2", COLD_IDLE_MS: "0", COLD_WARM_REQUESTS: "2",
    WARMUP: "2", ITERATIONS: "3", DURATION: "0", CONCURRENCY: "1,3", RUNS: "2",
    TENANT_MODE: "single", TENANT_COUNT: "1", SEED_ROWS: "100", LOAD_MODEL: "constant", TIMEOUT_MS: "1000",
    OUT: out, ADMIN_TOKEN: "", TURSO_URL: "", TURSO_TOKEN: "", TURSODB_URL: "", TURSODB_TOKEN: "",
  };
  const child = Bun.spawn([process.execPath, "src/bench.ts"], { cwd: join(import.meta.dir, ".."), env, stdout: "pipe", stderr: "pipe" });
  const deadline = setTimeout(() => child.kill(), 10000);
  try {
    const [status, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
    expect({ status, stderr }).toEqual({ status: 0, stderr: "" });
    const doc = JSON.parse(await readFile(out, "utf8")) as ResultDoc;
    expect(doc.coldStarts).toHaveLength(2); // Not multiplied by --concurrency.
    expect(doc.scenarios).toHaveLength(2);
    expect(doc.scenarios.map((s) => s.concurrency)).toEqual([1, 3]);
    expect(doc.coldStarts[0]).toMatchObject({ first: { success: 2 }, following: { success: 4 }, firstOnNewIsolate: 1, pairedTrials: 2 });
    expect(doc.coldStarts[1]?.firstOnNewIsolate).toBe(0);
    // 12 cold requests, then 20 ordinary requests including warmup, then metadata.
    expect(paths.slice(0, 32)).toEqual(Array(32).fill("/bench/d1/point-read"));
    expect(paths.slice(32)).toEqual(["/cdn-cgi/trace", "/bench/meta", "/bench/identity"]);
    expect(doc.coldStarts[0]?.trials[0]?.first).toMatchObject({ ok: true, sample: { lifecycle: { requestNumber: 1 } } });
    expect(doc.coldStarts[1]?.trials[1]?.following[1]).toMatchObject({ ok: true, sample: { lifecycle: { requestNumber: 12 } } });
    expect((await readFile(join(directory, "results-cold-start.csv"), "utf8")).trim().split("\n")).toHaveLength(3);
    expect((await readFile(join(directory, "results.csv"), "utf8")).trim().split("\n")).toHaveLength(5);
    expect(stdout).toContain("COLD START — first request after idle");

    const invalid = Bun.spawn([process.execPath, "src/bench.ts"], { cwd: join(import.meta.dir, ".."), env: { ...env, TESTS: "cold-start", MODES: "direct", BACKENDS: "turso" }, stdout: "pipe", stderr: "pipe" });
    const [invalidStatus, message] = await Promise.all([invalid.exited, new Response(invalid.stderr).text()]);
    expect(invalidStatus).not.toBe(0);
    expect(message).toContain("cold-start requires --modes=edge");
    expect(paths).toHaveLength(35);
  } finally {
    clearTimeout(deadline);
    child.kill();
    await server.stop(true);
    await rm(directory, { recursive: true, force: true });
  }
}, 15000);
