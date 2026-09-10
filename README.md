# Turso vs D1 benchmark (multi-tenant LMS workload)

Fair latency/throughput comparison between **Turso Cloud**, **Turso Database
(tursodb, MVCC)**, **Cloudflare D1**, and **D1 with global read replication**,
measured from the same edge vantage point, with realistic multi-tenant LMS
query patterns (Manhali workload).

## Benchmark architecture

```text
bun runner (src/) ──HTTPS──▶ Cloudflare Worker (worker/src/) ──▶ D1
       │                              │                        ──▶ libSQL (turso*)
       │                     ┌────────┴─────────┐              ──▶ Turso Database (tursodb*)
       │                     │ raw / drizzle /  │
       │                     │ sessions (RR) /  │
       │                     │ pooled libSQL /  │
       │                     │ pooled tursodb / │
       │                     │ BEGIN CONCURRENT │
       └──── direct ──▶ Turso Cloud libSQL (reference only)
```

- The runner (`src/bench.ts`) generates a **deterministic op sequence** per
  scenario (pure function of the op index: same rows, same payload sizes, same
  tenant spread for every backend) and replays it through a
  **constant-concurrency pool**: N virtual workers each start the next op as
  soon as the previous one finishes, until `--duration` expires or
  `--iterations` is reached (whichever comes first). Legacy burst mode
  (`--load-model=burst`, fixed `Promise.all` batches) is kept only for
  before/after comparison.
- The Worker executes the **same SQL** on D1, libSQL and Turso Database (see
  `worker/src/queries.ts`), times it server-side (`dbMs`/`queryMs`), and
  reports routing metadata (`servedByPrimary`, `servedByRegion`,
  `tenantBinding`), the engine, the transaction mode actually used, and the
  MVCC conflict/retry counters. `Cache-Control: no-store` on all bench
  responses; every request uses fresh randomized IDs — no result caching, no
  same-row hot spot (scan uses `WHERE id >= ? ORDER BY id LIMIT ?` with a
  random start ID).
- `direct` mode (bun → Turso Cloud libSQL) is a **client-to-cloud reference
  only**. D1 has no public endpoint, so direct-vs-edge comparisons are
  meaningless; the console report excludes direct mode from best-per-category
  tables. Turso Database is Worker-only (edge mode).

## Turso Database (`tursodb`) backends — new engine + Concurrent Writes

Turso Cloud hosts two engines, and the benchmark keeps them apart so a result
is never attributed to the wrong cause:

| backend | engine | writes | client reuse |
|---|---|---|---|
| `turso-reused` | libSQL (existing Turso Cloud DB) | autocommit (existing behavior) | pool of 8 Connections |
| `tursodb-reused` | **Turso Database** (`turso db create --tursodb`) | normal `BEGIN IMMEDIATE` transaction for the transactional write tests, autocommit for a single-statement `insert` | pool of 8 Connections |
| `tursodb-concurrent` | **Turso Database** | `BEGIN CONCURRENT` (MVCC) with conflict retries | one reused Connection for transactions (each `transactionAsync` opens its own session), pool of 8 for reads |
| `d1` / `d1-eeur` | D1 | autocommit; `batch()` (atomic) for the transactional write tests | n/a |

`turso-reused` is the existing baseline and is unchanged. `tursodb-reused`
answers *is the new engine itself faster?*; `tursodb-concurrent` answers *how
much do MVCC / concurrent writes add on top of the engine?* Reads are never
wrapped in `BEGIN CONCURRENT` (it is a write-transaction feature): for read
tests `tursodb-reused` and `tursodb-concurrent` use the identical normal
`SELECT` path, so libSQL-vs-Turso-Database read latency is an engine-vs-engine
comparison.

### Concurrent Writes are an Early Preview feature

- MVCC is **optimistic**: transactions write row versions and conflicts are
  detected at **commit time**, at **row granularity**.
- Transactions touching **different rows** commit in parallel; transactions
  touching the **same row** collide and the loser must retry.
- **Hot-row contention is not solved by MVCC** — `hot-row-write` exists to
  measure that directly. Expect conflicts, retries and lower throughput there.
- Concurrent Writes on Turso Cloud are an **early preview** (account-level
  toggle) and **not production-ready**; the benchmark makes no availability or
  durability claims about them.

### Provisioning

1. Enable the feature in the Turso Dashboard: **Settings → General →
   Concurrent Writes** (early preview, per account).
2. Create the database in the **same group/region** as the existing libSQL
   database (here: group `manhali`, `aws-eu-west-1`) so the two engines are
   co-located and the comparison is not a distance measurement:

```bash
turso db create --tursodb bench-tursodb --group manhali --wait
turso db show bench-tursodb --url          # → TURSODB_URL
turso db tokens create bench-tursodb       # → TURSODB_TOKEN
```

3. Put `TURSODB_URL` in `worker/wrangler.jsonc` `vars` and the token in the
   Worker secret (`bun run worker:secret:tursodb`); keep `TURSO_URL` /
   `TURSO_TOKEN` for the existing libSQL database. **Never commit tokens**, and
   result files never contain them (hosts only).
4. Reseed so both engines and D1 hold identical data:
   `bun run seed:lms` (seeds libSQL + tursodb + every D1 target and verifies
   row counts on all three).

Notes from a real setup:

- The CLI prints the new database URL as `turso://<db>-<org>.<region>.turso.io`;
  the SDK normalizes `turso://`/`libsql://` to `https://`, and both forms work
  as `TURSODB_URL`.
- On Turso Cloud you do **not** set `PRAGMA journal_mode = 'mvcc'` (Cloud
  rejects journal-mode changes; the engine manages it). `BEGIN CONCURRENT`
  simply works once the database is a `tursodb` database.
- A real same-row conflict surfaces as `SQLITE_ABORT` with the message
  `Tursodb error: Write-write conflict`. The conflict detector matches
  `busy`/`conflict`/`locked`/`snapshot` in the error code **or** message, so
  this shape is retried; query timeouts and constraint errors are not.
- `connect()` + `transactionAsync(...).concurrent()` requires
  `@tursodatabase/serverless` ≥ 1.4.0 (installed: 1.4.0). Against a libSQL
  database the same call fails with HTTP 400 — that is expected, not a bug in
  the benchmark.

The Worker reports the engine and the transaction mode it actually executed in
every response, and `/bench/meta` reports row counts per engine (and a
`tursodb_error` if the database is unreachable).

### Conflict and retry measurement

`tursodb-concurrent` write operations run as one atomic
`BEGIN CONCURRENT .. COMMIT` batch — `conn.batch(statements, "concurrent")`,
a single HTTP request on a pooled connection — and retry only on **retryable
MVCC conflicts** (`SQLITE_BUSY` / `SQLITE_BUSY_SNAPSHOT` /
`Write-write conflict` messages). Application and constraint errors are never
retried. `--write-retries` (default `3`, max `20`) bounds the loop; backoff is
small exponential jitter (~1–3 ms, 2–6 ms, 4–12 ms, … capped at 50 ms).

The SDK also offers `transactionAsync(fn).concurrent()`, which is the API the
Turso docs show for MVCC. It is **not used here**: it allocates a dedicated
server session per transaction, and at benchmark concurrency Turso Cloud
rejects the resulting session count with
`DatabaseError: Database connections limit exceeded, try to reduce
concurrency` — which then fails even simple reads on that database until the
sessions are reaped (the raw evidence is kept in
`results-tursodb-txnasync-broken.json`). The atomic-batch form keeps
concurrency bounded by the connection pool, exactly like `turso-reused`.

The Worker measures the **entire loop**, so an operation that conflicts twice
and then commits is reported with the latency of all three attempts plus the
backoff — never just the successful attempt. Every response carries:

```text
attempted, attemptsTotal, committed, successAfterRetry, failedAfterRetries,
conflicts, retries, conflictRate, retryRate, avgRetries
```

An operation that exhausts its retry budget is counted as a **failure**
(`errorsByClass.conflict`) and excluded from the success percentiles, so
conflicts are never hidden. For non-MVCC backends these fields are empty in
CSV/JSON and `-` in the console report.

There is deliberately **no mutex, write queue, or promise chain** around
`tursodb-concurrent` in the Worker: each request takes a pooled connection
(the same 8-slot reuse pool as `turso-reused`, with `checkoutMs` reporting
queueing) and runs its transaction as one request. Concurrency is bounded by
that pool, not serialized by application code.

### Write benchmarks

- `insert` — single-statement write (kept as-is). Note that a one-statement
  insert is a very short transaction and therefore does **not** demonstrate the
  full benefit of `BEGIN CONCURRENT`.
- `independent-writes` — each op does `SELECT progress … ; UPDATE
  concurrent_progress SET progress = progress + 1 WHERE student_id = ?` for a
  different `student_id` (seeded 1..`--rows`) inside one transaction. This is
  the Manhali pattern (student A → row A, student B → row B, …) and the
  workload where row-level MVCC should help most.
- `hot-row-write` — every op reads and updates the single `hot_counter` row
  (id = 1). Pure write/write conflict: MVCC cannot remove contention here.

Both transactional tests use each backend's normal transaction equivalent
(`BEGIN IMMEDIATE` for libSQL/Turso Database, D1 `batch()` for D1) so
engine-vs-engine and transaction-mode-vs-transaction-mode are separate
comparisons.

### Measured SDK/server behavior worth knowing

- **A hung Turso fetch used to own a pool slot forever.** The reused-client
  pools originally chained each slot's next request on the previous request's
  promise, so one stalled fetch (e.g. a Turso call left running after the
  client aborted at its 5 s timeout) blocked that slot for the isolate's
  lifetime; after enough timeouts every slot was stuck and even `point-read`
  stopped answering — visible here as `/bench/meta` (fresh clients) returning
  in 1.7 s while pooled reads hung past 25 s. The pool now (a) aborts queued
  work as soon as the client's request signal fires and (b) recycles a
  connection and frees its slot if a request holds it longer than 15 s.
  `--warmup` also runs sequentially before each timed window — use a small
  value (`--warmup=10`) for sweeps, or it dominates wall time.
- **`transactionAsync(...).concurrent()` does not survive benchmark
  concurrency.** It opens a dedicated server session per transaction; at
  c≥10 against the tursodb database the server answers
  `DatabaseError: Database connections limit exceeded, try to reduce
  concurrency`, and the database keeps rejecting requests (including pooled
  reads) until sessions are reaped. Use atomic
  `conn.batch(stmts, "concurrent")` instead, which is what this benchmark
  runs — one request per transaction, bounded by the connection pool.
- **`conn.batch()` is not pipelined on the new engine.** Seeding the same data
  with a 100-statement `batch()` of single-row `INSERT`s took ~10.5 s
  (~105 ms/statement), while one multi-row `INSERT` of 100 rows took ~383 ms.
  `src/seed.ts` therefore writes 100 rows per statement (identical logical
  data on every backend); D1 seeding is unchanged.
- A rejected `BEGIN CONCURRENT` (e.g. run against a libSQL database) surfaces
  as `HTTP error! status: 400` with no server message.
- A real write/write conflict surfaces as `SQLITE_ABORT` /
  `Tursodb error: Write-write conflict`.
- Turso Cloud does not allow `PRAGMA journal_mode = 'mvcc'`; the engine
  manages MVCC itself, so the pragma is not set anywhere in this project.

## Why single-tenant and distributed-tenant benchmarks differ

- **Single** (`--tenant-mode=single`): all concurrency hits ONE database.
  Shows where a single tenant/DB saturates (latency knee, error rate). This is
  the primary D1 capacity question: one D1 database has finite write/read
  throughput, so p95 vs concurrency is the curve that matters.
- **Distributed** (`--tenant-mode=distributed --tenant-count=N`): requests
  spread uniformly over N databases. Manhali maps tenant → isolated D1
  database, so aggregate throughput can scale with tenant count while
  per-tenant load stays flat. Report both **total RPS** and **per-tenant RPS**.
- A D1 database per tenant scales differently from one large D1 database:
  contention (writes, hot rows, storage) is isolated per tenant, at the cost
  of N databases to manage. The distributed benchmark only measures
  isolated-DB scaling when tenant DBs are **separate D1 databases** (see
  below); `tenantDb: "shared"` rows measure request fan-out, not DB scaling.

## Provisioning tenant databases

The Worker routes `?tenant=N` to the `DB_TENANT_N` binding (WEUR family) or
`DB_EEUR_TENANT_N` (EEUR), when present — see `worker/wrangler.jsonc`. Without
that binding it falls back to `tenant_id` scoping in the shared primary and
reports `"tenantDb": "shared"` / `tenantBinding: "DB"`, so shared-fallback
rows can never be mistaken for isolated-DB numbers. Turso always scopes via
`tenant_id` in one database (`"shared"` by design).

To run a true isolated-DB test: create N D1 databases, add the bindings,
redeploy, reseed (`seed-lms` seeds every bound database), then run with
`--tenant-count` matching the provisioned bindings.

## Why Turso connection setup is measured separately

`turso` (aka `turso-raw`/`turso-request-client`) opens a client per request
and closes it — one possible app implementation. `turso-reused` reuses a pool
of 8 clients. The response splits `clientCreationMs` / `queryMs` /
`clientCloseMs` (`checkoutMs` for pool wait) so SDK overhead is visible
separately from query latency. Note: `connect()` itself does no I/O; `close()`
sends an extra Hrana request when a baton is held. The SDK documents
Connection as single-stream (concurrent calls serialize on an internal lock),
which is why the pool — the SDK's own recommended pattern — is used instead
of one global client (safe but fully serialized).

`tursodb-reused` uses the exact same pooled model (pool of 8, one
`connect()` per pool slot created once per Worker isolate, never closed per
request) so the engine comparison with `turso-reused` is apples-to-apples:
both report `clientCreationMs: null` and only `checkoutMs`/`queryMs`.
`tursodb-concurrent` reads through that same pool and runs its write
transactions as atomic `batch(stmts, "concurrent")` requests on pooled
connections — same pool, same reuse model, no per-request client and no
application-level queue. No client is created or closed per request on either
tursodb backend.

## Drizzle benchmarks

`d1-drizzle` / `d1-eeur-drizzle` / `turso-drizzle` compose the identical
statements through drizzle-orm's `sql` template (timed as `buildMs`) and
execute through the same underlying client as the raw path. Honest subset: it
measures query-composition overhead, not the relational mapper — treat the
raw-vs-drizzle gap as a lower bound. Raw and Drizzle are always reported as
separate backends, never blended.

## Read replication

`d1-rr` / `d1-eeur-rr` route reads through the Sessions API
(`withSession()`); writes still go to the primary. Every response carries
`servedByPrimary` / `servedByRegion` plus per-scenario `replicaReads` /
`primaryReads`; the runner aggregates `rep%` (replica-hit rate) and served
regions. Replication is **not** scored as a win automatically: when the
primary is already nearby, the session round-trip can cost more than it saves
— compare `d1` vs `d1-rr` at the same concurrency and let the numbers speak.

## Setup

```bash
bun install
cp .env.example .env   # fill TURSO_TOKEN + ADMIN_TOKEN (any random string)
bun run worker:secret:turso   # TURSO_TOKEN -> Worker env
bun run worker:secret:admin   # ADMIN_TOKEN -> Worker env (same value as .env)

# New Turso Database (optional; required for the tursodb* backends):
turso db create --tursodb bench-tursodb --group manhali --wait
# set TURSODB_URL in worker/wrangler.jsonc vars + TURSODB_TOKEN in .env, then:
bun run worker:secret:tursodb # TURSODB_TOKEN -> Worker env
bun run worker:deploy
```

Env vars take precedence over CLI flags (note: `bun` loads `.env`
automatically, so a flag cannot override a value that is already in `.env` —
export the env var instead). `/admin/*` and all write endpoints require
`Authorization: Bearer ADMIN_TOKEN`; reads are open.

Harness checks: `bun run typecheck` (tsc over client + Worker) and `bun test`
(`tests/` — MVCC retry/conflict accounting, pool queueing regressions, and the
read/write classification the reporters use).

## Seed

```bash
# Representative academy dataset (deterministic, identical on libSQL + tursodb +
# all D1 DBs). Also seeds concurrent_progress (1..--rows) and hot_counter, and
# verifies kv/concurrent_progress/hot_counter row counts on every side.
# tursodb is seeded when TURSODB_URL/TURSODB_TOKEN are set (otherwise a warning):
bun run seed:lms
# Custom sizes:
bun run seed -- --tenants=100 --students-per-tenant=1000 --courses-per-tenant=20 --lessons-per-course=20 --rows=10000
```

Seed datasets must match for a fair comparison — `seed` writes the same
schema (outside any concurrent transaction; `BEGIN CONCURRENT` is only used for
benchmark application transactions), the same rows, the same payload sizes and
the same randomized ID layout to libSQL, tursodb and D1, then runs `ANALYZE`
(best-effort, reported per target) on all of them. If the Worker has no
`TURSODB_URL`, `seed` warns and skips tursodb; the benchmark refuses to run
`tursodb*` backends in that case.

## New results

### Validation run (2026-09-09, deployed Worker)

Matrix: backends `d1,turso,turso-reused,d1-rr` × tests
`point-read,lesson-page,mixed(lms)` × `single` tenant × concurrency `1,5`;
`iterations=200`, `warmup=30`, `runs=2`, constant load. Seed:
`tenants=10, students-per-tenant=200, courses-per-tenant=5,
lessons-per-course=10, rows=10000`. Server-side db p50 (median across runs),
ms; `rep%` = replica-hit rate; errors 0 everywhere.

| backend | point-read c=1/5 | lesson-page c=1/5 | mixed-lms c=1/5 |
|---|---|---|---|
| d1 (WEUR) | 35 / 35 | 138 / 155 | 137 / 140 |
| turso (fresh client) | 65 / 68 | 169 / 189 | 190 / 181 |
| turso-reused (pooled) | 40 / 39 | 135 / 154 | 133 / 140 |
| d1-rr (sessions) | 44 / 41 (100%) | 168 / 225 (100%) | 165 / 199 (88%) |

Read: pooled Turso reuse removes ~25ms/op of connect+close overhead
(fresh 65ms vs reused 40ms on point-read) — the timing split works as
designed. LMS pages are a 4-query tie between D1 primary and pooled Turso
(~135ms). Sessions served 88–100% from the WEUR replica and cost +10–70ms
vs the primary at this vantage point (primary already nearby).

### Full Manhali benchmark — single-tenant saturation (2026-09-09)

Matrix: backends `d1,d1-eeur,turso-reused,d1-rr` × tests
`lesson-page,mixed(lms)` × `single` tenant × concurrency `1,10,50,200`;
`duration=60s`, `warmup=100`, `runs=3`, constant load, deployed Worker
(`colo MRS`, Turso `aws-eu-west-1`, SDK `@tursodatabase/serverless@1.4.0`).
Seed: `tenants=10, students-per-tenant=200, courses-per-tenant=5,
lessons-per-course=10, rows=10000`. Full per-run data: `results.json` /
`results.csv`. Tables below are server-side db p50 (median across runs), ms.

SINGLE TENANT SATURATION — lesson-page

| backend | c=1 | c=10 | c=50 | c=200 |
|---|---|---|---|---|
| d1 (WEUR) | 149 | 252 | 1004 | 3759 |
| d1-eeur (EEUR) | 138 | 138 | 482 | 1866 |
| d1-rr (sessions, WEUR) | 164 | 197 | 894 | 3478 |
| turso-reused (pooled) | 140 | 142 | 142 | 144 |

SINGLE TENANT SATURATION — mixed-lms

| backend | c=1 | c=10 | c=50 | c=200 |
|---|---|---|---|---|
| d1 (WEUR) | 140 | 217 | 716 | 2679 |
| d1-eeur (EEUR) | 86 | 114 | 303 | 1381 |
| d1-rr (sessions, WEUR) | 146 | 173 | 720 | 1362 |
| turso-reused (pooled) | 153 | 118 | 159 | 164 |

Error rates were 0% up to c=50 (d1-rr mixed: 0.1%); at c=200 errors lift off:
turso-reused 7.6–11.7%, d1-rr 11.9–27.6% (mostly timeouts at the 5s cap).

Read: at low concurrency all primaries tie (~90–150ms LMS pages; EEUR fastest
from MRS). Past c=10 the curves split hard — pooled Turso stays flat
(144–164ms at c=200) while every D1 path degrades 10–25× (up to 3.7s) and
starts timing out. Sessions cost +10–70ms vs the WEUR primary with no
distance to save (replica served 43–100% from WEUR).

### Distributed tenants (not yet run — needs `DB_TENANT_*` bindings)

```text
DISTRIBUTED TENANTS — <date, dims, duration=60s, runs=5>
(paste console output)
```

## Recommended benchmark commands

Single-tenant saturation (primary reported category — latency knee per DB):

```bash
bun run bench -- \
  --backends=d1,d1-eeur,turso,turso-reused,d1-rr,d1-eeur-rr \
  --tests=point-read,scan,lesson-page,mixed \
  --workload=lms \
  --scan-limits=100 \
  --tenant-mode=single \
  --duration=60 \
  --concurrency=1,5,10,25,50,100,200 \
  --runs=5
```
> The recorded results above came from the ~2h subset (`bun run bench:2h`:
> 4 backends × lesson-page/mixed × c=1,10,50,200 × 3 runs). The full command
> above is the reference for a complete saturation sweep.

Distributed tenants (isolated-DB scaling; needs `DB_TENANT_*` bindings):

```bash
bun run bench -- \
  --backends=d1,d1-eeur \
  --tests=lesson-page,mixed \
  --workload=lms \
  --tenant-mode=distributed \
  --tenant-count=10,50,100 \
  --duration=60 \
  --concurrency=50,100,200 \
  --runs=5
```

Turso Database / Concurrent Writes (needs the tursodb database + Worker config):

```bash
# Read comparison: is the new engine different on reads?
bun run bench -- \
  --backends=d1-eeur,turso-reused,tursodb-reused \
  --tests=point-read,scan,lesson-page \
  --workload=lms --tenant-mode=single --scan-limits=100 \
  --duration=30 --concurrency=1,10,50,200 --runs=5 \
  --out=results-tursodb-reads.json

# Write comparison: engine vs transaction mode vs D1.
bun run bench -- \
  --backends=d1-eeur,turso-reused,tursodb-reused,tursodb-concurrent \
  --tests=insert,independent-writes,hot-row-write \
  --workload=lms --tenant-mode=single \
  --duration=30 --concurrency=1,10,50,100,200 --runs=5 --write-retries=3 \
  --out=results-tursodb-writes.json

# Realistic LMS mixed workload (reads + writes) across all four.
bun run bench -- \
  --backends=d1-eeur,turso-reused,tursodb-reused,tursodb-concurrent \
  --tests=mixed --workload=lms --tenant-mode=single \
  --duration=60 --concurrency=1,10,50,100,200 --runs=5 \
  --out=results-tursodb-lms.json

# Focused default sweep (~40 min at --duration=30 --runs=1):
bun run bench:turso-next
```

`--duration` (30–60 s per scenario) is preferred over a fixed iteration count
for saturation curves; raise `--runs` for confidence. Never write results over
the existing historical files: use `--out=results-tursodb*.json` (the runner
writes the matching `.csv` next to it).

Other useful flags:
`--backends=d1,d1-eeur,turso,turso-reused,tursodb,tursodb-reused,tursodb-concurrent,d1-rr,d1-eeur-rr,d1-drizzle,turso-drizzle`
`--tests=point-read,scan,insert,update,student-dashboard,course-page,lesson-page,quiz-page,submit-quiz-answer,update-progress,enrollment,independent-writes,hot-row-write,mixed`
(`scan-100` still accepted as a legacy alias; `tursodb` is an alias for
`tursodb-reused`), `--workload=kv|lms` (mixed preset: kv = 70/15/10/5
reads/scan/updates/inserts; lms = 40 lesson-page, 20 course-page, 15
student-dashboard, 10 update-progress, 10 quiz-page, 5 submit-quiz-answer;
the mixed LMS workload runs unchanged against all four tursodb/D1 backends),
`--scan-limits=10,100,500,1000`,
`--iterations=2000 --duration=0` (duration preferred when set),
`--write-retries=3` (MVCC conflict retries, 0–20),
`--load-model=constant|burst`, `--timeout-ms=5000`, `--warmup=100`,
`--modes=edge,direct` (direct = libSQL only), `--tursodb-url` /
`--tursodb-token` (or `TURSODB_URL`/`TURSODB_TOKEN`),
`--out=results.json` (writes `results.csv` alongside).

## Interpreting results

- Every scenario runs `--runs` times; tables show **medians across runs**
  (D1 varies ±30% run to run). `results.json` keeps every run plus environment
  metadata (colo, row counts per engine, transaction mode, SDK/wrangler
  versions — never tokens); `results.csv` has one row per run for graphing,
  including the MVCC counters (`attemptsTotal`, `committed`, `conflicts`,
  `conflictRate`, `retries`, `retryRate`, `avgRetries`, `successAfterRetry`,
  `failedAfterRetries`; empty for non-MVCC backends).
- Compare **server-side `db p50`** across backends (excludes client→edge
  network, identical for all edge backends); `e2e` adds client network on top.
- Saturation: find where p95/p99 bend upward and where `err%` (grouped by
  `timeout` / `http` / `rate-limit` / `backend` / `conflict`) lifts off — that
  concurrency is the single-DB operating limit, not the c=1 latency.
- The console report separates **READ LATENCY** (grids) from
  **WRITE LATENCY** + **WRITE / TRANSACTION COMPARISON** (mode and MVCC
  counters per backend and concurrency). `tursodb-reused` vs `tursodb-concurrent`
  isolates the effect of `BEGIN CONCURRENT`; `turso-reused` vs `tursodb-reused`
  isolates the engine.
- For `tursodb-concurrent`, read `conflict%` and `retry%` together with
  throughput: retries are inside the measured latency, so a high conflict rate
  with flat p95 means conflicts are cheap, not absent. `hot-row-write` is
  expected to conflict heavily — that is the finding, not a bug.
- The report prints best p50 / p95 / throughput / error rate **per category**
  only. There is no overall winner: reads, writes, low/high concurrency, and
  single vs distributed layouts favor different backends.
- **No tursodb numbers are recorded here yet.** Fill in the tables below only
  from an actual run of the commands above (`results-tursodb*.json`), with the
  database URLs/regions, SDK version, seed dims, duration and runs recorded in
  the same `environment` block.

## Historical results (legacy micro-benchmark, 2026-09-09)

Previous methodology: burst `Promise.all` batches, ~100 iterations,
cache-friendly `scan-100`, trivial KV queries only. Kept for reference; do not
compare directly with new runs.

Server-side DB time p50, ms. `rep%` = share served by read replica.
`e2e` adds ~75–180ms client→edge network on top (same for all backends).

| backend | point-read c=1/10/50 | scan-100 c=1/10/50 | insert c=1/10/50 |
|---|---|---|---|
| d1 (WEUR primary) | 34 / 67 / 212 | 35 / 81 / 432 | 60 / 72 / 226 |
| d1-eeur (EEUR primary) | 32 / 49 / 133 | 33 / 48 / 163 | 46 / 60 / 108 |
| turso (Ireland) | 41 / 38 / 50 | 40 / 45 / 64 | 54 / 145 / 619 |
| d1-rr (sessions) | 42 / 92 / 323 (100/90/98%) | 38 / 100 / 396 (0/90/98%) | 46 / 144 / 551 (0%) |
| d1-eeur-rr (sessions) | 31 / 71 / 189 (0/63/98%) | 33 / 64 / 229 (0/90/98%) | 49 / 127 / 335 (0%) |

Takeaways (legacy methodology):

- Single-shot reads are a tie (~35ms) across all primaries.
- Turso reads stay flat under concurrency; D1 primaries degrade 3–6× at c=50.
- D1 wins concurrent writes (EEUR 108ms vs Turso 619ms at c=50).
- Read replicas only pay off far from the primary: measured from an edge
  near the primaries, replica reads cost more (session overhead, no distance
  saved) — 323ms vs 212ms on WEUR at c=50.
- D1 c=50 numbers vary ±30% run to run; shapes reproduced 3×.

## New results

Run the recommended commands above, then paste the console saturation tables
here. Required context per table: date, seed dims, duration, runs, Worker URL,
colo, Turso/libSQL + tursodb regions/hosts, SDK versions, transaction mode (all
recorded in `results.json` `environment`).

`results*.json` payloads are git-ignored (they carry full per-run data);
this repo commits the matching `results*.csv` one-row-per-run exports and
reproduces the JSON with the commands above.

```text
SINGLE TENANT SATURATION — <date, dims, duration, runs>
(paste console output)

DISTRIBUTED TENANTS — <date, dims, duration, runs>
(paste console output)
```

### Turso Database / Concurrent Writes results

Measured 2026-09-10 from colo `MRS`, Worker `https://bench-turso-vs-d1.manhali-sandbox.workers.dev`
(version `2174c62e`), libSQL `bench-turso-vs-d1` and tursodb `bench-tursodb`, both
group `manhali` / `aws-eu-west-1`, SDK `@tursodatabase/serverless@1.4.0`,
seed `T10/S200/C5/L10, rows=10000`, `--duration=20 --runs=1 --warmup=5`, constant
load, `MRS` client. Raw data: `results-tursodb-reads.json`,
`results-tursodb-writes.json`, `results-tursodb-lms.json`,
`results-tursodb-lms-ops.json`, `results-d1-ceiling.json`,
`results-d1-ceiling-lms.json`;
`results-tursodb-txnasync-broken.*` and `results-tursodb-poolgate-broken.*` keep
the two failure modes described below.

POINT READ (server-side db p50 ms; ok/err)

| backend | c=1 | c=10 | c=50 | c=100 | c=200 |
|---|---|---|---|---|---|
| d1-eeur | 148 (129/0) | 139 (1422/0) | 220 (4658/0) | 363 (5353/0) | 585 (6606/0) |
| turso-reused (libSQL) | 155 (125/0) | 148 (1261/0) | 154 (2555/4366) | 158 (6088/6890) | 180 (12219/7067) |
| tursodb-reused | 142 (138/0) | 146 (1332/0) | **148 (6477/0)** | **152 (12457/0)** | **166 (15930/4070)** |
| tursodb-concurrent | 145 (135/0) | 143 (1377/0) | 154 (6256/0) | 160 (11075/803) | 172 (15345/4655) |

The new engine is the only one that stays flat **and** error-free through c=100
(~150 ms, ~600 RPS, no timeouts); at c=200 it still holds p50 166 ms at 914 RPS
while the 8-connection pool starts queueing past the 5 s op timeout. libSQL
degrades from c=50 (63% timeouts). D1 keeps 0 errors but its latency grows with
concurrency (585 ms p50 at c=200).

WRITES at c=1 / c=10 (server-side db p50 ms, median; `--timeout-ms=15000`)

| backend | tx mode | insert | independent-writes | hot-row-write |
|---|---|---|---|---|
| d1-eeur | transaction | 157 / 165 | 153 / 165 | 154 / 168 |
| turso-reused (libSQL) | none·transaction | 155 / 190 (0.0% err at c=1, 2.0% at c=10) | 158 / 187 | 157 / 198 |
| tursodb-reused | none·transaction | 289 / 820 | 279 / 569 | 307 / 765 |
| tursodb-concurrent | concurrent | 293 / 2294 | 339 / 2240 | 293 / 238 (44.2% err at c=10) |

At c=10 `tursodb-concurrent` (BEGIN CONCURRENT) is **slower than the same engine
without MVCC** (2.2–2.3 s vs 0.57–0.82 s p50) — in this preview concurrent
transactions are not a throughput win at this concurrency. The MVCC counters
work as designed: `hot-row-write` c=10 recorded 1790 conflicts, 1457 retries,
266 operations that succeeded after retrying and 333 that exhausted the retry
budget (44% of ops) — the same-row contention MVCC cannot remove. A separate
40-way burst through the deployed Worker measured 140 conflicts / 32
failed-after-retries in 631 ms for the same test.

MIXED LMS (40% lesson-page, 20% course-page, 15% dashboard, 10% update-progress,
10% quiz-page, 5% submit-quiz-answer), server-side db p50 ms (ok/err)

| backend | tx mode | c=1 | c=10 | c=50 | c=100 | c=200 |
|---|---|---|---|---|---|---|
| d1-eeur | — | 240 (74/0) | 257 (753/0) | 482 (2137/0) | 941 (2163/0) | 1911 (2341/0) |
| turso-reused (libSQL) | — | 264 (72/0) | 265 (572/3) | **279 (3060/68)** | n/a | n/a |
| tursodb-reused | — | 303 (62/0) | 264 (746/0) | 308 (1796/0) | n/a | n/a |
| tursodb-concurrent | concurrent | 301 (60/0) | 340 (546/0) | 1093 (246/83) | n/a | n/a |

Single LMS operations at c=10 (db p50 ms; `results-tursodb-lms-ops.json`)

| op | d1-eeur | turso-reused (libSQL) | tursodb-reused | tursodb-concurrent (MVCC) |
|---|---|---|---|---|
| lesson-page (4 reads) | 290 | 268 | 288 | 269 |
| update-progress (upsert + update) | **230** | 360 | 1023 | 2196 (concurrent txn) |
| submit-quiz-answer (read + insert + update) | **351** | 445 (0 err) | 970 (0 err) | 1888 (12/124 err; 54 conflicts, 42 retries, 12 failed-after-retries, 4 succeeded-after-retry) |

Reads and low-concurrency mixed traffic are a three-way tie (~240–300 ms p50,
0 errors). Writes separate the engines: D1 is 2–8× faster than either Turso
engine, and `BEGIN CONCURRENT` currently *adds* latency (1.9–2.2 s vs 0.97–1.02 s
for the same engine without MVCC) while conflicts on the same quiz-attempt rows
show up as retries and failures. D1's mixed workload stays error-free to c=200
(1.9 s p50) where the Turso backends are already timing out at c=50.

### Known limits of the Turso write path in this preview

Two failure modes were hit while measuring writes, both fixed in the harness and
both worth knowing before trusting any Turso write number:

1. `transactionAsync(...).concurrent()` allocates a server session per
   transaction and Turso Cloud rejects the count
   (`DatabaseError: Database connections limit exceeded`), which then poisons
   even reads on that database until sessions are reaped. The benchmark now uses
   atomic `batch(stmts, "concurrent")` on pooled connections.
2. Turso **writes stall for minutes** under sustained load while reads stay
   fast: a single direct `INSERT` into tursodb measured **144.8 s** (the next two
   inserts: 184 ms and 230 ms; a point read in the same window: 138 ms). With a
   5 s client timeout those stalls surface as aborted requests, and before the
   harness bounded them they left the Worker with requests that never settled
   (Cloudflare cancels hung handlers → `HTTP 500 error code: 1101`). The harness
   now sets `defaultQueryTimeout` on every Turso connection, aborts queued work
   when the client is gone, and recycles a pool slot after 15 s, which turns
   those stalls into ordinary `TimeoutError: Query timed out` responses.

Consequence for reading the write table: at c≥50 the Turso backends are
dominated by stall-driven timeouts, not by steady-state throughput, so only the
c=1/c=10 cells (and the D1 curve through c=200 in `results-d1-ceiling.json`) are
usable. This is a property of the early-preview platform, not of the benchmark:
reads on the same engine and the same database are unaffected.
