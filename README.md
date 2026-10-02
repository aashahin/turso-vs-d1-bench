# Manhali database-per-tenant benchmark

This benchmark compares Cloudflare D1, Turso/libSQL and Turso Database from the same Cloudflare Worker for a multi-tenant LMS. It measures latency, reliability, idle observations, independent student writes, exam bursts and tenant switching. It reports factual signals, with no weighted overall winner.

**Historical results remain historical.** They do not answer the isolated Turso vs isolated D1 question. Original documentation and interpretations are preserved in [historical-readme.md](docs/historical-readme.md). Existing CSV/JSON files are never overwritten by the runner.

Read the [audit and implementation plan](docs/audit-plan.md), [methodology and setup](docs/methodology.md), and [engineering report](docs/engineering-report.md).

## Backends

| Backend | Engine | Tenant isolation | Execution |
| --- | --- | --- | --- |
| `d1-per-tenant`, `d1-eeur-per-tenant` | D1 | One bound database per tenant | Raw SQL |
| `turso-per-tenant` | libSQL | One manifest database per tenant | Bounded tenant pool |
| `tursodb-per-tenant` | Turso Database | One manifest database per tenant | Same bounded tenant pool |
| `d1-shared`, `d1-eeur-shared`, `turso-shared`, `tursodb-shared` | Corresponding engine | Shared schema | Explicit reference topology |
| `d1-per-tenant-drizzle`, `d1-eeur-per-tenant-drizzle`, `turso-per-tenant-drizzle` | Corresponding engine | Per tenant | SQL composition overhead, not a full ORM |
| `d1-per-tenant-rr`, `d1-eeur-per-tenant-rr` | D1 | Per tenant | Sessions/read replication |
| `tursodb-per-tenant-concurrent` | Turso Database | Per tenant | Experimental concurrent transactions with measured retries |

Legacy names remain accepted. A legacy distributed D1 selection becomes strict per-tenant routing; legacy Turso selections remain shared and are labeled accordingly in metadata. Direct mode is a per-request libSQL reference from the runner and is excluded from decision comparisons. D1 EEUR is a binding family name, not proof of actual database location.

## Setup and checks

```bash
bun install --frozen-lockfile
bun test
bun run typecheck
```

Configure disposable databases, per-tenant Worker bindings/manifests and `ADMIN_TOKEN` as described in [setup](docs/methodology.md#setup). `bun run worker:deploy` embeds build provenance before deploying. Seeding changes benchmark tables and must finish before measurements. Topology validation is mandatory and authenticated, including read-only runs.

For the canonical run, seed 100 tenants with 500 students each (only the requested tenant's LMS rows reside in each isolated database):

```bash
bun run seed --backends=d1-per-tenant,turso-per-tenant,tursodb-per-tenant --tenant-count=100 --tenants=100 --students-per-tenant=500
bun run bench:manhali
```

The curated command uses five runs, eight-second ordinary load windows and one idle observation per run/backend. It needs 100 configured databases **per selected engine**. Select just configured backends with `--backends=...`. It covers baseline reads, balanced LMS at c=1/10/50/100, writes at c=1/10/50, exams with 50/100/250 students, 10/50/100-tenant churn with uniform/hotset traffic, 10/25/50/100/200 RPS arrivals, and 30-second idle observations. Validation, drains and idle waits add wall time; it is a decision suite, not an instant smoke test.

`bun run bench:manhali:full` expands load windows to 30 seconds, repeats to ten, churn up to the configured tenant count and Zipf, exams to 500 students, arrival rates to 500 RPS, and idle intervals to 30s/2m/5m with all variants. The default requires matching 500-tenant/500-student seeds and can take many hours. Idle intervals and matrix dimensions remain independently configurable through the ordinary `bench` command.

`bun run bench:manhali:6h --reseed=true` uses 30 already-provisioned isolated databases per engine, five repeats, 30-second windows, and only 30-second idle intervals. It keeps the extensive workload, concurrency, arrival-rate, exam and distribution coverage. Setup and validation count toward the six-hour deadline; provisioning stays separate. Reseeding resets the selected disposable benchmark databases before timing. Results record the budget and deadline, and a budget expiry saves only completed checkpoint records with `partial: true`; outcomes of uncheckpointed operations are explicitly unknown. Run the supervisor under a persistent host service for execution independent of the chat session or terminal.

## Focused live commands

These assume `WORKER_URL`, `ADMIN_TOKEN`, deployed mappings and matching seed dimensions. Bun loads `.env`; environment variables take precedence over flags. Unset conflicting benchmark overrides before using a command. Every invocation gets a unique result filename by default; an explicit existing `--out` is rejected.

```bash
# Quick smoke against one configured per-tenant backend
bun run bench --backends=d1-per-tenant --tests=point-read --iterations=20 --concurrency=1 --warmup=0 --runs=1 --tenants=100 --students-per-tenant=500

# Curated production-decision suite
bun run bench:manhali

# Extensive suite (seed/configure 500 tenants first)
bun run bench:manhali:full

# Execute on an actual Saudi runner; labels do not move the runner
bun run bench:manhali --runner-label=saudi --runner-region=SA

# D1 vs libSQL per-tenant only
bun run bench:manhali --backends=d1-per-tenant,turso-per-tenant

# Idle observations only; longer intervals are optional
bun run bench --backends=d1-per-tenant,turso-per-tenant --suites=idle-start --tenant-mode=distributed --tenant-count=10 --tenants=100 --students-per-tenant=500 --idle-intervals-ms=30000 --idle-variants=idle-same-row,idle-different-row,idle-same-tenant,idle-different-tenant --cold-samples=1 --runs=5

# Tenant switching only
bun run bench --backends=d1-per-tenant,turso-per-tenant --suites=tenant-churn --tenant-mode=distributed --tenant-count=10,50,100 --tenants=100 --students-per-tenant=500 --tenant-distribution=hotset --duration=10 --iterations=100000 --concurrency=50 --runs=5

# Exam bursts only
bun run bench --backends=d1-per-tenant,turso-per-tenant,tursodb-per-tenant --tests=exam-burst --exam-students=50,100,250 --tenants=100 --students-per-tenant=500 --runs=5

# Independent offered-load sweep; no completion-driven refill
bun run bench --backends=d1-per-tenant,turso-per-tenant --suites=saturation --tests=lms-balanced --load-model=arrival-rate --target-rps=10,25,50,100,200 --max-in-flight=1000 --duration=10 --iterations=100000 --tenants=100 --students-per-tenant=500 --runs=5
```

All new artifacts contain source/deployment provenance, arguments without tokens, topology, seed/schema evidence, runtime/SDK versions, timeout/pool settings and source-location labels. JSON retains per-run observations and idle trials; CSV carries one row per run; `-comparison.json` provides machine-readable Manhali signals. Unknown regions and insufficient samples are explicit.

No new live vendor comparison has been measured as part of this implementation. Run these commands from the intended origins before making the database selection.
