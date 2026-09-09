# Turso vs D1 vs D1 read-replica benchmark

Fair latency/throughput comparison between **Turso Cloud**, **Cloudflare D1**,
and **D1 with global read replication**, measured from the same edge vantage point.

## Method

- `d1` / `d1-eeur`: plain D1 binding against the WEUR / EEUR primary.
- `d1-rr` / `d1-eeur-rr`: same databases through the Sessions API
  (`withSession()`), which routes reads to the nearest global read replica
  when read replication is enabled. Each response carries
  `servedByPrimary`/`servedByRegion`; the runner aggregates replica-hit rate
  as `rep%`. Writes still go to the primary.
- `turso`: Turso Cloud primary (`aws-eu-west-1`) over HTTPS Hrana.
The bun runner (`src/bench.ts`) hammers those endpoints and records **end-to-end**
(client→edge→DB) plus **server-side DB time** separately, so client network noise
doesn't pollute the DB comparison. An extra `direct` mode hits Turso straight from
bun for a client-to-cloud reference (D1 has no public HTTP endpoint, so it is
edge-only by construction).

Both primaries live in the same region: Turso `aws-eu-west-1`, D1 `WEUR`.

Tests: `point-read` (PK lookup), `scan-100` (100-row range), `insert` (single write).

## Setup

```bash
bun install
cp .env.example .env   # fill TURSO_TOKEN + ADMIN_TOKEN (any random string)
bun run worker:secret:turso   # TURSO_TOKEN -> Worker env
bun run worker:secret:admin   # ADMIN_TOKEN -> Worker env (same value as .env)
```

Env vars take precedence over CLI flags. `/admin/*` and insert endpoints
require `Authorization: Bearer ADMIN_TOKEN`; reads are open.

Provisioning (already done for Manhali Sandbox):

```bash
turso db create bench-turso-vs-d1 --group manhali
CLOUDFLARE_ACCOUNT_ID=<sandbox> bunx wrangler d1 create bench-turso-vs-d1 --location weur
```

## Run

```bash
bun run worker:dev          # --remote: real D1, not local miniflare
bun run seed -- --rows=10000
bun run bench -- --modes=edge --iterations=100 --concurrency=1,10,50
bun run bench -- --modes=edge,direct --tests=point-read --iterations=200 --concurrency=10

# flags: --backends=d1,d1-eeur,turso,d1-rr,d1-eeur-rr
#        --tests=point-read,scan-100,insert --modes=edge,direct
#        --iterations=N --concurrency=1,10,50 --warmup=10
#        --seed-rows=10000 --out=results.json
```

Results print as a TSV table and land in `results.json` for plotting.

## Results (2026-09-09, 10k rows, 100 iters, deployed Worker)

Server-side DB time p50, ms. `rep%` = share served by read replica.
`e2e` adds ~75–180ms client→edge network on top (same for all backends).

| backend | point-read c=1/10/50 | scan-100 c=1/10/50 | insert c=1/10/50 |
|---|---|---|---|
| d1 (WEUR primary) | 34 / 67 / 212 | 35 / 81 / 432 | 60 / 72 / 226 |
| d1-eeur (EEUR primary) | 32 / 49 / 133 | 33 / 48 / 163 | 46 / 60 / 108 |
| turso (Ireland) | 41 / 38 / 50 | 40 / 45 / 64 | 54 / 145 / 619 |
| d1-rr (sessions) | 42 / 92 / 323 (100/90/98%) | 38 / 100 / 396 (0/90/98%) | 46 / 144 / 551 (0%) |
| d1-eeur-rr (sessions) | 31 / 71 / 189 (0/63/98%) | 33 / 64 / 229 (0/90/98%) | 49 / 127 / 335 (0%) |

Takeaways:

- Single-shot reads are a tie (~35ms) across all primaries.
- Turso reads stay flat under concurrency; D1 primaries degrade 3–6× at c=50.
- D1 wins concurrent writes (EEUR 108ms vs Turso 619ms at c=50).
- Read replicas only pay off far from the primary: measured from an edge
  near the primaries, replica reads cost more (session overhead, no distance
  saved) — 323ms vs 212ms on WEUR at c=50.
- D1 c=50 numbers vary ±30% run to run; shapes reproduced 3×.
