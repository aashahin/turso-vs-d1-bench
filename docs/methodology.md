# Measurement contract

## Setup

Use disposable benchmark databases. Do not point the seed endpoint at academy databases: it clears the benchmark's application tables. Provisioning, schema creation, seeding and validation occur before measured windows.

D1 per-tenant modes require `DB_TENANT_1...N` or `DB_EEUR_TENANT_1...N` in `worker/wrangler.jsonc`. Explicit shared modes use `DB`/`DB_EEUR`. Missing isolated bindings fail. Preflight reads a persistent `bench_identity` stamp from each database and rejects two tenant bindings that reach the same physical stamp, even if the binding names differ. Each measured D1 response carries its validated binding identifier; the artifact maps that identifier to its physical stamp. No identity lookup is added to timed SQL.

Turso per-tenant modes require JSON **secrets** `TURSO_TENANTS` and `TURSODB_TENANTS`:

```json
{
  "1": {"databaseId":"unique-physical-stamp-1","url":"https://tenant-1.example.invalid","token":"SET_PRIVATELY","engine":"libsql","region":"operator-verified-region"},
  "2": {"databaseId":"unique-physical-stamp-2","url":"https://tenant-2.example.invalid","token":"SET_PRIVATELY","engine":"libsql","region":"operator-verified-region"}
}
```

Use `engine: "tursodb"` for `TURSODB_TENANTS`. Tokens stay in Worker secrets and private ignored setup files. URL origins and stamp IDs must be unique within a manifest. Seed stamps are checked against mappings. Shared Turso modes use the existing `TURSO_URL/TURSO_TOKEN` or `TURSODB_URL/TURSODB_TOKEN`. Manifests do not put every academy's data into every isolated database.

```bash
# Plan only: no remote changes
bun run provision --engine=d1 --prefix=bench-manhali-d1 --count=100 --location=weur
bun run provision --engine=libsql --prefix=bench-manhali-libsql --count=100 --group=YOUR_TEST_GROUP
bun run provision --engine=tursodb --prefix=bench-manhali-tursodb --count=100 --group=YOUR_TEST_GROUP

# Explicit provisioning: journals resources privately, does not seed/deploy
bun run provision --engine=d1 --prefix=bench-manhali-d1 --count=100 --location=weur --apply=true --manifest-file=bench-private-d1.json
bun run provision --engine=libsql --prefix=bench-manhali-libsql --count=100 --group=YOUR_TEST_GROUP --apply=true --manifest-file=bench-private-libsql.json
bun run provision --engine=tursodb --prefix=bench-manhali-tursodb --count=100 --group=YOUR_TEST_GROUP --apply=true --manifest-file=bench-private-tursodb.json
```

Merge emitted D1 bindings into the Worker config. Turso helpers write separate private `*-worker-secret.json` files with the exact manifest shape. Upload them through stdin without printing their contents:

```bash
bun run worker:secret:tenants < bench-private-libsql-worker-secret.json
bun run worker:secret:tursodb-tenants < bench-private-tursodb-worker-secret.json
bun run worker:secret:admin
bun run worker:deploy
```

The provisioning helper uses the installed Wrangler and Turso CLI; no benchmark creation command has been executed during implementation. It creates Turso tokens with seven-day expiration. Preserve journals for cleanup and account for plan quotas. Partial failures are not silently retried; inspect the journal/vendor dashboard before replacing resources. Cleanup requires both flags and validates the disposable prefix:

```bash
bun run cleanup --engine=libsql --prefix=bench-manhali-libsql --manifest-file=bench-private-libsql.json --apply=true --confirm-delete=bench-manhali-libsql
```

Repeat with the matching D1/Turso Database journal and prefix. Cleanup does not redeploy/remove Worker mappings: update those separately. Seeding and live benchmark commands are in the [README](../README.md).

## Topology and schema validation

Every edge invocation first validates all selected targets, sequentially and outside timing, through authenticated `/admin/validate-target`. It checks persistent identity, manifest/binding routing, engine attestation, schema version, canonical SHA-256 schema/index fingerprint, deterministic seed dimensions, immutable row counts, minimum enrollment rows, equal starting table counts across comparable backends, absence of other tenants in isolated users, deployment/build provenance and SDK versions. It records mutable table counts too. Region unknown is a warning, not a fabricated location. D1 query-serving-region metadata is collected when available; Turso regions come from operator attestations.

`schema.sql` and its Worker-safe compiled constant are parity tested against real local SQLite. A changed table/index/constraint changes the fingerprint. All engines use the **same seeder** and global arithmetic IDs. Isolated databases contain one tenant's LMS rows; every database retains the same KV/concurrency dimensions. Timestamps, payloads, lesson titles/bodies and ID derivation are identical. Every student is also enrolled in the first course for the common exam scenario, in addition to the usual deterministic course pair. A one-course dataset avoids duplicate enrollment insertion.

Engine identification is a provisioning/seed attestation plus `sqlite_version()` observation, not independent proof from an engine-specific introspection API. `sqliteCompatibilityVersion` records that function result; the actual vendor `engineVersion` is unknown/null. Do not falsely label a libSQL database as Turso Database in a manifest. Bootstrap confidence intervals do not correct a falsely attested topology.

Preflight necessarily warms Worker/database infrastructure. Idle experiments begin their silence interval after this phase. Immutable topology evidence is captured once per invocation; writes mutate application rows during and across repetitions. Reseed separate invocations for comparisons requiring identical mutable starting state. Identity/schema/configuration changes during a run invalidate the comparison.

## Tenant pools

One LRU cache per engine/configuration per Worker isolate holds lazily created tenant pools. It caps active pools, clients per tenant and total clients, including closing/quarantined clients. It checks idle TTL on checkout instead of relying on background timers in a suspended isolate. Busy pools are never evicted. Queue aborts remove waiters; failures retire clients; stalled/aborted work is quarantined until it settles and can close safely. Capacity loss remains visible. Retirement and late cleanup promises are attached to the originating invocation through `ctx.waitUntil`; the platform background grace period still bounds cleanup after a disconnect. Rejected closes retain their capacity reservation and increment close-failure/quarantine counters.

Configure the Worker `TURSO_POOL_CONFIG` JSON var, for example:

```json
{"sizePerTenant":8,"maxActivePools":100,"idleTtlMs":60000,"maxTotalConnections":128}
```

The runner's corresponding flags are `--turso-pool-size-per-tenant`, `--turso-max-active-pools`, `--turso-pool-idle-ttl-ms` and `--turso-max-total-connections`. Preflight rejects mismatches. Changing configuration requires deploying a fresh isolate/version; the cache does not accumulate arbitrary configurations inside one isolate.

`connect()` is allocation only in serverless SDK 1.4.0. First-query network/TLS/session setup remains inside query timing. The SDK connection has one serialized stream, no caller-provided AbortSignal on query methods, and `close()` does not lock against ongoing query work. Those facts motivated quarantine rather than pretending a watchdog canceled the backend. Closed counts mean SDK close completion, not verified server session reaping. Caches are per isolate, never a service-wide connection cap.

Per-operation events record pool hit/miss, client allocation/reuse and evictions. Cumulative snapshots keyed by observed isolate record created/reused/closed clients, close failures, waits, allocation time, active/peak pools, active/peak clients and quarantined clients. Snapshots carry a monotonic sequence per pool manager so an older network response cannot replace a newer observation. Do not sum cumulative snapshots across requests/scenarios. Asynchronous close completions are reported in snapshots; the per-request `connectionClosed` event is zero because completion attribution to that request is unavailable. Churn rates use observed request events; requests whose response was lost can leave incomplete telemetry. No heap-memory estimate is invented.

## Load scheduling and traffic

`constant` remains an alias for **closed-loop** concurrency. Each completed operation frees a slot for another. `burst` launches fixed concurrent batches. Both are useful concurrency experiments, but neither guarantees an offered arrival rate.

`arrival-rate` schedules each request at `start + index / targetRps`, independent of completion. `--max-in-flight` bounds runner-observed operations; an arrival is dropped when that cap is occupied or its start delay exceeds `--max-start-lag-ms`. Drops do not postpone subsequent arrivals. Absolute scheduling avoids the completion-driven coordinated omission in the old load model. It reports scheduled/started/completed/dropped/late starts, start delay and peak in-flight. A late start exceeds 1ms; the delay distribution includes every offered arrival. Offered counts include admission drops; started operation counts do not.

Duration-based arrivals offer `min(iterations, ceil(duration * targetRps))` requests. Raise `--iterations` sufficiently for a duration sweep. `BUDGET_LIMITED_WINDOW` warns when the iteration cap shortens the requested window. The scheduler keeps the offering window through the final interval, then drains admitted work. Successful RPS is successes divided by total measured wall time, including drain; scheduled RPS and offered window are recorded separately. It is a fixed-rate schedule, not Poisson traffic or a hardware-grade load appliance.

`uniform`, `hotset` (default 10% tenants/80% traffic when selected) and Zipf (`--zipf-exponent=1.1`) are deterministic by seed/index. Hash salts separate tenant selection from operation selection. Counts and top-10%-tenant/max-tenant shares verify observed concentration. Tiny tenant counts necessarily round the hotset upward.

## LMS profiles and writes

Percentages below sum to 100. `--lms-mix` accepts a JSON map of integer percentages and supports the known LMS operation names. A permuted 100-operation deck gives exact offered counts for full blocks; a partial tail is explicitly reported. Arrival drops may change the started mix and are visible separately.

| Operation | Read-heavy | Balanced | Write-heavy |
| --- | ---: | ---: | ---: |
| lesson-page | 50 | 40 | 10 |
| course-page | 25 | 20 | 0 |
| student-dashboard | 15 | 15 | 5 |
| update-progress | 4 | 10 | 35 |
| quiz-page | 5 | 10 | 0 |
| submit-quiz-answer | 1 | 5 | 30 |
| enrollment | 0 | 0 | 10 |
| order-event | 0 | 0 | 10 |

`lms-balanced` preserves the original requested mix. `mixed --workload=lms` uses it too. Progress stores a percentage, not a completed-row count. Multi-statement LMS writes now use atomic D1 batch / Turso immediate batch equivalents; attempt resolution remains outside the batch on both because D1 has no interactive transaction. This lookup/create is included in timing and statement-attempt counters. It can race for the same student; exam students are distinct within a burst.

`exam-burst` launches one common-course/quiz submission per distinct student, with progress and answer writes in the same atomic operation. There is no shared aggregate row. Total completion time, successes, failure classes, retry/conflict counters and latency are retained. `hot-row-write` remains an adversarial single-row contention benchmark. Enrollment events are idempotent and may perform no insert on existing enrollments; order events insert real metadata rows. The lightweight workload is not the complete Manhali business application, payment system or relational ORM.

## Timing definitions

| Field | Timer interval / limitation |
| --- | --- |
| `e2eMs` | Runner executor start, before URL/body construction, through complete response body and JSON parsing. Contains runner/network/platform latency. |
| `scheduledLatency` | Successful e2e plus recorded arrival start delay; schedule-to-completion observation. Separate from successful e2e. |
| `runnerToEdgeMs` | Null: one-way delay cannot be measured without synchronized clocks. |
| `workerHandlerMs` | Handler entry through query completion and response payload assembly, captured before JSON serialization. No platform startup before handler entry. |
| `tenantRoutingMs` | Null on the Worker: synchronous routing CPU cannot be isolated with its clock. |
| `poolCheckoutMs` / `checkoutMs` | Wait plus lookup/eviction and client allocation through acquiring a slot. |
| `connectionCreationMs` | Null on the Worker; SDK allocation CPU cannot be isolated. Direct Bun reference can record allocation, never TCP/TLS establishment. |
| `dbExecutionMs` / `queryMs` | SQL operations, Worker-to-database transport, result decoding, attempt resolution and retries/backoff. Not engine CPU time. |
| `dbMs` | `runOp` wall time, including pool checkout or fresh-client close plus query work. |
| `totalServerMs` | Handler entry through metadata JSON serialization, carried in `X-Bench-Total-Server-Ms`. Response transfer follows this timer. |

`Server-Timing` repeats handler/query clock spans. These intervals overlap; do not add them. Deployed Worker clocks advance only after I/O, so even the I/O-spanning intervals cannot precisely attribute synchronous CPU around their endpoints. [Cloudflare performance/timer documentation](https://developers.cloudflare.com/workers/runtime-apis/performance/) `serializationMs` is null/unavailable on the Worker. Its frozen synchronous clock cannot isolate response JSON/Response construction CPU. `buildMs`, `clientCreationMs` and pool `connectionCreateMs` are also null in Worker responses. Responses serialize benchmark metadata/counts, not a full LMS page. D1 exposes rows-read/written counters through all/run; Turso rows-written counters are collected when returned. Unknown rows-read/storage values remain null/absent. Statement-attempt counts include resolution and retries but exclude internal BEGIN/COMMIT/probe/close statements; they are not a vendor bill.

## Timeouts and reliability

`--timeout-ms` is the same runner and Worker operation budget for every backend. Turso query/check-out budgets are bounded by it. Transactions, retries and backoff are inside the Worker operation deadline. D1 has no application AbortSignal execution cancellation, so its timer bounds observation rather than proving server cancellation. Turso's SDK owns fetch timeout signals; request aborts stop future work where checked and quarantine current work. A timed-out write may commit; no automatic transport-timeout retries are introduced.

Failure classes include timeout, HTTP/backend, connection, rate-limit, conflict, constraint and application errors. Timeout phase counters separate runner, Worker, query, pool checkout and pool operation where evidence survives. Lost/late responses can hide the server's failure phase; runner timeout is then the observable classification.

Successful latency samples exclude failures. Reliability includes every started operation; offered failure rate additionally includes dropped arrivals. No-latency failures are never deleted. Transaction retry exhaustion counts as a conflict failure. The report also exposes drops and observed operation counts. Raw errors are withheld to protect credentials.

## Statistics and decision signals

Default repetitions are five; ten is supported. Run-level medians, minimum/maximum, IQR, coefficient of variation and a deterministic 2,000-resample median bootstrap 95% interval (at least five runs) are reported. Failed runs contribute zero throughput and their real error rates, but no invented latency. Median error rate and pooled error rate are distinct.

Serialized JSON/CSV suppress p99 below 1,000 successes per run; legacy in-memory nearest-rank statistics remain available to existing regression tests. Even 1,000 observations are a minimum screening threshold, not a precise tail estimate. `INSUFFICIENT_SAMPLES`, `HIGH_VARIANCE` (CV > 0.2), `HIGH_ERROR_RATE` (>=1%), `OFFERED_LOAD_NOT_ACHIEVED`, `POOL_SATURATED`, `WORKER_LIFECYCLE_CHANGED`, `BACKEND_REGION_UNKNOWN` and exploratory-run warnings qualify results. Invalid topology aborts instead of being ranked.

Saturation is an arrival-rate suite with tested-point evidence: >=1% errors, offered-load loss, runner queue p95 >10ms, p95/p99 >2x the lowest-load baseline, or <=5% throughput gain despite >20% offered-load increase. These documented thresholds are diagnostics, not subjective weights or an assumed production SLA. The highest stable **tested** offered rate is reported; nothing beyond the tested range is extrapolated.

Machine-readable comparison groups keep topology, ORM access layer, tenant count, distribution, operation mix, load model/rate, concurrency and timeout equivalent. They report lowest scenario/balanced/write/exam p95, lowest observed error rate, idle p50/p95 and churn observations, without an overall winner. Direct results are excluded. Bootstrap CIs share database state/network conditions and do not guarantee independent observations.

## Idle and source location

`cold-start` is a retained CLI alias, reported as first request after idle. No wait guarantees isolate eviction, cache flush, connection replacement or backend cold start. The raw lifecycle identifies observed first/reused/unknown isolates; pool events distinguish newly allocated/reused clients and LRU-present/absent tenant pools. A new SDK object does not prove a new physical socket.

Variants hold the same row, switch to another row, vary rows within the same tenant, or switch tenants for followups. Trials record the prior runner tenant and whether the first tenant matches it. Other traffic may touch the same Worker/database during runner silence. Trials are sequential, so later followups have ordering/cache effects; they do not causally isolate page cache from socket/server cache.

`--runner-label` and `--runner-region` are explicit operator metadata. `runnerHost` is observed; `colo` is Worker-reported per response, not inferred from a hostname. A Saudi label only represents Saudi traffic when the process actually runs there. Use separate artifacts from real Saudi/MENA origins. The repository provisions no Saudi runner and contains no new live performance claims.


## Operational scale beyond this matrix

Static D1 bindings are intentionally the production comparison path here. Cloudflare documents an approximately 5,000-binding limit per Worker script and larger account database limits; thousands/tens of thousands of academies therefore also need a separately evaluated deployment/sharding/routing strategy. This benchmark does not implement a shard dispatcher or use D1 REST calls as a silent substitute for Worker bindings. See [D1 limits](https://developers.cloudflare.com/d1/platform/limits/) and [binding APIs](https://developers.cloudflare.com/workers/runtime-apis/bindings/). A 500-tenant run cannot establish control-plane provisioning, migration, backup, recovery or credential-rotation feasibility at 50,000 tenants.


## Live-run setup updates

The extensive preset now respects `--tenants`: `--tenants=100` caps churn at 10/50/100 databases without reducing repeats, durations or idle intervals. A `-checkpoint.json` companion records partial progress; it is explicitly running/complete rather than a substitute for final results.

Worker secrets have a 5 KB value limit. Large isolated manifests can use contiguous `TURSO_TENANTS_0...` and `TURSODB_TENANTS_0...` JSON chunks, each containing complete tenant mappings. Gaps and duplicate tenant/database mappings fail validation. Values remain encrypted Worker secrets. [Workers limits](https://developers.cloudflare.com/workers/platform/limits/)

Live D1 initialization uses prepared DDL batches because its exec path splits SQL at line boundaries. D1 blocks `sqlite_version()`; that optional observation is null there. Schema fingerprints exclude the verified provider-owned `_cf_` and `__turso_internal_` objects and continue checking all benchmark tables/indexes/constraints. Provider schema entries remain in topology evidence.
