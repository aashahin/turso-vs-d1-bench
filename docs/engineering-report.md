# Manhali benchmark engineering report

Audit baseline: `7b1b2c6915bd442de8f9b0662bc70176834ff133`. Implementation date: 2026-10-01. This report records the initial local audit and validation, before live provisioning and deployment. Subsequent work provisioned isolated databases, deployed the Worker, and completed the time-limited live suite. See [the live-run update](live-run-20261002.md) for the current deployment, final results, setup recovery and latest checks. Historical result files were preserved.

## A. Problems found and fixes

| Severity | Location | Why it distorted results | Implemented correction |
| --- | --- | --- | --- |
| Critical | `worker/src/tenant.ts` | D1 could silently use a shared fallback; Turso was always shared | Explicit shared/per-tenant names; missing mappings fail; physical stamps validated before measurement |
| Critical | `src/runner.ts` | Completion-driven “constant” concurrency hid offered-load loss | Preserve closed loop; add independent absolute arrival schedule, drops, delays and schedule-to-completion statistics |
| High | `worker/src/backends.ts` | Single URL/token pool did not model tenant switching; replaced clients were discarded | Bounded per-engine tenant LRU with lazy slots, idle eviction, global cap and credential identity |
| High | `worker/src/pool.ts` | Replacing still-running clients could exceed physical lifetime assumptions | Quarantine outstanding work; retain capacity through close; report rejected closes; own cleanup with `ctx.waitUntil` |
| High | `src/bench.ts` | Metadata validation came after measurement | Mandatory authenticated preflight before the invocation's workload; verify routing and deployment fingerprint in measured responses |
| High | `src/seed.ts`, `worker/src/seed.ts` | Duplicate seed implementations differed in lesson text and timestamps; isolated D1 had all tenants | One seeder for all engines; per-tenant subset, deterministic timestamps/data; one-course uniqueness fix |
| High | `shared/sql.ts`, `worker/src/queries.ts` | LMS progress stored completed-row count as a percentage; multi-statement write atomicity differed | Correct percentage calculation; common atomic write batches; same SQL/bind registry in edge/direct paths |
| High | `worker/src/backends.ts`, `worker/src/index.ts` | Runner 5s, query 10s and pool 15s could leave backend work beyond runner failures | Same operation budget in runner/Worker; query and checkout budgets validated; timeout phases retained |
| High | `src/exec.ts` | Direct submit path inserted an attempt even after finding an existing attempt | Insert only when lookup misses; direct SQL uses the edge registry; close on failure as well as success |
| High | `src/bench.ts` | Result output could replace historical data | Unique default names, pre-run collision check and exclusive creation of all companions |
| Medium | `src/workloads/lms.ts`, `src/workloads/profiles.ts` | Stochastic operation mix was unverified and no exam profile existed | Exact deterministic 100-operation blocks; offered/started counts; read-heavy/balanced/write-heavy profiles; independent exam students |
| Medium | `src/workloads/distribution.ts` | Uniform traffic did not represent idle/uneven academies | Uniform, hotset and Zipf selectors; seed-controlled replay; actual concentration/counts |
| Medium | `worker/src/timing.ts` | Deployed Worker clocks cannot isolate synchronous CPU; zero routing/allocation/ORM/serialization durations would mislead | Mark these values unavailable/null and record clock semantics; keep useful I/O spans and end-to-end latency |
| Medium | `src/cold-start.ts` | Same-row followups entangled row cache, connection and tenant warming | Preserve observational lifecycle pairing; add row/tenant variants, intervals, previous-tenant and LRU/client evidence |
| Medium | `src/results.ts`, reporters | Small-sample p99 and little repeated-run dispersion could imply certainty | Serialized p99 null below 1,000 successes/run; median/min/max/IQR/CV and run bootstrap CIs; explicit warnings |
| Medium | `src/reporters/manhali.ts` | Comparison grouping could confuse topology/ORM/load dimensions | Comparable groups, factual latency/reliability/churn signals and documented saturation diagnostics; no overall score |
| Medium | `worker/src/cost.ts` | D1 first-row helpers hid provider rows-read/write counters | Preserve those counters with equivalent all()/first-row execution; report observed statement attempts and unknown values honestly |
| Medium | environment/report metadata | Origin, engine evidence and deployed source were incomplete | Runner labels/host/region; observed colo; attested database regions/engine, SQLite compatibility version, source/deployment/schema fingerprints, SDK/Bun/Wrangler versions |
| Medium | runtime errors/reporting | Raw SDK/server error text could disclose secrets | Safe Worker errors and redacted runner/conflict samples; private setup files; no tokens in results |

Integration review also fixed over-eviction while a previous client was still closing, shared Turso identity checking, isolated Drizzle accidentally taking a shared/fresh-client route, and erroneous tenant filtering of the non-tenant `bench_writes` table. Real SQLite and installed-SDK network-boundary tests protect those integrations.

## B. Methodology changes

A scenario now declares engine, tenant isolation, database count, routing strategy, access layer, distribution, load model, offered rate, concurrency and timeout policy. Historical shared-schema results are never relabeled as isolated results. Legacy interfaces remain available, but distributed D1 now fails closed.

Latency, saturation, reliability, idle, churn, writes, read-heavy and mixed LMS suites are explicit. Saturation uses open-loop offered load and reports observed error/latency/queue/plateau evidence. Hot-row writes remain an adversarial benchmark, outside the primary canonical LMS suite. Direct runner-to-libSQL measurements remain reference-only.

Failed operations contribute to reliability even without latency. Successful latency remains separate; offered failure rate additionally includes admission drops. Run medians and dispersion expose instability instead of reducing it to an overall winner. The canonical suite defaults to five repetitions; full uses ten. Source labels never imply that a process moved to Saudi Arabia.

See [methodology.md](methodology.md) for timer endpoints, exact profile percentages, timeout semantics, statistical thresholds and caveats.

## C. Architecture

- **D1 routing:** an explicit binding for each isolated tenant. Preflight reads distinct persistent database stamps and schema/seed evidence. Measured responses identify the validated binding without adding an identity query to every timed operation.
- **Turso routing:** cached immutable secret manifests map tenants to distinct URL/token/stamp records. Isolated raw and composition-only Drizzle routes use the appropriate physical tenant pool. Both libSQL and Turso Database use the installed serverless SDK.
- **Pooling:** one bounded LRU per engine/configuration/isolate; lazy clients per tenant; safe idle/LRU eviction; bounded waits; failure retirement; stalled-work quarantine. Events and isolate-keyed snapshots expose churn. Total caps include clients awaiting close/quarantined work. Closures whose completion cannot be attributed to an operation remain cumulative observations.
- **Scheduling:** absolute offered arrivals independent of completions. Admission caps and lateness drops preserve the next scheduled arrival; late starts and queues are recorded. Existing concurrency and burst execution remain available.
- **Idle:** validation first, then configured runner silence, then controlled row/tenant variants. Worker lifecycle, SDK allocation/reuse and LRU membership are independent observations. No infrastructure cold-start guarantee is claimed.
- **Schema/seed:** shared SQL and one deterministic seed implementation. Schema version and SHA-256 table/index fingerprints are checked. Provisioning helpers are separate, plan-only by default, with explicit apply/cleanup flags.
- **Provenance:** `worker:deploy` embeds build/source/SDK metadata and uses Cloudflare's version metadata binding. Preflight rejects mixed deployments; measured responses are checked against the validated deployment/source fingerprint.

## D. Tests and validation

New test files:

- `tests/abort-phases.test.ts`: aborting a quiz lookup prevents later attempt creation/write batches for raw/session D1.
- `tests/tenant-pool.test.ts`: LRU reuse/eviction, idle and busy slots, global caps, queued/active aborts, failed/stalled work, late cleanup, cross-tenant identity, shutdown and rejected-close capacity.
- `tests/arrival-and-traffic.test.ts`: independent arrivals under stalled work, admission drops, deterministic late starts, schedule-to-completion/failure accounting, uniform/hotset/Zipf replay and concentration, exact profile mixes and distinct exam students.
- `tests/topology-and-schema.test.ts`: physical D1 separation, manifest routing, duplicate identities, schema/index/seed/engine/tenant/provenance rejection, schema parity, real SQLite seeding and LMS/exam SQL.
- `tests/reporting-and-idle.test.ts`: history preservation, partial companion cleanup, p99 suppression, deterministic bootstrap, failed-run reliability, equivalent comparison grouping, saturation evidence, secret redaction and idle variants.
- `tests/plan-and-mix.test.ts`: interleaved/rotated repetitions and custom LMS mix scoping, so atomic/ KV scenarios cannot be silently replaced.
- `tests/sdk-routing.test.ts`: the installed SDK executes raw and Drizzle requests against the correct tenant origin/token, reusing only that tenant's pool. Only the HTTP boundary is faked.
- `tests/sqlite.ts`: local SQLite adapter shared by integration tests; no physical vendor databases required.

Existing protections were preserved. `tests/edge-and-cli.test.ts` now requires preflight before idle and validates the new separate artifacts; its localhost subprocess integration remains. `tests/mvcc-retry-and-pool.test.ts` retains conflict/retry/queue regression cases and now expects queued requests to honor their own timeout before subsequent recovered requests use fresh clients. `tests/runner.test.ts` adds persistent topology-invalid detection beyond the error-message sample cap and warnings for operation budgets that shorten duration-based offered windows. Other existing tests retain their assertions, with formatting changes only.

Final checks:

- `bun test`: **152 passed, 0 failed**.
- `bun run typecheck`: **passed**.
- Additional `--noUnusedLocals --noUnusedParameters`: **passed**.
- `bun run worker:deploy --dry-run --outdir=/tmp/manhali-worker-build`: **passed**, bundle only; no deployment.
- Disposable provisioning plan command: **passed**, no remote changes.

The sandbox does not permit the CLI integration test's localhost listener. The full tests were run with approved escalation. An initial attempt to invoke Wrangler directly under Bun failed in esbuild; the repository script, which invokes Wrangler's supported executable, completed the dry run.

## E. Live benchmark commands

These assume a deployed Worker with the requested isolated resources and matching 100-tenant/500-student seed. Set `WORKER_URL` and `ADMIN_TOKEN` privately. Environment variables take precedence over flags. Every command generates unique outputs unless `--out` is provided; existing outputs are rejected.

```bash
# Quick smoke
bun run bench --backends=d1-per-tenant --tests=point-read --iterations=20 --concurrency=1 --warmup=0 --runs=1 --tenants=100 --students-per-tenant=500

# Canonical Manhali suite
bun run bench:manhali

# Full suite: provision/seed 500 tenants first
bun run bench:manhali:full

# Execute this on an actual Saudi-origin host
bun run bench:manhali --runner-label=saudi --runner-region=SA

# D1 vs libSQL per-tenant only
bun run bench:manhali --backends=d1-per-tenant,turso-per-tenant

# Idle only
bun run bench --backends=d1-per-tenant,turso-per-tenant --suites=idle-start --tenant-mode=distributed --tenant-count=10 --tenants=100 --students-per-tenant=500 --idle-intervals-ms=30000 --idle-variants=idle-same-row,idle-different-row,idle-same-tenant,idle-different-tenant --cold-samples=1 --runs=5

# Tenant churn only; repeat separately with uniform/zipf for comparable origins
bun run bench --backends=d1-per-tenant,turso-per-tenant --suites=tenant-churn --tenant-mode=distributed --tenant-count=10,50,100 --tenants=100 --students-per-tenant=500 --tenant-distribution=hotset --duration=10 --iterations=100000 --concurrency=50 --runs=5

# Exam only
bun run bench --backends=d1-per-tenant,turso-per-tenant,tursodb-per-tenant --tests=exam-burst --exam-students=50,100,250 --tenants=100 --students-per-tenant=500 --runs=5
```

The [README](../README.md) also gives an open-loop saturation command. [Setup](methodology.md#setup) gives provisioning, secret upload, seeding and guarded cleanup commands. The canonical suite requires 100 physical databases per selected engine; full requires 500. Unit tests use local SQLite, fakes and controlled clocks.

## F. Remaining limitations

1. **No new live performance evidence.** Local tests and bundle validation establish harness behavior, not vendor latency/reliability. The repository's current Worker config lacks isolated D1 tenant bindings. Configure/provision/seed the requested topology before measuring; old results do not substitute for it.
2. **Engine/region attestation.** Turso engine and region are recorded from provisioning/seed/operator evidence, plus SQLite compatibility/version observations. There is no independent engine-specific control-plane proof. Unknown actual vendor engine versions remain null.
3. **Cancellation/cleanup.** D1 cannot be canceled through an application AbortSignal; SDK fetch timeouts are SDK-owned. A timed-out write can commit. Worker background grace and server session reaping cannot be proven by a completed SDK close. Quarantine/close failures may reduce available capacity until a fresh isolate.
4. **Per-isolate visibility.** Pool caps and peak counters are not account-wide socket/session/memory limits. Cumulative snapshots must not be summed across requests. Lost responses can leave incomplete pool/query/row telemetry; row/cost counts are observed counters, not invoices.
5. **Idle attribution.** Runner silence does not force isolate/database/socket/cache eviction. Other traffic and sequential followup order can warm caches. SDK client allocation is not proof of a new physical connection.
6. **Network/timing.** One-way runner-to-edge time is unknown. Query time includes Worker-to-database transport/decoding, not just SQL CPU. Response serialization covers benchmark metadata, not full LMS page rendering. Worker clocks advance only after I/O; synchronous routing/allocation/composition/serialization CPU timings are explicitly unavailable, not precise zeros. [Workers timing limits](https://developers.cloudflare.com/workers/runtime-apis/performance/)
7. **Repeated state.** Application writes change tables during/across repetitions. Confidence intervals share that state and network conditions; there is no snapshot reset between scenarios. Reseed independent invocations for experiments requiring equivalent mutable starting states.
8. **Tail confidence.** 1,000 successes is only a minimum screening threshold for p99. Short windows, exams and idle observations are frequently insufficient. Run longer from each intended origin before treating tail latency as an architecture signal.
9. **Operational scale.** Static Worker bindings do not establish a deployment/routing strategy for tens of thousands of D1 tenants. Cloudflare documents roughly 5,000 bindings per script; shard/control-plane migration, backup, failover and credential rotation require separate production trials. A 500-tenant benchmark is not proof at 50,000 tenants. [D1 platform limits](https://developers.cloudflare.com/d1/platform/limits/)
10. **Application coverage.** LMS SQL is representative and deliberately small. The Drizzle path measures composition, not Manhali's actual ORM adapter. Exam bursts use independent students with no shared aggregate; real grading/payment/session behavior needs application-level trials.

No database recommendation is made from implementation tests. The new evidence and decision signals support the requested choice once equivalent live runs are available.
