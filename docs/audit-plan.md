# Manhali benchmark audit and implementation plan

Audit baseline: `7b1b2c6915bd442de8f9b0662bc70176834ff133` (2026-10-01).

The requested decision concerns isolated academy databases on Workers, uneven MENA traffic, idle behavior, independent student writes, and reliability. Existing historical measurements are evidence about their original configurations only.

## Audit findings before implementation

| Severity | Location | Distortion | Planned correction |
| --- | --- | --- | --- |
| Critical | worker/src/tenant.ts | Missing isolated D1 bindings fall back to shared; all Turso routes are shared | Explicit topology names, required mappings, database identity validation |
| Critical | src/runner.ts | Constant concurrency reduces offered load when slow | Preserve closed loop; independent arrival schedule with admission drops and schedule-to-completion timing |
| High | worker/src/backends.ts | Single URL pool, fixed limits, old clients discarded without closing; watchdog can replace still-running clients | Bounded tenant LRU, wait/creation/retirement telemetry, quarantine outstanding work |
| High | src/bench.ts | Seed/config checks occur after measurements and mismatches only warn | Authenticated per-database preflight before each live run |
| High | src/seed.ts, worker/src/seed.ts | Different lesson titles/bodies, nondeterministic default timestamps; every isolated D1 contains every tenant | Shared seed implementation with tenant subset and fixed timestamps |
| High | worker/src/queries.ts | Progress percentage stores a count; enrollment often a no-op; read/write atomicity differs by mode | Correct percentage, explicit query/transaction shapes, representative order/exam writes |
| High | worker/src/backends.ts | Runner 5s, SDK 10s, slot 15s budgets create backend work after runner failure | Same runner/handler budget for every backend, bounded phase budgets, report cancellation limits |
| Medium | src/workloads/lms.ts | Approximate stochastic mix unverified; only attempt-owner subset submits | Deterministic configurable profiles, observed counts, distinct exam student rows |
| Medium | src/results.ts, reporters | Small-sample p99, little run variation, comparison grouping misses access layer/topology | Suppress report p99 below 1,000 successes, dispersion/CI and comparable factual signals |
| Medium | src/bench.ts | Output overwrites results; geography and deployment evidence incomplete | Exclusive output creation and explicit provenance |
| Medium | src/cold-start.ts | Same-row followups mix row cache and connection reuse | Preserve lifecycle observations, add row/tenant variants and intervals |

Preserve deterministic ID generation, identical edge SQL/bind order, failure-aware latency, transaction retry accounting, direct-mode separation, lifecycle tracker, regression coverage, and all historical result files.

## Incremental implementation

1. Topology contracts, shared schema/seed evidence, bounded tenant pool and routing.
2. Arrival scheduling, distributions/profiles, request observations and timing.
3. Preflight/orchestration, suites, repeat statistics, reports and exclusive artifacts.
4. Canonical suite, setup/cleanup guidance, deterministic tests and final engineering report.

No dependency upgrade is planned: inspect installed serverless 1.4.0 and Drizzle 0.44.7. Drizzle currently measures SQL composition only; it is not a full ORM adapter benchmark. Unit tests use fakes; live provisioning and seeding precede timing.
