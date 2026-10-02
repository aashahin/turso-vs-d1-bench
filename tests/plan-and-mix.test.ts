import { expect, test } from "bun:test";
import { roundPlans, type ScenarioPlan } from "../src/plan.ts";
import { profileMix } from "../src/workloads/profiles.ts";

test("repetitions interleave equivalent scenarios and rotate backend order", () => {
  const plans: ScenarioPlan[] = [];
  for (const backend of [
    "d1-per-tenant",
    "turso-per-tenant",
    "tursodb-per-tenant",
  ])
    for (const concurrency of [1, 10])
      plans.push({
        backend,
        mode: "edge",
        suite: "mixed-lms",
        test: "lms-balanced",
        tenantMode: "single",
        tenantCount: 1,
        concurrency,
        loadModel: "closed-loop",
        offeredRps: null,
        scanLimit: null,
        distribution: "uniform",
      });
  expect(roundPlans(plans, 1).map((p) => [p.backend, p.concurrency])).toEqual([
    ["d1-per-tenant", 1],
    ["turso-per-tenant", 1],
    ["tursodb-per-tenant", 1],
    ["d1-per-tenant", 10],
    ["turso-per-tenant", 10],
    ["tursodb-per-tenant", 10],
  ]);
  expect(
    roundPlans(plans, 2)
      .slice(0, 3)
      .map((p) => p.backend),
  ).toEqual(["turso-per-tenant", "tursodb-per-tenant", "d1-per-tenant"]);
  expect(new Set(roundPlans(plans, 3))).toEqual(new Set(plans));
});
test("custom LMS mixes cannot silently replace atomic reads, writes or KV profiles", () => {
  const custom = { "order-event": 100 };
  expect(profileMix("point-read", "lms", custom)).toBeNull();
  expect(profileMix("update-progress", "lms", custom)).toBeNull();
  expect(profileMix("mixed", "kv", custom)).toBeNull();
  expect(profileMix("lms-balanced", "lms", custom)).toBe(custom);
  expect(profileMix("mixed", "lms", custom)).toBe(custom);
});

test("extensive preset respects the configured physical tenant count", async () => {
  const { canonicalPlans } = await import("../src/plan.ts");
  const { parseArgs } = await import("../src/config.ts");
  const args = {
    ...parseArgs({ manhali: true, full: true }),
    tenants: 100,
    adminToken: "test",
  };
  const plan = canonicalPlans(args, true);
  expect(Math.max(...plan.map((p) => p.tenantCount))).toBe(100);
  expect(
    new Set(
      plan.filter((p) => p.test === "tenant-churn").map((p) => p.tenantCount),
    ),
  ).toEqual(new Set([10, 50, 100]));
});
