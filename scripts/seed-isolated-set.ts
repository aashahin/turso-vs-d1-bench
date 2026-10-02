// Resumable setup only; this script is never part of a measured workload.
import { parseArgs, getRaw } from "../src/config.ts";
import { open, rename } from "node:fs/promises";
const args = parseArgs();
const backend = getRaw("backend", "");
if (
  !["turso-per-tenant", "tursodb-per-tenant", "d1-per-tenant"].includes(backend)
)
  throw new Error("an isolated benchmark backend is required");
const count = Math.max(...args.tenantCounts);
const concurrency = Number(getRaw("seed-concurrency", "3"));
if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 10)
  throw new Error("seed concurrency must be 1..10");
const progressPath = getRaw("seed-progress-file", "");
if (!progressPath) throw new Error("seed progress file required");
const dims = {
  seedRows: Number(getRaw("rows", String(args.seedRows))),
  tenants: args.tenants,
  studentsPerTenant: args.studentsPerTenant,
  coursesPerTenant: args.coursesPerTenant,
  lessonsPerCourse: args.lessonsPerCourse,
};
let saved:
  { backend: string; dimensions: typeof dims; completed: number[] } | undefined;
if (await Bun.file(progressPath).exists())
  saved = await Bun.file(progressPath).json();
if (
  saved &&
  (saved.backend !== backend ||
    JSON.stringify(saved.dimensions) !== JSON.stringify(dims))
)
  throw new Error("seed resume dimensions differ");
const completed = new Set(saved?.completed ?? []);
let writeQueue = Promise.resolve();
let next = 1;
async function save() {
  const snapshot = {
    backend,
    dimensions: dims,
    completed: [...completed].sort((a, b) => a - b),
    updatedAt: new Date().toISOString(),
  };
  writeQueue = writeQueue.then(async () => {
    const temp = progressPath + ".tmp";
    const handle = await open(temp, "w", 0o600);
    await handle.writeFile(JSON.stringify(snapshot));
    await handle.close();
    await rename(temp, progressPath);
  });
  await writeQueue;
}
await save();
const workers = Array.from({ length: concurrency }, async () => {
  for (;;) {
    const tenant = next++;
    if (tenant > count) return;
    if (completed.has(tenant)) continue;
    console.log(`seed ${backend}/tenant-${tenant}`);
    const response = await fetch(
      `${args.workerUrl}/admin/seed-target?${new URLSearchParams({ backend, tenant: String(tenant) })}`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${args.adminToken}`,
          "content-type": "application/json",
        },
        body: JSON.stringify(dims),
        signal: AbortSignal.timeout(300000),
      },
    );
    if (!response.ok)
      throw new Error(
        `seed failed ${backend}/tenant-${tenant}: HTTP ${response.status}`,
      );
    const body = (await response.json()) as { ok?: boolean };
    if (body.ok !== true) throw new Error("seed did not acknowledge success");
    completed.add(tenant);
    await save();
    console.log(`seed complete ${backend} ${completed.size}/${count}`);
  }
});
const results = await Promise.allSettled(workers);
await writeQueue;
const failure = results.find((r) => r.status === "rejected");
if (failure?.status === "rejected") throw failure.reason;
console.log(
  `seed set complete: ${backend}, ${completed.size} isolated databases`,
);
