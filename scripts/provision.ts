// Disposable database provisioning is never called by the measured workload.
import { getRaw } from "../src/config.ts";
import { open } from "node:fs/promises";
interface Resource {
  tenant: number;
  name: string;
  engine: "d1" | "libsql" | "tursodb";
  databaseId: string;
  binding?: string;
  url?: string;
  token?: string;
  region?: string;
}
const engine = getRaw("engine", "d1");
const prefix = getRaw("prefix", "");
const count = Number(getRaw("count", "10"));
const file = getRaw(
  "manifest-file",
  `bench-private-${engine}-${Date.now()}.json`,
);
const apply = process.argv.includes("--apply=true");
const resume = process.argv.includes("--resume=true");
const cleanup = process.argv.includes("--cleanup");
if (
  !["d1", "libsql", "tursodb"].includes(engine) ||
  !/^bench-[a-z0-9-]+$/.test(prefix) ||
  !Number.isInteger(count) ||
  count < 1 ||
  count > 1000
)
  throw new Error(
    "Use --engine=d1|libsql|tursodb --prefix=bench-<disposable-name> --count=1..1000",
  );
async function command(args: string[]): Promise<string> {
  const child = Bun.spawn(args, {
    stdout: "pipe",
    stderr: "pipe",
    env: { ...process.env, WRANGLER_SEND_METRICS: "false" },
  });
  const [stdout] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  if ((await child.exited) !== 0)
    throw new Error(
      `provisioning command failed: ${args[0]} (output suppressed to protect credentials)`,
    );
  return stdout.trim();
}
if (cleanup) {
  if (
    !apply ||
    process.argv
      .find((arg) => arg.startsWith("--confirm-delete="))
      ?.slice("--confirm-delete=".length) !== prefix
  )
    throw new Error(
      "Cleanup requires --apply=true and --confirm-delete=<exact disposable prefix>",
    );
  const resources = (await Bun.file(file).json()) as Resource[];
  if (
    !Array.isArray(resources) ||
    resources.some(
      (r) =>
        r.engine !== engine ||
        !r.name.startsWith(prefix + "-tenant-") ||
        !/^bench-[a-z0-9-]+-tenant-\d+$/.test(r.name),
    )
  )
    throw new Error("invalid cleanup manifest");
  for (const r of resources) {
    console.log(`deleting disposable ${engine}/${r.name}`);
    await command(
      engine === "d1"
        ? [
            "bun",
            "node_modules/wrangler/bin/wrangler.js",
            "d1",
            "delete",
            r.name,
            "--skip-confirmation",
            "--config",
            "worker/wrangler.jsonc",
          ]
        : ["turso", "db", "destroy", r.name, "--yes"],
    );
  }
} else {
  const location = getRaw("location", "weur");
  const group = getRaw("group", "");
  if (engine !== "d1" && !group)
    throw new Error(
      "Turso provisioning requires an explicitly selected --group",
    );
  const resources: Resource[] = [];
  for (let tenant = 1; tenant <= count; tenant++)
    resources.push({
      tenant,
      name: `${prefix}-tenant-${tenant}`,
      engine: engine as Resource["engine"],
      databaseId: "",
      region:
        engine === "d1"
          ? `location-hint:${location}`
          : getRaw("region", "unknown"),
      binding:
        engine === "d1"
          ? `${location === "eeur" ? "DB_EEUR" : "DB"}_TENANT_${tenant}`
          : undefined,
    });
  if (!apply) {
    console.log(
      JSON.stringify(
        {
          plan: resources,
          notes:
            "No infrastructure changes. Add --apply=true to provision. No seeding occurs.",
        },
        null,
        2,
      ),
    );
  } else {
    if (resume) {
      const existing = (await Bun.file(file).json()) as Resource[];
      if (
        !Array.isArray(existing) ||
        existing.some(
          (r) =>
            r.engine !== engine ||
            r.name !== `${prefix}-tenant-${r.tenant}` ||
            r.tenant < 1 ||
            r.tenant > count ||
            !r.databaseId,
        )
      )
        throw new Error("invalid resume journal");
      for (const saved of existing)
        Object.assign(resources[saved.tenant - 1]!, saved);
    }
    const handle = await open(file, resume ? "r+" : "wx", 0o600);
    try {
      for (const r of resources) {
        if (r.databaseId) continue;
        console.log(`creating disposable ${engine}/${r.name}`);
        if (engine === "d1") {
          const output = await command([
            "bun",
            "node_modules/wrangler/bin/wrangler.js",
            "d1",
            "create",
            r.name,
            "--location",
            location,
            "--config",
            "worker/wrangler.jsonc",
            "--update-config=false",
          ]);
          const id = output.match(
            /database_id["\s:=>]+["']?([a-f0-9-]{36})/i,
          )?.[1];
          if (!id)
            throw new Error(
              "D1 created but identity could not be parsed; inspect vendor dashboard before retrying",
            );
          r.databaseId = id;
        } else {
          await command([
            "turso",
            "db",
            "create",
            r.name,
            "--group",
            group,
            "--wait",
            ...(engine === "tursodb" ? ["--tursodb"] : []),
          ]);
          r.url = await command(["turso", "db", "show", r.name, "--http-url"]);
          r.token = await command([
            "turso",
            "db",
            "tokens",
            "create",
            r.name,
            "--expiration",
            "7d",
          ]);
          r.databaseId = crypto.randomUUID();
        }
        // Journal after each creation; preserves cleanup evidence on partial failure.
        await handle.truncate(0);
        await handle.write(
          JSON.stringify(
            resources.filter((r) => r.databaseId),
            null,
            2,
          ),
          0,
          "utf8",
        );
        await handle.sync();
      }
    } finally {
      await handle.close();
    }
    console.log(`saved private resource journal: ${file}`);
    if (engine === "d1")
      console.log(
        JSON.stringify(
          {
            d1_databases: resources.map((r) => ({
              binding: r.binding,
              database_name: r.name,
              database_id: r.databaseId,
            })),
          },
          null,
          2,
        ),
      );
    else {
      const secretFile = file.replace(/\.json$/, "") + "-worker-secret.json";
      const secret = Object.fromEntries(
        resources.map((r) => [
          r.tenant,
          {
            databaseId: r.databaseId,
            url: r.url,
            token: r.token,
            engine: r.engine,
            region: r.region,
          },
        ]),
      );
      const h = await open(secretFile, "wx", 0o600);
      try {
        await h.writeFile(JSON.stringify(secret));
      } finally {
        await h.close();
      }
      console.log(
        `upload ${secretFile} as ${engine === "libsql" ? "TURSO_TENANTS" : "TURSODB_TENANTS"}; tokens are not printed`,
      );
    }
  }
}
