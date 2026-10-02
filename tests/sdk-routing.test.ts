import { expect, spyOn, test } from "bun:test";
import worker from "../worker/src/index.ts";

test("installed SDK routes raw and Drizzle to the physical tenant origin/token and reuses only that tenant pool", async () => {
  const seen: { host: string; auth: string | null }[] = [];
  const fakeFetch = Object.assign(
    async (
      input: Parameters<typeof fetch>[0],
      init?: Parameters<typeof fetch>[1],
    ) => {
      const url = new URL(String(input));
      seen.push({
        host: url.hostname,
        auth: new Headers(init?.headers).get("authorization"),
      });
      if (url.pathname.endsWith("/cursor"))
        return new Response(
          [
            { baton: null, base_url: null },
            {
              type: "step_begin",
              step: 0,
              cols: [
                { name: "id", decltype: "INTEGER" },
                { name: "payload", decltype: "TEXT" },
              ],
            },
            {
              type: "row",
              row: [
                { type: "integer", value: "1" },
                { type: "text", value: "seeded" },
              ],
            },
            {
              type: "step_end",
              step: 0,
              affected_row_count: 0,
              last_insert_rowid: null,
            },
            { type: "step_begin", step: 1, cols: [] },
            {
              type: "step_end",
              step: 1,
              affected_row_count: 0,
              last_insert_rowid: null,
            },
          ]
            .map((row) => JSON.stringify(row))
            .join("\n") + "\n",
        );
      throw new Error("unexpected SDK request");
    },
    { preconnect: globalThis.fetch.preconnect },
  );
  const fake = spyOn(globalThis, "fetch").mockImplementation(fakeFetch);
  const env = {
    ADMIN_TOKEN: "test",
    TURSO_URL: "https://shared.invalid",
    TURSO_TOKEN: "shared-secret",
    TURSO_TENANTS: JSON.stringify({
      "1": {
        url: "https://tenant-a.invalid",
        token: "secret-a",
        databaseId: "sdk-a",
        engine: "libsql",
      },
      "2": {
        url: "https://tenant-b.invalid",
        token: "secret-b",
        databaseId: "sdk-b",
        engine: "libsql",
      },
    }),
  } as never;
  try {
    const results: Record<string, unknown>[] = [];
    for (const [backend, tenant] of [
      ["turso-per-tenant", 1],
      ["turso-per-tenant-drizzle", 2],
      ["turso-per-tenant-drizzle", 1],
    ] as const) {
      const res = await worker.fetch(
        new Request(
          `https://worker.invalid/bench/${backend}/point-read?tenant=${tenant}&id=1`,
        ),
        env,
      );
      expect(res.status).toBe(200);
      results.push((await res.json()) as Record<string, unknown>);
    }
    expect(seen).toEqual([
      { host: "tenant-a.invalid", auth: "Bearer secret-a" },
      { host: "tenant-b.invalid", auth: "Bearer secret-b" },
      { host: "tenant-a.invalid", auth: "Bearer secret-a" },
    ]);
    expect(results.map((r) => r.databaseIdentity)).toEqual([
      "sdk-a",
      "sdk-b",
      "sdk-a",
    ]);
    expect(results[2]?.pool).toMatchObject({
      poolHit: true,
      connectionReused: true,
    });
    expect(
      results.every((r) => r.tenantIsolation === "database-per-tenant"),
    ).toBe(true);
    expect(results[1]?.orm).toBe("drizzle");
    expect(results[0]?.queries).toBe(1);
    expect(results[0]?.tenantRoutingMs).toBeNull();
    expect(results[0]?.clientCreationMs).toBeNull();
    expect(results[0]?.pool).toMatchObject({ connectionCreateMs: null });
    expect(results[0]?.timingQuality).toMatchObject({
      synchronousPhaseTimers: "unavailable",
    });
  } finally {
    fake.mockRestore();
  }
});
