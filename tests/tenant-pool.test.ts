import { afterEach, describe, expect, jest, test } from "bun:test";
import { TenantPoolManager } from "../worker/src/pool.ts";
import { DEFAULT_POOL } from "../shared/policy.ts";
import { deferred } from "./helpers.ts";
afterEach(() => jest.useRealTimers());
const flush = async () => {
  for (let i = 0; i < 12; i++) await Promise.resolve();
};
function fixture(options = {}) {
  const created: {
    identity: string;
    closed: boolean;
    close(): Promise<void>;
  }[] = [];
  const pool = new TenantPoolManager(
    {
      ...DEFAULT_POOL,
      sizePerTenant: 1,
      maxActivePools: 2,
      maxTotalConnections: 2,
      operationTimeoutMs: 100,
      checkoutTimeoutMs: 100,
      queryTimeoutMs: 100,
      ...options,
    },
    (identity) => {
      const conn = {
        identity,
        closed: false,
        async close() {
          this.closed = true;
        },
      };
      created.push(conn);
      return conn;
    },
  );
  return { pool, created };
}
describe("tenant pool isolation and bounded resources", () => {
  test("LRU evicts B after A B A C and closes all clients on shutdown", async () => {
    const { pool, created } = fixture();
    const use = (t: string) => pool.use(t, t, async (c) => c.identity);
    await use("A");
    await use("B");
    expect((await use("A")).observation.connectionReused).toBe(true);
    await use("C");
    await flush();
    expect(created.map((c) => [c.identity, c.closed])).toEqual([
      ["A", false],
      ["B", true],
      ["C", false],
    ]);
    expect(pool.snapshot()).toMatchObject({
      poolEviction: 1,
      peakConnections: 2,
      activeTenantPools: 2,
    });
    await pool.close();
    expect(created.every((c) => c.closed)).toBe(true);
    expect(pool.snapshot().connections).toBe(0);
  });
  test("same tenant reuses its connection; different tenants never share", async () => {
    const { pool } = fixture();
    const a = await pool.use("A", "url-a", async (c) => c);
    const b = await pool.use("B", "url-b", async (c) => c);
    const again = await pool.use("A", "url-a", async (c) => c);
    expect(a.out).toBe(again.out);
    expect(a.out).not.toBe(b.out);
    expect(again.observation.poolHit).toBe(true);
    await pool.close();
  });
  test("idle eviction uses injected monotonic time and never closes busy work", async () => {
    let now = 0;
    const closed: string[] = [];
    const pool = new TenantPoolManager(
      { ...DEFAULT_POOL, sizePerTenant: 1, idleTtlMs: 10 },
      (id) => ({
        async close() {
          closed.push(id);
        },
      }),
      () => now,
    );
    await pool.use("A", "A", async () => {});
    now = 11;
    expect(pool.evictIdle()).toBe(1);
    await flush();
    expect(closed).toEqual(["A"]);
    expect((await pool.use("A", "A", async () => {})).observation.poolHit).toBe(
      false,
    );
    await pool.close();
  });
  test("global connection cap includes busy tenants and queued aborts do not leak waiters", async () => {
    const { pool } = fixture({ maxTotalConnections: 1 });
    const gate = deferred();
    const entered = deferred();
    const active = pool.use("A", "A", async () => {
      entered.resolve();
      await gate.promise;
    });
    await entered.promise;
    const controller = new AbortController();
    let ran = false;
    const waiting = pool
      .use(
        "B",
        "B",
        async () => {
          ran = true;
        },
        controller.signal,
      )
      .catch((e) => e);
    controller.abort();
    expect((await waiting).name).toBe("AbortError");
    expect(ran).toBe(false);
    expect(pool.snapshot().peakConnections).toBe(1);
    gate.resolve();
    await active;
    await pool.use("B", "B", async () => {});
    await pool.close();
    expect(pool.snapshot().connections).toBe(0);
  });
  test("already-aborted requests create no pool or connection", async () => {
    const { pool, created } = fixture();
    const c = new AbortController();
    c.abort();
    await expect(pool.use("A", "A", async () => {}, c.signal)).rejects.toThrow(
      "aborted",
    );
    expect(created).toHaveLength(0);
    await pool.close();
  });
  test("failed requests retire the client and next checkout creates a new one", async () => {
    const { pool, created } = fixture();
    await expect(
      pool.use("A", "A", async () => {
        throw new Error("query failure");
      }),
    ).rejects.toThrow("query failure");
    await pool.use("A", "A", async () => {});
    await flush();
    expect(created).toHaveLength(2);
    expect(created[0]?.closed).toBe(true);
    await pool.close();
    expect(pool.snapshot().connections).toBe(0);
  });
  test("stalled client remains bounded and quarantined until late work settles", async () => {
    jest.useFakeTimers();
    const { pool, created } = fixture({ maxTotalConnections: 1 });
    const late = deferred();
    const entered = deferred();
    const stalled = pool
      .use("A", "A", async () => {
        entered.resolve();
        await late.promise;
      })
      .catch((e) => e);
    await entered.promise;
    jest.advanceTimersByTime(100);
    expect((await stalled).timeoutClass).toBe("pool-operation");
    expect(pool.snapshot()).toMatchObject({
      connections: 1,
      quarantinedConnections: 1,
    });
    let ran = false;
    const blocked = pool
      .use("B", "B", async () => {
        ran = true;
      })
      .catch((e) => e);
    await flush();
    jest.advanceTimersByTime(100);
    expect((await blocked).timeoutClass).toBe("pool-checkout");
    expect(ran).toBe(false);
    expect(created).toHaveLength(1);
    late.resolve();
    await flush();
    await pool.use("B", "B", async () => {});
    await pool.close();
    expect(pool.snapshot()).toMatchObject({
      connections: 0,
      quarantinedConnections: 0,
      peakConnections: 1,
    });
    expect(jest.getTimerCount()).toBe(0);
  });
  test("abort during active query never recycles its connection into another tenant", async () => {
    const { pool } = fixture();
    const gate = deferred();
    const entered = deferred();
    const c = new AbortController();
    const a = pool
      .use(
        "A",
        "A",
        async () => {
          entered.resolve();
          await gate.promise;
        },
        c.signal,
      )
      .catch((e) => e);
    await entered.promise;
    c.abort();
    expect((await a).name).toBe("AbortError");
    const b = await pool.use("B", "B", async (c) => c.identity);
    expect(b.out).toBe("B");
    gate.resolve();
    await flush();
    await pool.close();
    expect(pool.snapshot().connections).toBe(0);
  });
});

test("TTL and LRU cannot evict a tenant while a query owns its slot", async () => {
  let now = 0;
  let closed = false;
  const entered = deferred(),
    release = deferred();
  const pool = new TenantPoolManager(
    { ...DEFAULT_POOL, idleTtlMs: 10 },
    () => ({
      async close() {
        closed = true;
      },
    }),
    () => now,
  );
  const active = pool.use("A", "A", async () => {
    entered.resolve();
    await release.promise;
  });
  await entered.promise;
  now = 100;
  expect(pool.evictIdle()).toBe(0);
  expect(closed).toBe(false);
  release.resolve();
  await active;
  await pool.close();
  expect(closed).toBe(true);
});
test("a rejected close is reported and retains capacity instead of creating unbounded replacements", async () => {
  const pool = new TenantPoolManager(
    { ...DEFAULT_POOL, maxTotalConnections: 1 },
    () => ({
      async close() {
        throw new Error("close rejected");
      },
    }),
  );
  await pool.use("A", "A", async () => {});
  await pool.close();
  expect(pool.snapshot()).toMatchObject({
    connectionClosed: 0,
    closeFailed: 1,
    connections: 1,
    quarantinedConnections: 1,
  });
});
