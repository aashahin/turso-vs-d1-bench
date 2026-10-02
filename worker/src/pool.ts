import {
  BenchError,
  bounded,
  safeError,
  type PoolConfig,
  validatePool,
} from "../../shared/policy.ts";
export interface PoolRequestContext {
  signal?: AbortSignal;
  waitUntil?: (promise: Promise<unknown>) => void;
}
export interface Closable {
  close(): Promise<void>;
}
export interface PoolMetrics {
  snapshotSequence?: number;
  connectionCreated: number;
  connectionReused: number;
  connectionClosed: number;
  closeFailed: number;
  poolHit: number;
  poolMiss: number;
  poolEviction: number;
  poolWaitMs: number;
  connectionCreateMs: number | null;
  activeTenantPools: number;
  peakActiveTenantPools: number;
  connections: number;
  peakConnections: number;
  quarantinedConnections: number;
}
export interface PoolObservation {
  poolHit: boolean;
  connectionReused: boolean;
  checkoutMs: number;
  connectionCreateMs: number | null;
  events: Pick<
    PoolMetrics,
    | "connectionCreated"
    | "connectionReused"
    | "connectionClosed"
    | "poolHit"
    | "poolMiss"
    | "poolEviction"
  >;
}
interface Slot<C> {
  conn: C;
  busy: boolean;
}
interface Pool<C> {
  slots: Slot<C>[];
  lastUsed: number;
  identity: string;
}
/** One manager per isolate/config/engine. Bounds include retiring and quarantined clients. */
export class TenantPoolManager<C extends Closable> {
  private pools = new Map<string, Pool<C>>();
  private waiters = new Set<() => void>();
  private closing = new Set<Promise<void>>();
  private stopped = false;
  private snapshotSequence = 0;
  readonly metrics: PoolMetrics = {
    connectionCreated: 0,
    connectionReused: 0,
    connectionClosed: 0,
    closeFailed: 0,
    poolHit: 0,
    poolMiss: 0,
    poolEviction: 0,
    poolWaitMs: 0,
    connectionCreateMs: 0,
    activeTenantPools: 0,
    peakActiveTenantPools: 0,
    connections: 0,
    peakConnections: 0,
    quarantinedConnections: 0,
  };
  constructor(
    readonly config: PoolConfig,
    private create: (identity: string) => C,
    private now: () => number = () => performance.now(),
  ) {
    validatePool(config);
  }
  snapshot(): PoolMetrics {
    return { ...this.metrics, snapshotSequence: ++this.snapshotSequence };
  }
  private wake(): void {
    for (const wake of this.waiters) wake();
    this.waiters.clear();
  }
  private gauges(): void {
    this.metrics.activeTenantPools = this.pools.size;
    this.metrics.peakActiveTenantPools = Math.max(
      this.metrics.peakActiveTenantPools,
      this.pools.size,
    );
    this.metrics.peakConnections = Math.max(
      this.metrics.peakConnections,
      this.metrics.connections,
    );
  }
  private retire(
    slot: Slot<C>,
    waitUntil?: PoolRequestContext["waitUntil"],
  ): void {
    const close = Promise.resolve()
      .then(() => slot.conn.close())
      .then(
        () => {
          this.metrics.connectionClosed++;
          this.metrics.connections--;
        },
        () => {
          this.metrics.closeFailed++;
          // A rejected close leaves server lifetime unknown; retain its capacity.
          this.metrics.quarantinedConnections++;
        },
      )
      .finally(() => {
        this.closing.delete(close);
        this.wake();
      });
    this.closing.add(close);
    waitUntil?.(close);
  }
  private evict(
    key: string,
    waitUntil?: PoolRequestContext["waitUntil"],
  ): void {
    const pool = this.pools.get(key)!;
    this.pools.delete(key);
    this.metrics.poolEviction++;
    for (const slot of pool.slots) this.retire(slot, waitUntil);
    this.gauges();
  }
  evictIdle(waitUntil?: PoolRequestContext["waitUntil"]): number {
    let count = 0;
    for (const [key, p] of this.pools)
      if (
        p.slots.every((s) => !s.busy) &&
        this.now() - p.lastUsed >= this.config.idleTtlMs
      ) {
        this.evict(key, waitUntil);
        count++;
      }
    return count;
  }
  private evictOldest(
    exclude: string,
    waitUntil?: PoolRequestContext["waitUntil"],
  ): boolean {
    for (const [key, p] of this.pools)
      if (key !== exclude && p.slots.every((s) => !s.busy)) {
        this.evict(key, waitUntil);
        return true;
      }
    return false;
  }
  async use<T>(
    key: string,
    identity: string,
    fn: (conn: C) => Promise<T>,
    context?: AbortSignal | PoolRequestContext,
  ): Promise<{ out: T; checkoutMs: number; observation: PoolObservation }> {
    const signal = context instanceof AbortSignal ? context : context?.signal;
    const waitUntil =
      context instanceof AbortSignal ? undefined : context?.waitUntil;
    if (signal?.aborted)
      throw new DOMException("request aborted", "AbortError");
    if (this.stopped) throw new Error("pool manager stopped");
    const t0 = this.now();
    let evictions = this.evictIdle(waitUntil);
    const hit = this.pools.has(key);
    this.metrics[hit ? "poolHit" : "poolMiss"]++;
    let createdMs = 0;
    let checkoutMs: number | null = null;
    const acquire = new AbortController();
    const onAbort = () => acquire.abort(signal?.reason);
    signal?.addEventListener("abort", onAbort, { once: true });
    const acquireTimer = setTimeout(
      () =>
        acquire.abort(
          new BenchError("timeout", "pool-checkout timeout", "pool-checkout"),
        ),
      this.config.checkoutTimeoutMs,
    );
    let created = false;
    let checkedOut = false;
    const observation = (): PoolObservation => ({
      poolHit: hit,
      connectionReused: checkedOut && !created,
      checkoutMs: checkoutMs ?? this.now() - t0,
      connectionCreateMs: createdMs,
      events: {
        connectionCreated: created ? 1 : 0,
        connectionReused: checkedOut && !created ? 1 : 0,
        connectionClosed: 0,
        poolHit: hit ? 1 : 0,
        poolMiss: hit ? 0 : 1,
        poolEviction: evictions,
      },
    });
    try {
      const slot = await bounded(
        async () => {
          for (;;) {
            if (this.stopped) throw new Error("pool manager stopped");
            if (acquire.signal.aborted) throw acquire.signal.reason;
            let p = this.pools.get(key);
            if (p && p.identity !== identity) {
              if (p.slots.some((s) => s.busy))
                throw new Error(
                  "database credentials changed during active operations",
                );
              this.evict(key, waitUntil);
              evictions++;
              p = undefined;
            }
            if (
              !p &&
              this.pools.size >= this.config.maxActivePools &&
              this.evictOldest(key, waitUntil)
            )
              evictions++;
            if (!p && this.pools.size < this.config.maxActivePools) {
              p = { slots: [], lastUsed: this.now(), identity };
              this.pools.set(key, p);
              this.gauges();
            }
            if (p) {
              // Touch LRU only on actual checkout, not an abandoned queued request.
              let s = p.slots.find((s) => !s.busy);
              if (
                !s &&
                p.slots.length < this.config.sizePerTenant &&
                this.metrics.connections < this.config.maxTotalConnections
              ) {
                const c0 = this.now();
                const conn = this.create(identity);
                createdMs = this.now() - c0;
                created = true;
                s = { conn, busy: false };
                p.slots.push(s);
                this.metrics.connectionCreated++;
                this.metrics.connectionCreateMs =
                  (this.metrics.connectionCreateMs ?? 0) + createdMs;
                this.metrics.connections++;
                this.gauges();
              } else if (s) this.metrics.connectionReused++;
              if (s) {
                checkedOut = true;
                s.busy = true;
                p.lastUsed = this.now();
                this.pools.delete(key);
                this.pools.set(key, p);
                return { slot: s, pool: p };
              }
              if (
                this.metrics.connections >= this.config.maxTotalConnections &&
                this.closing.size === 0 &&
                this.evictOldest(key, waitUntil)
              )
                evictions++;
            }
            await new Promise<void>((resolve, reject) => {
              const wake = () => {
                acquire.signal.removeEventListener("abort", abort);
                this.waiters.delete(wake);
                resolve();
              };
              const abort = () => {
                this.waiters.delete(wake);
                reject(
                  acquire.signal.reason ??
                    new DOMException("request aborted", "AbortError"),
                );
              };
              this.waiters.add(wake);
              acquire.signal.addEventListener("abort", abort, { once: true });
            });
          }
        },
        this.config.checkoutTimeoutMs,
        "pool-checkout",
        signal,
      ).finally(() => {
        clearTimeout(acquireTimer);
        signal?.removeEventListener("abort", onAbort);
        acquire.abort();
      });
      checkoutMs = this.now() - t0;
      this.metrics.poolWaitMs += checkoutMs;
      const work = Promise.resolve().then(() => fn(slot.slot.conn));
      let completed = false;
      void work.then(
        () => {
          completed = true;
        },
        () => {
          completed = true;
        },
      );
      let good = false;
      try {
        const out = await bounded(
          () => work,
          this.config.operationTimeoutMs,
          "pool-operation",
          signal,
        );
        good = true;
        return {
          out,
          checkoutMs,
          observation: {
            poolHit: hit,
            connectionReused: !created,
            checkoutMs,
            connectionCreateMs: createdMs,
            events: {
              connectionCreated: created ? 1 : 0,
              connectionReused: checkedOut && !created ? 1 : 0,
              connectionClosed: 0,
              poolHit: hit ? 1 : 0,
              poolMiss: hit ? 0 : 1,
              poolEviction: evictions,
            },
          },
        };
      } finally {
        if (good) {
          slot.slot.busy = false;
          slot.pool.lastUsed = this.now();
          if (this.stopped) {
            slot.pool.slots = slot.pool.slots.filter((s) => s !== slot.slot);
            this.retire(slot.slot, waitUntil);
          }
          this.wake();
        } else {
          slot.pool.slots = slot.pool.slots.filter((s) => s !== slot.slot);
          // SDK has no caller AbortSignal and close does not lock against a running query.
          // Retain capacity until the old work settles, then close. Never reuse it.
          this.metrics.quarantinedConnections++;
          const cleanup = () => {
            this.metrics.quarantinedConnections--;
            this.retire(slot.slot, waitUntil);
            this.wake();
          };
          if (completed) cleanup();
          else {
            const retiring = work.then(cleanup, cleanup);
            waitUntil?.(retiring);
          }
          this.wake();
        }
      }
    } catch (error) {
      if (checkoutMs === null) this.metrics.poolWaitMs += this.now() - t0;
      const classification = safeError(error);
      const failure = new BenchError(
        classification.errorClass,
        classification.error,
        classification.timeoutClass,
      );
      failure.evidence = { pool: observation(), poolMetrics: this.snapshot() };
      if (error instanceof Error) {
        Object.assign(error, { evidence: failure.evidence });
        throw error;
      }
      throw failure;
    }
  }
  async close(): Promise<void> {
    this.stopped = true;
    for (const [key, p] of this.pools)
      if (p.slots.every((s) => !s.busy)) this.evict(key);
    this.wake();
    await Promise.all(this.closing);
  }
}
