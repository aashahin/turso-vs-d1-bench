export interface SchedulerClock {
  now(): number;
  sleep(ms: number): Promise<void>;
}
export const realClock: SchedulerClock = {
  now: () => performance.now(),
  sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
};
export interface ArrivalMetrics {
  scheduled: number;
  started: number;
  completed: number;
  dropped: number;
  lateStarts: number;
  inFlight: number;
  peakInFlight: number;
  queueDelayMs: number[];
  offeredWindowMs: number;
  completedInWindow: number;
}
/** Fixed absolute arrivals. Admission drops do not delay later arrivals. No completion-driven refill. */
export async function arrivals(opts: {
  total: number;
  rps: number;
  maxInFlight: number;
  maxStartLagMs: number;
  clock?: SchedulerClock;
  execute: (index: number, queueDelayMs: number) => Promise<void>;
  onDrop?: (index: number) => void;
}): Promise<ArrivalMetrics> {
  if (!Number.isFinite(opts.rps) || opts.rps <= 0 || opts.maxInFlight < 1)
    throw new Error("invalid arrival scheduling options");
  const clock = opts.clock ?? realClock;
  const start = clock.now();
  const window = (opts.total / opts.rps) * 1000;
  const m: ArrivalMetrics = {
    scheduled: 0,
    started: 0,
    completed: 0,
    dropped: 0,
    lateStarts: 0,
    inFlight: 0,
    peakInFlight: 0,
    queueDelayMs: [],
    offeredWindowMs: window,
    completedInWindow: 0,
  };
  const pending = new Set<Promise<void>>();
  for (let i = 0; i < opts.total; i++) {
    const due = start + (i / opts.rps) * 1000;
    const wait = due - clock.now();
    if (wait > 0) await clock.sleep(wait);
    m.scheduled++;
    const delay = Math.max(0, clock.now() - due);
    m.queueDelayMs.push(delay);
    if (m.inFlight >= opts.maxInFlight || delay > opts.maxStartLagMs) {
      m.dropped++;
      opts.onDrop?.(i);
      continue;
    }
    m.started++;
    m.inFlight++;
    m.peakInFlight = Math.max(m.peakInFlight, m.inFlight);
    if (delay > 1) m.lateStarts++;
    const p = opts.execute(i, delay).finally(() => {
      m.inFlight--;
      m.completed++;
      if (clock.now() < start + window) m.completedInWindow++;
      pending.delete(p);
    });
    pending.add(p);
  }
  const endWait = start + window - clock.now();
  if (endWait > 0) await clock.sleep(endWait);
  await Promise.all(pending);
  return m;
}
