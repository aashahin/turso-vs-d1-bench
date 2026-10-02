/** Observed Worker-isolate identity; this does not measure platform boot time. */
export interface WorkerLifecycle {
  isolateId: string;
  requestNumber: number;
  firstRequest: boolean;
}

export function parseLifecycle(raw: unknown): WorkerLifecycle | null {
  if (typeof raw !== "object" || raw === null) return null;
  const value = raw as Record<string, unknown>;
  if (
    typeof value.isolateId !== "string" ||
    value.isolateId === "" ||
    typeof value.requestNumber !== "number" ||
    !Number.isSafeInteger(value.requestNumber) ||
    value.requestNumber < 1 ||
    value.firstRequest !== (value.requestNumber === 1)
  )
    return null;
  return {
    isolateId: value.isolateId,
    requestNumber: value.requestNumber,
    firstRequest: value.firstRequest,
  };
}
