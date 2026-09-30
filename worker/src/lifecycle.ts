import type { WorkerLifecycle } from "../../shared/lifecycle.ts";

/** Isolate-local instrumentation only; never contains request data or secrets. */
export function createLifecycleTracker(): () => WorkerLifecycle {
  let isolateId: string | undefined;
  let requestNumber = 0;
  return () => {
    // Workers allow randomness in request context, not module initialization.
    isolateId ??= crypto.randomUUID();
    requestNumber++;
    return { isolateId, requestNumber, firstRequest: requestNumber === 1 };
  };
}
