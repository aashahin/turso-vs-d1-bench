import { expect, test } from "bun:test";
import { runOp, type OpParams } from "../worker/src/queries.ts";
import { deferred } from "./helpers.ts";
test.each(["d1-per-tenant", "d1-per-tenant-rr"] as const)(
  "%s cannot start attempt creation or a write batch after aborting during lookup",
  async (backend) => {
    const entered = deferred(),
      reply = deferred();
    let writes = 0;
    const empty = {
      results: [],
      meta: { last_row_id: null, rows_read: 0, rows_written: 0 },
    };
    const db: D1Database = {
      prepare: () => ({
        bind() {
          return this;
        },
        first: async () => null,
        all: async () => {
          entered.resolve();
          await reply.promise;
          return empty;
        },
        run: async () => {
          writes++;
          return empty;
        },
      }),
      batch: async () => {
        writes++;
        return [];
      },
      exec: async () => {},
      withSession: () => db,
    };
    const params: OpParams = {
      tenant: 1,
      quiz: 1,
      student: 1,
      course: 1,
      lesson: 1,
      question: 1,
      attempt: 0,
      id: 1,
      startId: 1,
      limit: 1,
      position: 1,
      payload: "",
      answer: "B",
      completed: 1,
      retries: 0,
    };
    const c = new AbortController();
    const operation = runOp(
      backend,
      db,
      { DB: db, DB_EEUR: db } as never,
      "submit-quiz-answer",
      params,
      "isolated",
      { signal: c.signal },
    );
    await entered.promise;
    c.abort();
    reply.resolve();
    await expect(operation).rejects.toThrow("aborted");
    expect(writes).toBe(0);
  },
);
