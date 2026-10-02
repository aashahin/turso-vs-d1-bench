import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCheckpoint } from "../src/checkpoint.ts";
test("live checkpoints update only their own unique file and preserve final history", async () => {
  const directory = await mkdtemp(join(tmpdir(), "bench-checkpoint-"));
  const path = join(directory, "checkpoint.json");
  try {
    const update = await createCheckpoint(path);
    await update({ status: "running", completedSteps: 1 });
    await expect(createCheckpoint(path)).rejects.toThrow();
    await update({ status: "complete", completedSteps: 2 });
    expect(JSON.parse(await readFile(path, "utf8"))).toMatchObject({
      status: "complete",
      completedSteps: 2,
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
