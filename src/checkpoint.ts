import { open, readFile, rename, unlink } from "node:fs/promises";
import { serializeResult } from "./artifacts.ts";
/** A unique live-run checkpoint can be updated; final/historical result files cannot. */
export async function createCheckpoint(path: string) {
  const owner = crypto.randomUUID();
  const handle = await open(path, "wx", 0o600);
  await handle.writeFile(
    JSON.stringify({ checkpointOwner: owner, status: "starting" }),
  );
  await handle.close();
  return async (document: Record<string, unknown>) => {
    const existing = JSON.parse(await readFile(path, "utf8")) as {
      checkpointOwner?: string;
    };
    if (existing.checkpointOwner !== owner)
      throw new Error("checkpoint ownership changed");
    const temporary = `${path}.${crypto.randomUUID()}.tmp`;
    const output = await open(temporary, "wx", 0o600);
    try {
      await output.writeFile(
        serializeResult({
          ...document,
          checkpointOwner: owner,
          updatedAt: new Date().toISOString(),
        }),
      );
      await output.close();
      await rename(temporary, path);
    } catch (error) {
      await output.close().catch(() => {});
      await unlink(temporary).catch(() => {});
      throw error;
    }
  };
}
