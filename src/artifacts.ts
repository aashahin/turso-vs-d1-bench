import { open, unlink } from "node:fs/promises";
/** Reserve all paths before writing any. O_EXCL also protects against concurrent runners. */
export async function writeArtifacts(
  files: Record<string, string>,
): Promise<void> {
  const handles: Awaited<ReturnType<typeof open>>[] = [];
  const created: string[] = [];
  try {
    for (const path of Object.keys(files)) {
      handles.push(await open(path, "wx", 0o600));
      created.push(path);
    }
    for (let i = 0; i < created.length; i++)
      await handles[i]!.writeFile(files[created[i]!]!);
  } catch (error) {
    await Promise.all(handles.map((h) => h.close()));
    await Promise.all(created.map((path) => unlink(path)));
    throw error;
  }
  await Promise.all(handles.map((h) => h.close()));
}

/** Report validity separately from the legacy raw nearest-rank statistic API. */
export function serializeResult(document: unknown): string {
  return JSON.stringify(
    document,
    function (key, value: unknown) {
      if (
        key === "p99" &&
        typeof value === "number" &&
        typeof this.n === "number" &&
        this.n < 1000
      )
        return null;
      return value;
    },
    2,
  );
}
