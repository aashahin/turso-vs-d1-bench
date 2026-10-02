import { parseArgs } from "./config.ts";
import { canonicalPlans } from "./plan.ts";
import { runBenchmark } from "./bench.ts";
const full = process.argv.includes("--full");
const args = parseArgs({ manhali: true, full });
await runBenchmark(args, canonicalPlans(args, full));
