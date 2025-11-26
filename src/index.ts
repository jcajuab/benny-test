import { loadConfig } from "./config";
import { s3 } from "./s3Client";
import { processMessages } from "./processor";

type Args = {
  messagesPrefix?: string;
  detailsPrefix?: string;
  limit?: number;
  dryRun?: boolean;
};

function parseArgs(argv: string[]): Args {
  const args: Args = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--messages-prefix") {
      args.messagesPrefix = argv[++i];
    } else if (a === "--details-prefix") {
      args.detailsPrefix = argv[++i];
    } else if (a === "--limit") {
      const v = argv[++i];
      args.limit = v ? Number(v) : undefined;
    } else if (a === "--dry-run") {
      args.dryRun = true;
    }
  }
  return args;
}

async function main() {
  const argv = process.argv.slice(2);
  const args = parseArgs(argv);
  const config = loadConfig();

  if (args.messagesPrefix) config.messagesPrefix = args.messagesPrefix;
  if (args.detailsPrefix) config.detailsPrefix = args.detailsPrefix;

  console.log(
    `Loaded config: bucket=${config.bucket}, detailsPrefix=${config.detailsPrefix}, rawFilesPrefix=${config.rawFilesPrefix}`,
  );

  const counters = await processMessages(s3, config, {
    limit: args.limit,
    dryRun: args.dryRun,
  });
  console.log(
    `Done. processed=${counters.processed} created=${counters.created} skipped=${counters.skipped} failed=${counters.failed}`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
