import { HeadObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { listJsonlKeys, readJsonlFromS3 } from "./jsonlReader";
import { extractEmail, toEml, GmailMessage } from "./emailExtractor";
import { AppConfig } from "./config";

export type ProcessorOptions = {
  limit?: number;
  dryRun?: boolean;
};

type Counters = { processed: number; created: number; skipped: number; failed: number };

async function exists(s3: S3Client, bucket: string, key: string): Promise<boolean> {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch (err: any) {
    if (err?.$metadata?.httpStatusCode === 404) return false;
    if (err?.name === "NotFound") return false;
    throw err;
  }
}

export async function processMessages(
  s3: S3Client,
  config: AppConfig,
  opts: ProcessorOptions = {},
): Promise<Counters> {
  const counters: Counters = { processed: 0, created: 0, skipped: 0, failed: 0 };
  const limit = opts.limit ?? Infinity;
  const dryRun = opts.dryRun ?? false;

  const keys = await listJsonlKeys(s3, config.bucket, config.detailsPrefix);
  console.log(`Found ${keys.length} JSONL file(s) under ${config.detailsPrefix}`);

  for (const key of keys) {
    console.log(`Reading ${key}...`);
    for await (const record of readJsonlFromS3<GmailMessage>(s3, config.bucket, key)) {
      try {
        if (counters.processed >= limit) return counters;
        counters.processed += 1;

        const msgId =
          record.id ||
          (record as any)._airbyte_data?.id ||
          (record as any).data?.id ||
          (record as any).message?.id ||
          (record as any).messageId ||
          null;
        if (!msgId) {
          counters.failed += 1;
          console.warn(
            `Missing message id in ${key}, skipping; keys=${Object.keys(record).join(",")}`,
          );
          continue;
        }

        const targetKey = `${config.rawFilesPrefix}${config.workspaceId}/gmail/${msgId}.eml`;

        const already = await exists(s3, config.bucket, targetKey);
        if (already) {
          counters.skipped += 1;
          console.log(`Skipping existing ${targetKey}`);
          continue;
        }

        const email = extractEmail(record);
        const eml = toEml(email);

        // Preview log
        console.log(
          JSON.stringify(
            {
              files: {
                insert: {
                  workspace_id: config.workspaceId,
                  connector_id: config.connectorId,
                  path: targetKey,
                  format: "eml",
                  mime_type: "message/rfc822",
                  size: eml.length,
                  checksum: null,
                  is_viewable: true,
                  sync_status: "synced",
                },
              },
            },
            null,
            2,
          ),
        );

        if (dryRun) {
          console.log(`(Preview only) Would create ${targetKey}`);
        } else {
          await s3.send(
            new PutObjectCommand({
              Bucket: config.bucket,
              Key: targetKey,
              Body: eml,
              ContentType: "message/rfc822",
            }),
          );
          counters.created += 1;
          console.log(`Created ${targetKey}`);
        }
      } catch (err: any) {
        counters.failed += 1;
        console.warn(
          `Error processing message ${msgId} from ${key}: ${(err as Error).message}`,
        );
      }
    }
  }

  return counters;
}
