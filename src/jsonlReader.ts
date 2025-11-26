import { GetObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import readline from "node:readline";
import { Readable } from "node:stream";

export async function listJsonlKeys(
  s3: S3Client,
  bucket: string,
  prefix: string,
): Promise<string[]> {
  const keys: string[] = [];
  let continuationToken: string | undefined;
  do {
    const res = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken,
      }),
    );
    res.Contents?.forEach((c) => {
      if (c.Key && c.Key.toLowerCase().endsWith(".jsonl")) keys.push(c.Key);
    });
    continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (continuationToken);
  return keys;
}

export async function* readJsonlFromS3<T>(
  s3: S3Client,
  bucket: string,
  key: string,
): AsyncGenerator<T> {
  const obj = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  const body = obj.Body as Readable;
  const rl = readline.createInterface({ input: body, crlfDelay: Infinity });
  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      yield JSON.parse(trimmed) as T;
    } catch (err) {
      console.warn(`Failed to parse JSONL line in ${key}: ${(err as Error).message}`);
    }
  }
}
