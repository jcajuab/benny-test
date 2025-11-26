import { S3Client } from "@aws-sdk/client-s3";
import { loadConfig } from "./config";

const cfg = loadConfig();

export const s3 = new S3Client({
  region: cfg.region,
});
