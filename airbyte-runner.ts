import { GetObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import type { _Object as S3Object } from "@aws-sdk/client-s3";
import fs from "node:fs";
import path from "node:path";
import { processDocument } from "./index";

type AirbyteJob = {
  id: number;
  status: string;
};

async function fetchJob(jobId: number): Promise<AirbyteJob> {
  try {
    const { job } = await airbyteApi<{ job: AirbyteJob }>("jobs/get", { jobId });
    return job;
  } catch (error: any) {
    const status = (error as any).status as number | undefined;
    const message = (error as Error).message || String(error);
    if (status === 401 || status === 403 || /401|403/.test(message)) {
      console.warn(
        "Job polling unauthorized/forbidden (likely scope limits or PAT scope); continuing without polling.",
      );
      return { id: jobId, status: "unknown" };
    }
    throw error;
  }
}

const normalizeBase = (base: string) => base.replace(/\/+$/, "");

const AIRBYTE_BASE = normalizeBase(process.env.AIRBYTE_API_BASE || "https://api.airbyte.com/v1");
const AIRBYTE_TOKEN_BASE = normalizeBase(
  process.env.AIRBYTE_TOKEN_BASE || "https://api.airbyte.com/api/v1",
);
const AIRBYTE_TOKEN_URL = `${AIRBYTE_TOKEN_BASE}/applications/token`;

const env = (name: string) => {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required env var: ${name}`);
  return value;
};

const envBool = (name: string, fallback = false) => {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  return ["1", "true", "yes", "on"].includes(raw.toLowerCase());
};

const mimeFromExt: Record<string, string> = {
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  pdf: "application/pdf",
  txt: "text/plain",
};

const config = {
  airbyteToken: (process.env.AIRBYTE_ACCESS_TOKEN || "").trim() || null,
  airbyteClientId: process.env.AIRBYTE_CLIENT_ID || null,
  airbyteClientSecret: process.env.AIRBYTE_CLIENT_SECRET || null,
  connectionId: env("AIRBYTE_CONNECTION_ID"),
  bucket: env("AIRBYTE_S3_BUCKET"),
  prefix: env("AIRBYTE_S3_PREFIX"),
  region: env("AWS_REGION"),
  stateFile: process.env.AIRBYTE_STATE_FILE || ".airbyte-processed.json",
  skipTrigger: envBool("AIRBYTE_SKIP_TRIGGER", false),
  skipPoll: envBool("AIRBYTE_SKIP_POLL", false),
  latestRunOnly: envBool("AIRBYTE_LATEST_RUN_ONLY", true),
  seedStateOnly: envBool("AIRBYTE_SEED_STATE_ONLY", false),
  processEnabled: envBool("AIRBYTE_PROCESS_ENABLED", false),
  workspaceId: process.env.AIRBYTE_WORKSPACE_ID ?? process.env.WORKSPACE_ID ?? null,
  connectorName: process.env.CONNECTOR_NAME || "airbyte-drive",
  connectorType: process.env.CONNECTOR_TYPE || "google_drive",
  connectorStatus: process.env.CONNECTOR_STATUS || "synced",
};

const s3 = new S3Client({ region: config.region });

async function ensureAccessToken(): Promise<string> {
  if (config.airbyteToken) return config.airbyteToken;

  if (config.airbyteClientId && config.airbyteClientSecret) {
    const res = await fetch(AIRBYTE_TOKEN_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        client_id: config.airbyteClientId,
        client_secret: config.airbyteClientSecret,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(
        `Airbyte token fetch failed (${res.status}) via ${AIRBYTE_TOKEN_URL}: ${text}`,
      );
    }

    const json = (await res.json()) as { access_token?: string };
    if (!json.access_token) throw new Error("Airbyte token response missing access_token");
    return json.access_token;
  }

  throw new Error(
    "Missing AIRBYTE_ACCESS_TOKEN or AIRBYTE_CLIENT_ID/SECRET. Provide a PAT or client credentials.",
  );
}

const airbyteApi = async <T>(path: string, body: object): Promise<T> => {
  const token = await ensureAccessToken();

  const url = `${AIRBYTE_BASE}/${path}`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    const error = new Error(`Airbyte API ${url} failed (${res.status}): ${text}`);
    // @ts-expect-error augment
    error.status = res.status;
    throw error;
  }

  return res.json() as Promise<T>;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function triggerAndWait(): Promise<number> {
  const trigger = await airbyteApi<{ jobId: number }>("jobs", {
    connectionId: config.connectionId,
    jobType: "sync",
  });

  const jobId = trigger.jobId;
  console.log(`Triggered Airbyte sync job ${jobId}`);

  if (config.skipPoll) {
    console.log("AIRBYTE_SKIP_POLL is set; skipping job status polling.");
    return jobId;
  }

  for (;;) {
    const job = await fetchJob(jobId);
    console.log(`Job ${jobId} status: ${job.status}`);

    if (job.status === "succeeded" || job.status === "unknown") return jobId;
    if (["cancelled", "failed", "error", "incomplete"].includes(job.status)) {
      throw new Error(`Airbyte job ${jobId} ended with status ${job.status}`);
    }

    await sleep(5000);
  }
}

type S3Entry = { key: string; size?: number; lastModified?: Date; etag?: string };

async function listObjects(): Promise<S3Entry[]> {
  const resp = await s3.send(
    new ListObjectsV2Command({ Bucket: config.bucket, Prefix: config.prefix }),
  );

  const entries: Array<S3Entry | null> = (resp.Contents || []).map((item: S3Object) => {
    const key = item.Key ?? null;
    if (!key) return null;
    return {
      key,
      size: item.Size ?? undefined,
      lastModified: item.LastModified ?? undefined,
      etag: item.ETag ?? undefined,
    };
  });

  return entries
    .filter((entry): entry is S3Entry => entry !== null)
    .filter((entry) => !entry.key.endsWith("airbyte_meta.json"));
}

function extractRunFolder(key: string): string | null {
  // assumes keys look like `${prefix}YYYY-MM-DD/...` or similar
  const withoutPrefix = key.startsWith(config.prefix) ? key.slice(config.prefix.length) : key;
  const parts = withoutPrefix.split("/").filter(Boolean);
  return parts[0] || null;
}

function filterLatestRun(entries: S3Entry[]): S3Entry[] {
  if (!config.latestRunOnly) return entries;
  const byFolder = new Map<string, { entries: S3Entry[]; newest: number }>();
  for (const entry of entries) {
    const folder = extractRunFolder(entry.key) ?? "__root__";
    const ts = entry.lastModified ? entry.lastModified.getTime() : 0;
    const bucket = byFolder.get(folder) ?? { entries: [], newest: 0 };
    bucket.entries.push(entry);
    bucket.newest = Math.max(bucket.newest, ts);
    byFolder.set(folder, bucket);
  }
  if (byFolder.size === 0) return [];
  let latestFolder = null;
  let latestTs = -1;
  for (const [folder, data] of byFolder.entries()) {
    if (data.newest > latestTs) {
      latestTs = data.newest;
      latestFolder = folder;
    }
  }
  if (!latestFolder) return entries;
  return byFolder.get(latestFolder)?.entries ?? entries;
}

function loadProcessedKeys(): Set<string> {
  const fullPath = path.resolve(config.stateFile);
  try {
    const raw = fs.readFileSync(fullPath, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return new Set(parsed);
    return new Set<string>();
  } catch {
    return new Set<string>();
  }
}

function saveProcessedKeys(keys: Set<string>) {
  const fullPath = path.resolve(config.stateFile);
  fs.writeFileSync(fullPath, JSON.stringify(Array.from(keys), null, 2));
}

async function getObjectBuffer(key: string): Promise<Buffer> {
  const obj = await s3.send(new GetObjectCommand({ Bucket: config.bucket, Key: key }));
  const body = await obj.Body?.transformToByteArray();
  if (!body) throw new Error(`Empty object body for ${key}`);
  return Buffer.from(body);
}

async function maybeGetAirbyteMeta(key: string): Promise<unknown> {
  const metaKey = `${key}.airbyte_meta.json`;
  try {
    const obj = await s3.send(new GetObjectCommand({ Bucket: config.bucket, Key: metaKey }));
    const body = await obj.Body?.transformToByteArray();
    if (!body) return null;
    return JSON.parse(new TextDecoder().decode(body));
  } catch {
    return null;
  }
}

async function run() {
  if (config.skipTrigger) {
    console.log("AIRBYTE_SKIP_TRIGGER is set; skipping sync trigger.");
  } else {
    await triggerAndWait();
  }

  console.log("Listing S3 objects produced by Airbyte...");
  const entriesAll = await listObjects();
  const entries = filterLatestRun(entriesAll);
  console.log(
    `Found ${entriesAll.length} candidate objects under prefix ${config.prefix} ` +
      `(processing ${entries.length} from ${config.latestRunOnly ? "latest run folder" : "all runs"})`,
  );

  const processed = loadProcessedKeys();
  const newEntries = entries.filter((entry) => !processed.has(entry.key));
  console.log(`Filtered to ${newEntries.length} new objects (state file: ${config.stateFile})`);

  if (config.seedStateOnly) {
    console.log("AIRBYTE_SEED_STATE_ONLY is set; seeding state with current keys and exiting.");
    newEntries.forEach((entry) => processed.add(entry.key));
    saveProcessedKeys(processed);
    return;
  }

  const now = new Date().toISOString();

  console.log("\n--- Supabase upsert preview ---");
  console.log(
    JSON.stringify(
      {
        connectors: {
          upsert: {
            workspace_id: config.workspaceId,
            airbyte_connection_id: config.connectionId,
            name: config.connectorName,
            type: config.connectorType,
            status: config.connectorStatus,
            updated_at: now,
          },
        },
      },
      null,
      2,
    ),
  );

  for (const entry of newEntries) {
    const key = entry.key;
    const ext = path.extname(key).replace(".", "").toLowerCase();
    const supabaseFileRow = {
      workspace_id: config.workspaceId,
      connector_id: "(lookup connectors.id by airbyte_connection_id)",
      path: key,
      format: ext || null,
      mime_type: mimeFromExt[ext] || null,
      size: entry.size ?? null,
      checksum: entry.etag ?? null,
      is_viewable: true,
      sync_status: "synced",
      last_synced_at: now,
      created_at: now,
      updated_at: now,
    };

    console.log(JSON.stringify({ files: { insert: supabaseFileRow } }, null, 2));

    console.log(`\nProcessing ${key}`);
    if (!config.processEnabled) {
      console.log("- AIRBYTE_PROCESS_ENABLED=false, skipping ingestion");
      processed.add(key);
      continue;
    }

    const data = await getObjectBuffer(key);
    const airbyteMeta = await maybeGetAirbyteMeta(key);

    if (airbyteMeta) {
      console.log(`- Airbyte metadata: ${JSON.stringify(airbyteMeta).slice(0, 200)}...`);
    } else {
      console.log("- No Airbyte metadata found");
    }

    await processDocument({ filename: key, data });
    processed.add(key);
  }

  saveProcessedKeys(processed);
  console.log(`\nUpdated processed state with ${processed.size} total keys`);
}

run().catch((error) => {
  console.error("\n=== AIRBYTE RUNNER FAILED ===");
  console.error(error);
  process.exit(1);
});
