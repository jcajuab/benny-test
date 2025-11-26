# Gmail to S3 to .eml (Unstructured-ready) – Ingestion Flow

This describes the standalone CLI that converts Airbyte Gmail JSONL dumps into canonical `.eml` files in S3. It is designed to be idempotent and ready for downstream Unstructured → embedding → vector DB.

## Overview
- Input: Airbyte Cloud Gmail connection writing JSONL to S3:
  - `raw/messages/` (metadata, currently unused)
  - `raw/messages_details/` (full Gmail payloads)
- Output: `.eml` files in S3 under:
  - `${RAW_FILES_PREFIX}${WORKSPACE_ID}/gmail/${messageId}.eml`
  - Default: `raw/raw-files/<workspace>/gmail/<id>.eml` (prefix derived from `AIRBYTE_S3_PREFIX`)
- Mode: Idempotent. Before writing, we `HeadObject`; if the target exists, we skip.
- Base64url bodies are decoded; we prefer HTML parts, fall back to plain text. Attachments are currently ignored.

## Env Vars (see `.env.example`)
- `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `AIRBYTE_S3_BUCKET` (e.g., `benny-raw-files-test`)
- `AIRBYTE_S3_PREFIX` (e.g., `raw/`)
- `RAW_FILES_PREFIX` (default `${AIRBYTE_S3_PREFIX}raw-files/`, e.g., `raw/raw-files/`)
- `AIRBYTE_WORKSPACE_ID` (or `WORKSPACE_ID`)
- `CONNECTOR_ID` (optional; falls back to `AIRBYTE_CONNECTION_ID`)
- `AIRBYTE_CONNECTION_ID` (used if `CONNECTOR_ID` is not set)

## CLI
```bash
# Preview only (no upload)
bun run src/index.ts --limit 5 --dry-run

# Real upload, process all
bun run src/index.ts

# Flags
--messages-prefix   # override, default ${AIRBYTE_S3_PREFIX}messages/
--details-prefix    # override, default ${AIRBYTE_S3_PREFIX}messages_details/
--limit N           # process at most N messages this run
--dry-run           # skip upload, just log previews
```

## Flow
1) List JSONL files under details prefix (default `raw/messages_details/`).
2) Stream each JSONL line, parse Gmail payload (unwraps `_airbyte_data`/`data`).
3) Choose body part (prefer `text/html`, else `text/plain`), base64url decode.
4) Build `.eml` with headers (From/To/Cc/Bcc/Subject/Date/Message-ID) and body.
5) Target key: `${RAW_FILES_PREFIX}${WORKSPACE_ID}/gmail/${messageId}.eml`.
6) If target exists → skip; else upload (`ContentType: message/rfc822`).
7) Log Supabase-style preview payload for `files` table (includes connector/workspace/path/mime/size).

## Idempotency
- HeadObject check on the target key prevents duplicate uploads per messageId.
- No local state file; re-runs are safe as long as messageIds are stable.

## Integrating into a workflow
- After upload, feed the `.eml` keys to Unstructured partition API; carry metadata (workspace_id, connector_id, messageId, threadId, from/to/cc/bcc/subject/date).
- Embed the returned chunks and upsert into the vector store with payload including messageId/connector/workspace.
- Use `--limit` for throttled runs; use `--dry-run` for inspection.

## Notes / Caveats
- Attachments are ignored (only the first HTML or plain body is used). We can extend to attachments if needed.
- Checksums are currently null; we can populate with S3 ETag or MD5 of the `.eml` if desired.
- The script reads only `messages_details/` (not `messages/`); extend if you need metadata from both.
- Airbyte Gmail output examples (reference):
  - `raw/messages_details/*.jsonl` — full Gmail payloads with `payload`/`parts`/base64url bodies (we consume this).
  - `raw/messages/*.jsonl` — lighter metadata listing (id/threadId/snippet); currently unused but can be inspected for cross-checks.

## Code structure (how it was built)
- `src/index.ts`: CLI entrypoint. Parses flags, loads config, calls `processMessages`.
- `src/config.ts`: Loads env vars, applies defaults (prefixes, workspace/connector).
- `src/s3Client.ts`: AWS S3 client configured with region.
- `src/jsonlReader.ts`: Lists JSONL keys under a prefix; streams JSONL line-by-line from S3.
- `src/emailExtractor.ts`: Unwraps Airbyte envelopes (`_airbyte_data`/`data`), picks first HTML/plain part, base64url-decodes, builds `.eml` string.
- `src/processor.ts`: Orchestrates listing JSONL files, per-line processing, idempotent upload (HeadObject + PutObject), logs Supabase-style previews, respects `--limit` and `--dry-run`.
- `.env.example`: Reference env vars for local runs.

## How the code functions
- Start: `index.ts` parses args, loads config (env + defaults), and calls `processMessages`.
- Processing:
  1) `processor.ts` uses `listJsonlKeys` to find JSONL files under the details prefix.
  2) For each JSONL key, `readJsonlFromS3` streams lines; each non-empty line is parsed as a Gmail message.
  3) `emailExtractor.extractEmail` unwraps `_airbyte_data`/`data`, finds headers, selects body part (HTML preferred), decodes base64url, and assembles `.eml` (via `toEml`).
  4) Target S3 key is derived from `RAW_FILES_PREFIX`, `WORKSPACE_ID`, and `messageId`.
  5) `HeadObject` checks if it exists—skip if yes. Otherwise, PutObject (or preview-only if `--dry-run`).
  6) Logs a JSON “preview” shaped like a `files` table insert (path, mime, size, connector/workspace).
- End: logs summary counts (processed/created/skipped/failed).

## How to use (quick recap)
- Set envs (see `.env.example`), ensure Airbyte Gmail dumps exist in S3 under `raw/messages_details/`.
- Preview only: `bun run src/index.ts --limit 5 --dry-run`
- Real upload: `bun run src/index.ts` (optionally `--limit N`)
- Output path: `${RAW_FILES_PREFIX}${WORKSPACE_ID}/gmail/${messageId}.eml` (default `raw/raw-files/<workspace>/gmail/<id>.eml`)
- Idempotent: reruns skip existing keys via HeadObject.
