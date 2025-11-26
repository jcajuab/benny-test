export type AppConfig = {
  region: string;
  bucket: string;
  airbytePrefix: string;
  messagesPrefix: string;
  detailsPrefix: string;
  rawFilesPrefix: string;
  workspaceId: string;
  connectorId?: string | null;
  connectionId?: string | null;
};

const required = (name: string) => {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var ${name}`);
  return v;
};

export function loadConfig(): AppConfig {
  const airbytePrefix = process.env.AIRBYTE_S3_PREFIX || "raw/";
  const workspaceId = process.env.AIRBYTE_WORKSPACE_ID || process.env.WORKSPACE_ID;
  if (!workspaceId) throw new Error("Missing AIRBYTE_WORKSPACE_ID (or WORKSPACE_ID)");
  return {
    region: required("AWS_REGION"),
    bucket: required("AIRBYTE_S3_BUCKET"),
    airbytePrefix,
    messagesPrefix: process.env.MESSAGES_PREFIX || `${airbytePrefix}messages/`,
    detailsPrefix: process.env.DETAILS_PREFIX || `${airbytePrefix}messages_details/`,
    rawFilesPrefix: process.env.RAW_FILES_PREFIX || `${airbytePrefix}raw-files/`,
    workspaceId,
    connectorId: process.env.CONNECTOR_ID || null,
    connectionId: process.env.AIRBYTE_CONNECTION_ID || null,
  };
}
