export type AppConfig = {
  region: string;
  bucket: string;
  airbytePrefix: string;
  messagesPrefix: string;
  detailsPrefix: string;
  rawFilesPrefix: string;
  workspaceId: string; // required
  connectorId: string; // required
};

const required = (name: string) => {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required env var ${name}`);
  return v;
};

export function loadConfig(): AppConfig {
  const airbytePrefix = process.env.AIRBYTE_S3_PREFIX || "raw/";
  const workspaceId = process.env.AIRBYTE_WORKSPACE_ID;
  if (!workspaceId) throw new Error("Missing AIRBYTE_WORKSPACE_ID");
  const connectorId = process.env.AIRBYTE_CONNECTION_ID;
  if (!connectorId) throw new Error("Missing AIRBYTE_CONNECTION_ID");
  return {
    region: required("AWS_REGION"),
    bucket: required("AIRBYTE_S3_BUCKET"),
    airbytePrefix,
    messagesPrefix: process.env.MESSAGES_PREFIX || `${airbytePrefix}messages/`,
    detailsPrefix: process.env.DETAILS_PREFIX || `${airbytePrefix}messages_details/`,
    rawFilesPrefix: process.env.RAW_FILES_PREFIX || `${airbytePrefix}raw-files/`,
    workspaceId,
    connectorId,
  };
}
