import { Buffer } from "node:buffer";

export interface GmailHeader {
  name: string;
  value: string;
}

export interface GmailBody {
  size?: number;
  data?: string | null;
}

export interface GmailPart {
  mimeType?: string;
  filename?: string;
  headers?: GmailHeader[];
  body?: GmailBody;
  parts?: GmailPart[];
}

export interface GmailMessage {
  id?: string;
  data?: GmailMessage; // handle nested data envelope
  _airbyte_data?: GmailMessage; // Airbyte envelope
  threadId?: string;
  payload?: GmailPart;
  snippet?: string;
}

function base64UrlDecode(data: string): string {
  const padded = data.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (data.length % 4)) % 4);
  return Buffer.from(padded, "base64").toString("utf8");
}

function findHeader(headers: GmailHeader[] | undefined, name: string): string | undefined {
  const h = headers?.find((hh) => hh.name?.toLowerCase() === name.toLowerCase());
  return h?.value;
}

function findPart(part: GmailPart | undefined, preferHtml = true): { mime: string; data: string } | null {
  if (!part) return null;
  const isMultipart = part.mimeType?.toLowerCase().startsWith("multipart/");
  if (!isMultipart && part.body?.data) {
    return { mime: part.mimeType || "text/plain", data: part.body.data };
  }
  const parts = part.parts || [];
  if (preferHtml) {
    const html = parts.find((p) => (p.mimeType || "").toLowerCase() === "text/html" && p.body?.data);
    if (html?.body?.data) return { mime: "text/html", data: html.body.data };
  }
  const plain = parts.find((p) => (p.mimeType || "").toLowerCase() === "text/plain" && p.body?.data);
  if (plain?.body?.data) return { mime: "text/plain", data: plain.body.data };
  // Recurse
  for (const p of parts) {
    const found = findPart(p, preferHtml);
    if (found) return found;
  }
  return null;
}

export type ExtractedEmail = {
  messageId: string;
  subject?: string;
  from?: string;
  to?: string;
  cc?: string;
  bcc?: string;
  date?: string;
  bodyMime: "text/html" | "text/plain";
  body: string;
  snippet?: string;
};

export function extractEmail(msg: GmailMessage): ExtractedEmail {
  const core: GmailMessage = msg._airbyte_data ?? msg.data ?? msg;
  if (!core.id) throw new Error("Missing message id");
  const headers = core.payload?.headers || [];
  const part = findPart(core.payload, true);
  if (!part) throw new Error("No body found");
  const decoded = base64UrlDecode(part.data);
  const bodyMime = part.mime.toLowerCase().includes("html") ? "text/html" : "text/plain";

  const get = (name: string) => findHeader(headers, name);

  return {
    messageId: core.id,
    subject: get("Subject"),
    from: get("From"),
    to: get("To"),
    cc: get("Cc"),
    bcc: get("Bcc"),
    date: get("Date"),
    bodyMime,
    body: decoded,
    snippet: msg.snippet,
  };
}

export function toEml(email: ExtractedEmail): string {
  const headers = [
    email.from ? `From: ${email.from}` : null,
    email.to ? `To: ${email.to}` : null,
    email.cc ? `Cc: ${email.cc}` : null,
    email.bcc ? `Bcc: ${email.bcc}` : null,
    email.subject ? `Subject: ${email.subject}` : null,
    email.date ? `Date: ${email.date}` : null,
    `Message-ID: <${email.messageId}@gmail>`,
    "MIME-Version: 1.0",
    `Content-Type: ${email.bodyMime}; charset=\"UTF-8\"`,
  ]
    .filter(Boolean)
    .join("\r\n");

  return `${headers}\r\n\r\n${email.body}`;
}
