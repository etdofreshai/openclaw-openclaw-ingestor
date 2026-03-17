/**
 * Memory Database API client.
 * All writes go through the HTTP API — zero direct Postgres.
 */

const API_URL = process.env.MEMORY_DATABASE_API_URL || 'https://memory-database.etdofresh.com';
const WRITE_TOKEN = process.env.MEMORY_DATABASE_API_WRITE_TOKEN || '';

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 1000;

interface MessagePayload {
  source: string;
  external_id: string;
  timestamp: string;
  sender: string;
  recipient: string;
  content: string;
  metadata: Record<string, unknown>;
}

interface ApiResponse {
  record_id: string;
  [key: string]: unknown;
}

function log(msg: string): void {
  console.log(`[api] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[api] ${new Date().toISOString()} ${msg}`);
}

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function headers(): Record<string, string> {
  const h: Record<string, string> = { 'Content-Type': 'application/json' };
  if (WRITE_TOKEN) {
    h['Authorization'] = `Bearer ${WRITE_TOKEN}`;
  }
  return h;
}

/**
 * Post a message to the Memory DB API.
 * Returns { record_id } on success (201) or duplicate (409).
 * Retries on transient errors with exponential backoff.
 */
export async function postMessage(payload: MessagePayload): Promise<{ record_id: string; duplicate: boolean }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(`${API_URL}/api/messages`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify(payload),
      });

      if (res.status === 201) {
        const body = await res.json() as ApiResponse;
        return { record_id: body.record_id, duplicate: false };
      }

      if (res.status === 200) {
        // API returns 200 for duplicates (existing record returned)
        try {
          const body = await res.json() as ApiResponse;
          // Response may have record_id (UUID) or just id (numeric)
          const recordId = body.record_id || (body.id ? String(body.id) : '');
          return { record_id: recordId, duplicate: true };
        } catch {
          return { record_id: '', duplicate: true };
        }
      }

      if (res.status === 409) {
        // Duplicate — treat as success. Try to get record_id from body.
        try {
          const body = await res.json() as ApiResponse;
          return { record_id: body.record_id || '', duplicate: true };
        } catch {
          return { record_id: '', duplicate: true };
        }
      }

      if (res.status === 429) {
        const retryAfter = res.headers.get('Retry-After');
        const waitMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : BASE_DELAY_MS * Math.pow(2, attempt);
        log(`Rate limited (429), waiting ${waitMs}ms before retry ${attempt}/${MAX_RETRIES}`);
        await sleep(waitMs);
        continue;
      }

      const text = await res.text();
      if (res.status >= 500 && attempt < MAX_RETRIES) {
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Server error ${res.status}, retry ${attempt}/${MAX_RETRIES} in ${waitMs}ms: ${text.slice(0, 200)}`);
        await sleep(waitMs);
        continue;
      }

      throw new Error(`API POST /api/messages failed: ${res.status} — ${text.slice(0, 500)}`);
    } catch (err) {
      if (err instanceof TypeError && attempt < MAX_RETRIES) {
        // Network error (fetch TypeError)
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Network error, retry ${attempt}/${MAX_RETRIES} in ${waitMs}ms: ${(err as Error).message}`);
        await sleep(waitMs);
        continue;
      }
      throw err;
    }
  }

  throw new Error('postMessage: exhausted retries');
}

/**
 * Upload an attachment (multipart/form-data).
 * Returns { record_id, sha256 }.
 */
export async function postAttachment(
  buffer: Buffer,
  mimeType: string,
  filename: string,
  metadata: Record<string, unknown>,
): Promise<{ record_id: string; sha256: string }> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const formData = new FormData();
      const blob = new Blob([buffer], { type: mimeType });
      formData.append('file', blob, filename);
      formData.append('mime_type', mimeType);
      formData.append('original_file_name', filename);
      formData.append('source', 'openclaw');
      formData.append('metadata', JSON.stringify(metadata));

      const h: Record<string, string> = {};
      if (WRITE_TOKEN) {
        h['Authorization'] = `Bearer ${WRITE_TOKEN}`;
      }

      const res = await fetch(`${API_URL}/api/attachments`, {
        method: 'POST',
        headers: h,
        body: formData,
      });

      if (res.status === 201 || res.status === 200) {
        const body = await res.json() as { record_id: string; sha256: string };
        return body;
      }

      if (res.status === 409) {
        // Duplicate attachment
        try {
          const body = await res.json() as { record_id: string; sha256: string };
          return body;
        } catch {
          throw new Error('Duplicate attachment but could not parse record_id');
        }
      }

      if (res.status === 429) {
        const retryAfter = res.headers.get('Retry-After');
        const waitMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : BASE_DELAY_MS * Math.pow(2, attempt);
        log(`Rate limited on attachment upload, waiting ${waitMs}ms`);
        await sleep(waitMs);
        continue;
      }

      const text = await res.text();
      if (res.status >= 500 && attempt < MAX_RETRIES) {
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Server error ${res.status} on attachment, retry ${attempt}/${MAX_RETRIES}`);
        await sleep(waitMs);
        continue;
      }

      throw new Error(`API POST /api/attachments failed: ${res.status} — ${text.slice(0, 500)}`);
    } catch (err) {
      if (err instanceof TypeError && attempt < MAX_RETRIES) {
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Network error on attachment upload, retry ${attempt}/${MAX_RETRIES}`);
        await sleep(waitMs);
        continue;
      }
      throw err;
    }
  }

  throw new Error('postAttachment: exhausted retries');
}

/**
 * Link an attachment to a message.
 */
export async function linkAttachment(
  messageRecordId: string,
  attachmentRecordId: string,
  ordinal: number,
): Promise<void> {
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(`${API_URL}/api/message-attachment-links`, {
        method: 'POST',
        headers: headers(),
        body: JSON.stringify({
          message_record_id: messageRecordId,
          attachment_record_id: attachmentRecordId,
          ordinal,
          role: 'inline',
        }),
      });

      if (res.status === 201 || res.status === 200 || res.status === 409) {
        return; // success or duplicate
      }

      if (res.status === 429) {
        const retryAfter = res.headers.get('Retry-After');
        const waitMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : BASE_DELAY_MS * Math.pow(2, attempt);
        await sleep(waitMs);
        continue;
      }

      const text = await res.text();
      if (res.status >= 500 && attempt < MAX_RETRIES) {
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Server error ${res.status} on link, retry ${attempt}/${MAX_RETRIES}`);
        await sleep(waitMs);
        continue;
      }

      throw new Error(`API POST /api/message-attachment-links failed: ${res.status} — ${text.slice(0, 500)}`);
    } catch (err) {
      if (err instanceof TypeError && attempt < MAX_RETRIES) {
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Network error on link, retry ${attempt}/${MAX_RETRIES}`);
        await sleep(waitMs);
        continue;
      }
      throw err;
    }
  }

  throw new Error('linkAttachment: exhausted retries');
}
