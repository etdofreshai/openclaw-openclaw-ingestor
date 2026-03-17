/**
 * Process a single JSONL entry: post message, upload attachments, link them.
 */

import { postMessage, linkAttachment } from './api.js';
import { extractAndUploadAttachments } from './attachments.js';

const MAX_CONTENT_LENGTH = 100_000;

interface ContentBlock {
  type: string;
  text?: string;
  [key: string]: unknown;
}

interface JournalEntry {
  type: string;
  id: string;
  timestamp: string;
  message?: {
    role: string;
    content: string | ContentBlock[];
    model?: string;
  };
}

function log(prefix: string, msg: string): void {
  console.log(`[${prefix}] ${new Date().toISOString()} ${msg}`);
}

function logError(prefix: string, msg: string): void {
  console.error(`[${prefix}] ${new Date().toISOString()} ${msg}`);
}

/**
 * Extract text content from message content (string or content blocks).
 * Joins text blocks, skips tool_use/tool_result blocks for the main text.
 */
function extractTextContent(content: string | ContentBlock[]): string {
  if (typeof content === 'string') {
    return content.slice(0, MAX_CONTENT_LENGTH);
  }

  const texts: string[] = [];
  for (const block of content) {
    if (block.type === 'text' && block.text) {
      texts.push(block.text as string);
    }
  }

  const joined = texts.join('\n');
  return joined.slice(0, MAX_CONTENT_LENGTH);
}

/**
 * Process a single JSONL entry.
 * Returns true if processed, false if skipped.
 */
export async function ingestEntry(
  entry: JournalEntry,
  sessionId: string,
  logPrefix: string = 'ingest',
): Promise<boolean> {
  // Skip non-message entries
  if (entry.type !== 'message' || !entry.message) {
    return false;
  }

  const { role, content, model } = entry.message;
  const entryId = entry.id;
  const timestamp = entry.timestamp;

  // Determine sender/recipient
  const sender = role === 'user' ? 'ET' : 'OpenClaw';
  const recipient = role === 'user' ? 'OpenClaw' : 'ET';

  // Extract text content
  const textContent = extractTextContent(content);
  if (!textContent && (!Array.isArray(content) || content.length === 0)) {
    log(logPrefix, `Skipping empty message ${sessionId}:${entryId}`);
    return false;
  }

  // Post message to API
  const externalId = `${sessionId}:${entryId}`;
  const metadata: Record<string, unknown> = { sessionId, role };
  if (model) metadata.model = model;

  try {
    const result = await postMessage({
      source: 'openclaw',
      external_id: externalId,
      timestamp,
      sender,
      recipient,
      content: textContent || '[attachment only]',
      metadata,
    });

    if (result.duplicate) {
      log(logPrefix, `Duplicate: ${externalId}`);
      return true;
    }

    log(logPrefix, `Ingested: ${externalId} → ${result.record_id}`);

    // Handle attachments if content is an array with image blocks
    if (Array.isArray(content) && result.record_id) {
      const attachments = await extractAndUploadAttachments(
        content as ContentBlock[],
        sessionId,
        entryId,
      );

      for (const att of attachments) {
        try {
          await linkAttachment(result.record_id, att.attachmentRecordId, att.ordinal);
          log(logPrefix, `Linked attachment ${att.attachmentRecordId} to ${result.record_id}`);
        } catch (err) {
          logError(logPrefix, `Failed to link attachment: ${(err as Error).message}`);
        }
      }
    }

    return true;
  } catch (err) {
    logError(logPrefix, `Failed to ingest ${externalId}: ${(err as Error).message}`);
    return false;
  }
}
