/**
 * Process a single message from the OpenClaw API: post message, upload attachments, link them.
 */

import { postMessage, linkAttachment } from './api.js';
import { extractAndUploadAttachments } from './attachments.js';
import type { MessageInfo, ContentBlock } from './openclaw-client.js';

const MAX_CONTENT_LENGTH = 100_000;

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
      texts.push(block.text);
    }
  }

  const joined = texts.join('\n');
  return joined.slice(0, MAX_CONTENT_LENGTH);
}

/**
 * Process a single message from the OpenClaw API.
 * Returns true if processed, false if skipped.
 */
export async function ingestMessage(
  message: MessageInfo,
  sessionKey: string,
  logPrefix: string = 'ingest',
): Promise<boolean> {
  const { id: messageId, role, content, model, timestamp } = message;

  // Determine sender/recipient
  const sender = role === 'user' ? 'ET' : 'OpenClaw';
  const recipient = role === 'user' ? 'OpenClaw' : 'ET';

  // Extract text content
  const textContent = extractTextContent(content);
  if (!textContent && (!Array.isArray(content) || content.length === 0)) {
    log(logPrefix, `Skipping empty message ${sessionKey}:${messageId}`);
    return false;
  }

  // Post message to API
  const externalId = `${sessionKey}:${messageId}`;
  const metadata: Record<string, unknown> = { sessionId: sessionKey, role };
  if (model) metadata.model = model;

  try {
    const result = await postMessage({
      source: 'openclaw',
      external_id: externalId,
      timestamp: timestamp || new Date().toISOString(),
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
        sessionKey,
        messageId,
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
