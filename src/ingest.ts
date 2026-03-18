/**
 * Process a single message from the OpenClaw API: post message, upload attachments, link them.
 * Supports backfill options: dryRun, overwrite, attachmentsOnly, includeAttachments.
 */

import { postMessage, patchMessage, linkAttachment, findMessageByExternalId } from './api.js';
import { extractAndUploadAttachments } from './attachments.js';
import type { MessageInfo, ContentBlock } from './openclaw-client.js';

const MAX_CONTENT_LENGTH = 100_000;

export interface BackfillOptions {
  full?: boolean;              // reset state and reprocess all sessions (default: true for backfill)
  dryRun?: boolean;            // process everything but don't write to Memory DB API — just count/report
  overwrite?: boolean;         // on 409 duplicate, PATCH/PUT the existing record with new content
  attachmentsOnly?: boolean;   // skip message upsert, only upload attachments for already-ingested messages
  includeAttachments?: boolean; // whether to process attachment blobs (default: true)
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
  options?: BackfillOptions,
): Promise<boolean> {
  const { id: messageId, role, content, model, timestamp } = message;
  const dryRun = options?.dryRun ?? false;
  const overwrite = options?.overwrite ?? false;
  const attachmentsOnly = options?.attachmentsOnly ?? false;
  const includeAttachments = options?.includeAttachments ?? true;

  const dryTag = dryRun ? '[dry-run] ' : '';

  // Determine sender/recipient
  const sender = role === 'user' ? 'ET' : 'OpenClaw';
  const recipient = role === 'user' ? 'OpenClaw' : 'ET';

  // Extract text content
  const textContent = extractTextContent(content);
  if (!textContent && (!Array.isArray(content) || content.length === 0)) {
    log(logPrefix, `${dryTag}Skipping empty message ${sessionKey}:${messageId}`);
    return false;
  }

  const externalId = `${sessionKey}:${messageId}`;
  const metadata: Record<string, unknown> = { sessionId: sessionKey, sessionKey, role };
  if (model) metadata.model = model;

  const ts = timestamp
    ? (typeof timestamp === 'number' || /^\d+$/.test(String(timestamp))
        ? new Date(Number(timestamp)).toISOString()
        : String(timestamp))
    : new Date().toISOString();

  const messagePayload = {
    source: 'openclaw' as const,
    external_id: externalId,
    timestamp: ts,
    sender,
    recipient,
    content: textContent || '[attachment only]',
    metadata,
  };

  let messageRecordId: string = '';

  // ── Attachments-only mode ──
  if (attachmentsOnly) {
    // Skip message upsert; look up the existing record for attachment linking
    if (includeAttachments && Array.isArray(content)) {
      const existing = await findMessageByExternalId(externalId);
      if (!existing) {
        log(logPrefix, `${dryTag}No existing record for ${externalId} — skipping attachments-only`);
        return false;
      }
      messageRecordId = existing.record_id;
      log(logPrefix, `${dryTag}Attachments-only: found ${externalId} → ${messageRecordId}`);
    } else {
      log(logPrefix, `${dryTag}Attachments-only but includeAttachments=false — nothing to do for ${externalId}`);
      return false;
    }
  } else {
    // ── Normal message ingest (or dry-run) ──
    if (dryRun) {
      log(logPrefix, `${dryTag}Would ingest: ${externalId} (${textContent.length} chars)`);
      // For dry run, still count attachments if enabled
      if (includeAttachments && Array.isArray(content)) {
        const attachments = await extractAndUploadAttachments(
          content as ContentBlock[],
          sessionKey,
          messageId,
          { dryRun: true },
        );
        if (attachments.length > 0) {
          log(logPrefix, `${dryTag}Would upload ${attachments.length} attachment(s) for ${externalId}`);
        }
      }
      return true;
    }

    try {
      const result = await postMessage(messagePayload);

      if (result.duplicate) {
        if (overwrite) {
          // Attempt to update the existing record
          log(logPrefix, `Duplicate ${externalId} — overwrite mode, attempting update...`);
          const patchResult = await patchMessage(externalId, messagePayload);
          if (patchResult.updated) {
            log(logPrefix, `Updated: ${externalId} → ${patchResult.record_id}`);
            messageRecordId = patchResult.record_id;
          } else {
            log(logPrefix, `Update not supported for ${externalId}, skipping overwrite`);
            messageRecordId = result.record_id;
          }
        } else {
          log(logPrefix, `Duplicate: ${externalId}`);
          messageRecordId = result.record_id;
          // For duplicates without overwrite, still allow attachment processing
          // (message exists, we might want to add missing attachments)
        }
      } else {
        log(logPrefix, `Ingested: ${externalId} → ${result.record_id}`);
        messageRecordId = result.record_id;
      }
    } catch (err) {
      logError(logPrefix, `Failed to ingest ${externalId}: ${(err as Error).message}`);
      return false;
    }
  }

  // ── Handle attachments ──
  if (includeAttachments && Array.isArray(content) && messageRecordId) {
    const attachments = await extractAndUploadAttachments(
      content as ContentBlock[],
      sessionKey,
      messageId,
    );

    for (const att of attachments) {
      try {
        await linkAttachment(messageRecordId, att.attachmentRecordId, att.ordinal);
        log(logPrefix, `Linked attachment ${att.attachmentRecordId} to ${messageRecordId}`);
      } catch (err) {
        logError(logPrefix, `Failed to link attachment: ${(err as Error).message}`);
      }
    }
  }

  return true;
}
