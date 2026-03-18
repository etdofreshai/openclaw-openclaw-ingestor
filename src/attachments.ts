/**
 * Extract and upload attachments from OpenClaw message content blocks.
 */

import { postAttachment } from './api.js';

interface ContentBlock {
  type: string;
  text?: string;
  source?: {
    type: string;
    media_type?: string;
    data?: string;
    url?: string;
  };
  content?: ContentBlock[] | string;
  name?: string;
  input?: unknown;
  [key: string]: unknown;
}

interface AttachmentResult {
  attachmentRecordId: string;
  ordinal: number;
}

function log(msg: string): void {
  console.log(`[attachments] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[attachments] ${new Date().toISOString()} ${msg}`);
}

/**
 * Determine file extension from MIME type.
 */
function extFromMime(mime: string): string {
  const map: Record<string, string> = {
    'image/png': 'png',
    'image/jpeg': 'jpg',
    'image/gif': 'gif',
    'image/webp': 'webp',
    'image/svg+xml': 'svg',
    'application/pdf': 'pdf',
  };
  return map[mime] || 'bin';
}

/**
 * Extract image/document attachments from content blocks, upload them, and return results.
 * Recursively handles tool_result blocks and nested content.
 * When dryRun is true, counts attachments without uploading.
 */
export async function extractAndUploadAttachments(
  contentBlocks: ContentBlock[],
  sessionId: string,
  entryId: string,
  options?: { dryRun?: boolean },
): Promise<AttachmentResult[]> {
  const results: AttachmentResult[] = [];
  let ordinal = 0;
  const dryRun = options?.dryRun ?? false;

  async function uploadBase64Block(
    data: string,
    mediaType: string,
    sourceType: string,
  ): Promise<void> {
    const buffer = Buffer.from(data, 'base64');
    const ext = extFromMime(mediaType);
    const filename = `${sessionId}_${entryId}_${ordinal}.${ext}`;
    const metadata = { sessionId, entryId, ordinal, source_type: sourceType };

    if (dryRun) {
      log(`[dry-run] Would upload ${sourceType} ${mediaType}: ${filename} (${buffer.length} bytes)`);
      results.push({ attachmentRecordId: `dry-run-${ordinal}`, ordinal });
      ordinal++;
      return;
    }

    log(`Uploading ${sourceType} ${mediaType}: ${filename} (${buffer.length} bytes)`);
    const result = await postAttachment(buffer, mediaType, filename, metadata);
    results.push({ attachmentRecordId: result.record_id, ordinal });
    ordinal++;
  }

  async function uploadUrlBlock(url: string): Promise<void> {
    if (dryRun) {
      log(`[dry-run] Would fetch and upload URL: ${url}`);
      results.push({ attachmentRecordId: `dry-run-${ordinal}`, ordinal });
      ordinal++;
      return;
    }

    try {
      const res = await fetch(url);
      if (res.ok) {
        const arrayBuffer = await res.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);
        const contentType = res.headers.get('content-type') || 'application/octet-stream';
        const ext = extFromMime(contentType);
        const filename = `${sessionId}_${entryId}_${ordinal}.${ext}`;
        const metadata = {
          sessionId,
          entryId,
          ordinal,
          source_type: 'url',
          original_url: url,
        };

        log(`Uploading URL attachment: ${filename} (${buffer.length} bytes)`);
        const result = await postAttachment(buffer, contentType, filename, metadata);
        results.push({ attachmentRecordId: result.record_id, ordinal });
        ordinal++;
      } else {
        logError(`Failed to fetch URL: ${url} — ${res.status}`);
      }
    } catch (err) {
      logError(`Error fetching URL ${url}: ${(err as Error).message}`);
    }
  }

  async function processBlocks(blocks: ContentBlock[]): Promise<void> {
    for (const block of blocks) {
      try {
        // Handle image blocks
        if (block.type === 'image' && block.source) {
          if (block.source.type === 'base64' && block.source.data && block.source.media_type) {
            await uploadBase64Block(block.source.data, block.source.media_type, 'base64-image');
          } else if (block.source.type === 'url' && block.source.url) {
            await uploadUrlBlock(block.source.url);
          }
        }

        // Handle document blocks (PDFs, etc.)
        if (block.type === 'document' && block.source) {
          if (block.source.type === 'base64' && block.source.data && block.source.media_type) {
            await uploadBase64Block(block.source.data, block.source.media_type, 'base64-document');
          } else if (block.source.type === 'url' && block.source.url) {
            await uploadUrlBlock(block.source.url);
          }
        }

        // Recurse into tool_result blocks
        if (block.type === 'tool_result' && block.content) {
          if (Array.isArray(block.content)) {
            await processBlocks(block.content as ContentBlock[]);
          }
        }
      } catch (err) {
        logError(`Error processing attachment block: ${(err as Error).message}`);
        // Continue with other attachments
      }
    }
  }

  await processBlocks(contentBlocks);
  return results;
}
