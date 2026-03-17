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
 * Extract image attachments from content blocks, upload them, and return results.
 * Recursively handles tool_result blocks.
 */
export async function extractAndUploadAttachments(
  contentBlocks: ContentBlock[],
  sessionId: string,
  entryId: string,
): Promise<AttachmentResult[]> {
  const results: AttachmentResult[] = [];
  let ordinal = 0;

  async function processBlocks(blocks: ContentBlock[]): Promise<void> {
    for (const block of blocks) {
      try {
        if (block.type === 'image' && block.source) {
          if (block.source.type === 'base64' && block.source.data && block.source.media_type) {
            const buffer = Buffer.from(block.source.data, 'base64');
            const ext = extFromMime(block.source.media_type);
            const filename = `${sessionId}_${entryId}_${ordinal}.${ext}`;
            const metadata = { sessionId, entryId, ordinal, source_type: 'base64' };

            log(`Uploading base64 image: ${filename} (${buffer.length} bytes)`);
            const result = await postAttachment(buffer, block.source.media_type, filename, metadata);
            results.push({ attachmentRecordId: result.record_id, ordinal });
            ordinal++;
          } else if (block.source.type === 'url' && block.source.url) {
            // For URL-based images, try to fetch and upload
            try {
              const res = await fetch(block.source.url);
              if (res.ok) {
                const arrayBuffer = await res.arrayBuffer();
                const buffer = Buffer.from(arrayBuffer);
                const contentType = res.headers.get('content-type') || 'image/png';
                const ext = extFromMime(contentType);
                const filename = `${sessionId}_${entryId}_${ordinal}.${ext}`;
                const metadata = {
                  sessionId,
                  entryId,
                  ordinal,
                  source_type: 'url',
                  original_url: block.source.url,
                };

                log(`Uploading URL image: ${filename} (${buffer.length} bytes)`);
                const result = await postAttachment(buffer, contentType, filename, metadata);
                results.push({ attachmentRecordId: result.record_id, ordinal });
                ordinal++;
              } else {
                logError(`Failed to fetch image URL: ${block.source.url} — ${res.status}`);
              }
            } catch (err) {
              logError(`Error fetching image URL ${block.source.url}: ${(err as Error).message}`);
            }
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
