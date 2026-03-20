/**
 * JSONL file backfill: parse local OpenClaw session transcript files
 * and ingest messages into the Memory Database API.
 *
 * Each .jsonl file is one session with lines like:
 *   {"type":"session","id":"<uuid>","timestamp":"..."}
 *   {"type":"message","id":"<id>","timestamp":"...","message":{"role":"user","content":[...]}}
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import * as readline from 'node:readline';
import { postMessage, patchMessage } from './api.js';

/* ── Types ───────────────────────────────────────────── */

export interface JsonlBackfillOptions {
  path?: string;       // directory containing .jsonl files
  dryRun?: boolean;    // count only, don't write
  overwrite?: boolean; // overwrite existing (duplicate) messages
  limit?: number;      // max number of files to process
}

export interface JsonlBackfillStatus {
  running: boolean;
  runId: string | null;
  startedAt: string | null;
  completedAt: string | null;
  totalFiles: number;
  filesProcessed: number;
  filesSkipped: number;
  filesErrored: number;
  messagesIngested: number;
  messagesDuplicate: number;
  messagesSkipped: number;
  messagesErrored: number;
  currentFile: string | null;
  options: JsonlBackfillOptions;
  errors: string[];
}

interface JsonlSessionLine {
  type: 'session';
  id?: string;
  timestamp?: string;
  sessionKey?: string;
  kind?: string;
  label?: string;
  [key: string]: unknown;
}

interface JsonlContentBlock {
  type: string;
  text?: string;
  thinking?: string;
  source?: { type: string; media_type?: string; data?: string; url?: string };
  content?: JsonlContentBlock[] | string;
  [key: string]: unknown;
}

interface JsonlMessageLine {
  type: 'message';
  id: string;
  parentId?: string | null;
  timestamp: string;
  message: {
    role: string;
    content: string | JsonlContentBlock[];
    timestamp?: number;
    model?: string;
    api?: string;
    provider?: string;
    usage?: unknown;
    stopReason?: string;
  };
}

type JsonlLine = JsonlSessionLine | JsonlMessageLine | { type: string; [key: string]: unknown };

const DEFAULT_PATH = '/data/.openclaw/agents/main/sessions/';
const MAX_CONTENT_LENGTH = 100_000;

/* ── Singleton status ────────────────────────────────── */

const status: JsonlBackfillStatus = {
  running: false,
  runId: null,
  startedAt: null,
  completedAt: null,
  totalFiles: 0,
  filesProcessed: 0,
  filesSkipped: 0,
  filesErrored: 0,
  messagesIngested: 0,
  messagesDuplicate: 0,
  messagesSkipped: 0,
  messagesErrored: 0,
  currentFile: null,
  options: {},
  errors: [],
};

export function getJsonlBackfillStatus(): JsonlBackfillStatus {
  return { ...status, errors: [...status.errors] };
}

/* ── Helpers ──────────────────────────────────────────── */

function log(msg: string): void {
  console.log(`[jsonl-backfill] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[jsonl-backfill] ${new Date().toISOString()} ${msg}`);
}

function generateRunId(): string {
  return `jsonl-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Extract the session UUID from a filename like "215b2383-6477-4214-949d-a011b8201042.jsonl"
 */
function sessionIdFromFilename(filename: string): string {
  return path.basename(filename, '.jsonl');
}

/**
 * Extract text content from JSONL content blocks.
 * Skips images, tool calls, tool results, thinking blocks — text only.
 */
function extractText(content: string | JsonlContentBlock[]): string {
  if (typeof content === 'string') {
    return content.slice(0, MAX_CONTENT_LENGTH);
  }

  const texts: string[] = [];
  for (const block of content) {
    if (block.type === 'text' && block.text) {
      texts.push(block.text);
    }
    // Skip: thinking, toolCall, toolResult, image, custom, etc.
  }

  return texts.join('\n').slice(0, MAX_CONTENT_LENGTH);
}

/**
 * Determine if a message role should be ingested.
 * We only want user and assistant messages.
 */
function isIngestableRole(role: string): boolean {
  return role === 'user' || role === 'assistant';
}

/* ── File Parser ─────────────────────────────────────── */

interface ParsedSession {
  sessionInfo: JsonlSessionLine | null;
  messages: JsonlMessageLine[];
}

async function parseJsonlFile(filePath: string): Promise<ParsedSession> {
  const result: ParsedSession = { sessionInfo: null, messages: [] };

  const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    try {
      const parsed = JSON.parse(trimmed) as JsonlLine;

      if (parsed.type === 'session' && !result.sessionInfo) {
        result.sessionInfo = parsed as JsonlSessionLine;
      } else if (parsed.type === 'message') {
        const msgLine = parsed as JsonlMessageLine;
        // Only ingest user/assistant messages (skip toolResult, system, etc.)
        if (msgLine.message && isIngestableRole(msgLine.message.role)) {
          result.messages.push(msgLine);
        }
      }
    } catch {
      // Skip malformed lines
    }
  }

  return result;
}

/* ── Core Backfill Logic ─────────────────────────────── */

async function processFile(
  filePath: string,
  options: JsonlBackfillOptions,
): Promise<{ ingested: number; duplicates: number; skipped: number; errors: number }> {
  const counts = { ingested: 0, duplicates: 0, skipped: 0, errors: 0 };
  const sessionId = sessionIdFromFilename(filePath);
  const dryRun = options.dryRun ?? false;
  const overwrite = options.overwrite ?? false;
  const dryTag = dryRun ? '[dry-run] ' : '';

  const { sessionInfo, messages } = await parseJsonlFile(filePath);

  if (messages.length === 0) {
    log(`${dryTag}No ingestable messages in ${path.basename(filePath)}`);
    return counts;
  }

  // Build metadata context from session info
  const sessionKey = sessionInfo?.sessionKey || sessionInfo?.id || sessionId;
  const sessionMeta: Record<string, unknown> = {
    sessionId,
    sessionKey,
  };
  if (sessionInfo?.kind) sessionMeta.kind = sessionInfo.kind;
  if (sessionInfo?.label) sessionMeta.label = sessionInfo.label;

  for (const msgLine of messages) {
    const { id: messageId, parentId, timestamp: lineTimestamp, message } = msgLine;
    const { role, content, model, provider } = message;

    // Extract text
    const textContent = extractText(content);
    if (!textContent) {
      counts.skipped++;
      continue;
    }

    // Determine sender/recipient
    const sender = role === 'user' ? 'ET' : 'OpenClaw';
    const recipient = role === 'user' ? 'OpenClaw' : 'ET';

    // Determine timestamp: prefer the line-level ISO timestamp, fall back to message.timestamp (epoch ms)
    let ts: string;
    if (lineTimestamp) {
      ts = lineTimestamp;
    } else if (message.timestamp) {
      ts = new Date(message.timestamp).toISOString();
    } else {
      ts = new Date().toISOString();
    }

    const externalId = `openclaw:${sessionId}:${messageId}`;

    const metadata: Record<string, unknown> = {
      ...sessionMeta,
      messageId,
      role,
    };
    if (parentId) metadata.parentId = parentId;
    if (model) metadata.model = model;
    if (provider) metadata.provider = provider;

    const payload = {
      source: 'openclaw',
      external_id: externalId,
      timestamp: ts,
      sender,
      recipient,
      content: textContent,
      metadata,
    };

    if (dryRun) {
      counts.ingested++;
      continue;
    }

    try {
      const result = await postMessage(payload);

      if (result.duplicate) {
        counts.duplicates++;
        if (overwrite) {
          try {
            const patchResult = await patchMessage(externalId, payload);
            if (patchResult.updated) {
              log(`Updated: ${externalId}`);
            }
          } catch {
            // patch failed, that's OK — duplicate still counted
          }
        }
      } else {
        counts.ingested++;
      }
    } catch (err) {
      counts.errors++;
      const errMsg = `Failed ${externalId}: ${(err as Error).message}`;
      logError(errMsg);
      if (status.errors.length < 100) {
        status.errors.push(errMsg);
      }
    }
  }

  return counts;
}

/**
 * Run the JSONL backfill. Processes all .jsonl files in the given directory.
 * This runs asynchronously — call it with `void` from the HTTP handler.
 */
export async function runJsonlBackfill(options: JsonlBackfillOptions = {}): Promise<void> {
  if (status.running) return;

  const dirPath = options.path || DEFAULT_PATH;
  const dryRun = options.dryRun ?? false;
  const limit = options.limit;
  const dryTag = dryRun ? '[dry-run] ' : '';

  // Reset status
  status.running = true;
  status.runId = generateRunId();
  status.startedAt = new Date().toISOString();
  status.completedAt = null;
  status.totalFiles = 0;
  status.filesProcessed = 0;
  status.filesSkipped = 0;
  status.filesErrored = 0;
  status.messagesIngested = 0;
  status.messagesDuplicate = 0;
  status.messagesSkipped = 0;
  status.messagesErrored = 0;
  status.currentFile = null;
  status.options = { ...options };
  status.errors = [];

  log(`${dryTag}Starting JSONL backfill from ${dirPath}`);

  try {
    // List all .jsonl files
    if (!fs.existsSync(dirPath)) {
      throw new Error(`Directory not found: ${dirPath}`);
    }

    const allFiles = fs.readdirSync(dirPath)
      .filter(f => f.endsWith('.jsonl'))
      .sort(); // alphabetical (UUID-based, so roughly random but deterministic)

    status.totalFiles = allFiles.length;
    log(`${dryTag}Found ${allFiles.length} .jsonl files`);

    const filesToProcess = limit ? allFiles.slice(0, limit) : allFiles;
    log(`${dryTag}Processing ${filesToProcess.length} files${limit ? ` (limited to ${limit})` : ''}`);

    for (const filename of filesToProcess) {
      const filePath = path.join(dirPath, filename);
      status.currentFile = filename;

      try {
        const counts = await processFile(filePath, options);
        status.filesProcessed++;
        status.messagesIngested += counts.ingested;
        status.messagesDuplicate += counts.duplicates;
        status.messagesSkipped += counts.skipped;
        status.messagesErrored += counts.errors;

        // Log progress every 50 files
        if (status.filesProcessed % 50 === 0) {
          log(
            `${dryTag}Progress: ${status.filesProcessed}/${filesToProcess.length} files, ` +
            `${status.messagesIngested} ingested, ${status.messagesDuplicate} dupes, ` +
            `${status.messagesErrored} errors`,
          );
        }
      } catch (err) {
        status.filesErrored++;
        const errMsg = `File ${filename}: ${(err as Error).message}`;
        logError(errMsg);
        if (status.errors.length < 100) {
          status.errors.push(errMsg);
        }
      }
    }
  } catch (err) {
    const errMsg = `Fatal: ${(err as Error).message}`;
    logError(errMsg);
    status.errors.push(errMsg);
  } finally {
    status.running = false;
    status.currentFile = null;
    status.completedAt = new Date().toISOString();
    log(
      `${dryTag}Complete: ${status.filesProcessed} files, ` +
      `${status.messagesIngested} ingested, ${status.messagesDuplicate} duplicates, ` +
      `${status.messagesSkipped} skipped, ${status.messagesErrored} errors`,
    );
  }
}
