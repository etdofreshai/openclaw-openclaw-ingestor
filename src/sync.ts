/**
 * One-shot full sync: process all JSONL files and report counts.
 * Usage: node dist/sync.js [--full]
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { ingestEntry } from './ingest.js';

const SESSION_DIRS = [
  '/data/.openclaw/agents/main/sessions',
  '/data/.openclaw/cron/runs',
];

const STATE_FILE = path.join(process.cwd(), '.sync-state.json');

interface SyncState {
  offsets: Record<string, number>; // filePath → byte offset
  lastRun: string;
}

function log(msg: string): void {
  console.log(`[sync] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[sync] ${new Date().toISOString()} ${msg}`);
}

function loadState(): SyncState {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = fs.readFileSync(STATE_FILE, 'utf-8');
      return JSON.parse(data) as SyncState;
    }
  } catch (err) {
    logError(`Failed to load state: ${(err as Error).message}`);
  }
  return { offsets: {}, lastRun: '' };
}

function saveState(state: SyncState): void {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
    log(`State saved to ${STATE_FILE}`);
  } catch (err) {
    logError(`Failed to save state: ${(err as Error).message}`);
  }
}

function sessionIdFromPath(filePath: string): string {
  return path.basename(filePath, '.jsonl');
}

async function processFile(
  filePath: string,
  state: SyncState,
): Promise<{ processed: number; skipped: number; errors: number }> {
  const offset = state.offsets[filePath] || 0;
  let stat: fs.Stats;

  try {
    stat = fs.statSync(filePath);
  } catch {
    return { processed: 0, skipped: 0, errors: 0 };
  }

  if (stat.size <= offset) {
    return { processed: 0, skipped: 0, errors: 0 };
  }

  const sessionId = sessionIdFromPath(filePath);
  const fd = fs.openSync(filePath, 'r');
  const bufferSize = stat.size - offset;
  const buffer = Buffer.alloc(bufferSize);
  fs.readSync(fd, buffer, 0, bufferSize, offset);
  fs.closeSync(fd);

  const newData = buffer.toString('utf-8');
  const lines = newData.split('\n').filter(l => l.trim());

  let processed = 0;
  let skipped = 0;
  let errors = 0;

  for (const line of lines) {
    try {
      const entry = JSON.parse(line);
      const ok = await ingestEntry(entry, sessionId, 'sync');
      if (ok) {
        processed++;
      } else {
        skipped++;
      }
    } catch (err) {
      if (line.trim()) {
        errors++;
        logError(`Malformed JSON in ${path.basename(filePath)}: ${(err as Error).message}`);
      }
    }
  }

  state.offsets[filePath] = stat.size;
  return { processed, skipped, errors };
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const fullSync = args.includes('--full');

  log(`Starting ${fullSync ? 'FULL' : 'incremental'} sync...`);

  let state: SyncState;
  if (fullSync) {
    log('Full sync requested — resetting state');
    state = { offsets: {}, lastRun: '' };
  } else {
    state = loadState();
    if (state.lastRun) {
      log(`Last run: ${state.lastRun}`);
    }
  }

  let totalProcessed = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let totalFiles = 0;

  for (const dir of SESSION_DIRS) {
    if (!fs.existsSync(dir)) {
      log(`Directory not found, skipping: ${dir}`);
      continue;
    }

    const files = fs.readdirSync(dir).filter(f => f.endsWith('.jsonl'));
    log(`Found ${files.length} JSONL files in ${dir}`);

    for (const file of files) {
      const filePath = path.join(dir, file);
      const counts = await processFile(filePath, state);
      totalProcessed += counts.processed;
      totalSkipped += counts.skipped;
      totalErrors += counts.errors;
      totalFiles++;
    }
  }

  state.lastRun = new Date().toISOString();
  saveState(state);

  log('=== Sync Complete ===');
  log(`Files scanned: ${totalFiles}`);
  log(`Messages processed: ${totalProcessed}`);
  log(`Entries skipped (non-message): ${totalSkipped}`);
  log(`Errors: ${totalErrors}`);
}

main().catch(err => {
  logError(`Fatal error: ${(err as Error).message}`);
  process.exit(1);
});
