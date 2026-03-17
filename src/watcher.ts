/**
 * Real-time file watcher for OpenClaw JSONL session files.
 * Watches both session dirs, debounces changes, processes new bytes.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { ingestEntry } from './ingest.js';

const SESSION_DIRS = [
  '/data/.openclaw/agents/main/sessions',
  '/data/.openclaw/cron/runs',
];

const STATE_FILE = path.join(process.cwd(), '.watcher-state.json');
const DEBOUNCE_MS = 2000;
const POLL_INTERVAL_MS = 60_000;

interface WatcherState {
  offsets: Record<string, number>; // filePath → byte offset
}

function log(msg: string): void {
  console.log(`[watcher] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[watcher] ${new Date().toISOString()} ${msg}`);
}

function loadState(): WatcherState {
  try {
    if (fs.existsSync(STATE_FILE)) {
      const data = fs.readFileSync(STATE_FILE, 'utf-8');
      return JSON.parse(data) as WatcherState;
    }
  } catch (err) {
    logError(`Failed to load state: ${(err as Error).message}`);
  }
  return { offsets: {} };
}

function saveState(state: WatcherState): void {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (err) {
    logError(`Failed to save state: ${(err as Error).message}`);
  }
}

/**
 * Extract session ID from file path.
 * e.g., /data/.openclaw/agents/main/sessions/abc123.jsonl → abc123
 */
function sessionIdFromPath(filePath: string): string {
  return path.basename(filePath, '.jsonl');
}

/**
 * Process new bytes in a JSONL file since the last known offset.
 */
async function processFile(filePath: string, state: WatcherState): Promise<number> {
  const offset = state.offsets[filePath] || 0;
  let stat: fs.Stats;

  try {
    stat = fs.statSync(filePath);
  } catch {
    return 0;
  }

  if (stat.size <= offset) {
    return 0; // No new data
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
  for (const line of lines) {
    try {
      const entry = JSON.parse(line);
      const ok = await ingestEntry(entry, sessionId, 'watcher');
      if (ok) processed++;
    } catch (err) {
      // Skip malformed lines
      if (line.trim()) {
        logError(`Malformed JSON in ${filePath}: ${(err as Error).message}`);
      }
    }
  }

  state.offsets[filePath] = stat.size;
  saveState(state);

  if (processed > 0) {
    log(`Processed ${processed} entries from ${path.basename(filePath)}`);
  }

  return processed;
}

/**
 * Scan a directory for JSONL files and process any new data.
 */
async function scanDir(dir: string, state: WatcherState): Promise<number> {
  if (!fs.existsSync(dir)) return 0;

  const files = fs.readdirSync(dir).filter(f => f.endsWith('.jsonl'));
  let total = 0;

  for (const file of files) {
    const filePath = path.join(dir, file);
    total += await processFile(filePath, state);
  }

  return total;
}

/**
 * Start the watcher. Returns a cleanup function.
 */
export async function startWatcher(): Promise<() => void> {
  const state = loadState();
  const watchers: fs.FSWatcher[] = [];
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;
  let pollTimer: ReturnType<typeof setInterval> | null = null;
  let processing = false;
  let shutdownRequested = false;

  const processAll = async (): Promise<void> => {
    if (processing || shutdownRequested) return;
    processing = true;

    try {
      for (const dir of SESSION_DIRS) {
        await scanDir(dir, state);
      }
    } catch (err) {
      logError(`Error during scan: ${(err as Error).message}`);
    } finally {
      processing = false;
    }
  };

  const debouncedProcess = (): void => {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      void processAll();
    }, DEBOUNCE_MS);
  };

  // Set up fs.watch on each directory
  for (const dir of SESSION_DIRS) {
    if (!fs.existsSync(dir)) {
      log(`Directory not found, skipping watch: ${dir}`);
      continue;
    }

    try {
      const watcher = fs.watch(dir, { persistent: true }, (eventType, filename) => {
        if (filename && filename.endsWith('.jsonl')) {
          debouncedProcess();
        }
      });

      watcher.on('error', (err) => {
        logError(`Watch error on ${dir}: ${(err as Error).message}`);
      });

      watchers.push(watcher);
      log(`Watching: ${dir}`);
    } catch (err) {
      logError(`Failed to watch ${dir}: ${(err as Error).message}`);
    }
  }

  // Safety poll every 60s
  pollTimer = setInterval(() => {
    debouncedProcess();
  }, POLL_INTERVAL_MS);

  // Initial scan
  log('Running initial scan...');
  await processAll();
  log('Initial scan complete. Watching for changes...');

  // Cleanup function
  const cleanup = (): void => {
    shutdownRequested = true;
    log('Shutting down...');

    if (debounceTimer) clearTimeout(debounceTimer);
    if (pollTimer) clearInterval(pollTimer);

    for (const w of watchers) {
      try {
        w.close();
      } catch {
        // ignore
      }
    }

    saveState(state);
    log('Shutdown complete.');
  };

  return cleanup;
}
