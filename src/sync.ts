/**
 * One-shot pull-based sync: fetch sessions and messages from the OpenClaw API.
 * Usage: node dist/sync.js [--full]
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { listSessions, getSessionHistory } from './openclaw-client.js';
import { ingestMessage } from './ingest.js';

const STATE_FILE = path.join(process.cwd(), '.sync-state.json');
const MAX_SESSIONS = 500;
const MESSAGES_PER_SESSION = 500;

interface SyncState {
  /** sessionKey → lastMessageId (most recent message we've seen) */
  sessions: Record<string, string>;
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
  return { sessions: {}, lastRun: '' };
}

function saveState(state: SyncState): void {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
    log(`State saved to ${STATE_FILE}`);
  } catch (err) {
    logError(`Failed to save state: ${(err as Error).message}`);
  }
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const fullSync = args.includes('--full');

  // Validate env
  if (!process.env.OPENCLAW_URL || !process.env.OPENCLAW_TOKEN) {
    logError('OPENCLAW_URL and OPENCLAW_TOKEN must be set');
    process.exit(1);
  }

  log(`Starting ${fullSync ? 'FULL' : 'incremental'} sync...`);
  log(`OpenClaw API: ${process.env.OPENCLAW_URL}`);

  let state: SyncState;
  if (fullSync) {
    log('Full sync requested — resetting state');
    state = { sessions: {}, lastRun: '' };
  } else {
    state = loadState();
    if (state.lastRun) {
      log(`Last run: ${state.lastRun}`);
    }
  }

  // 1. List all sessions (paginate up to MAX_SESSIONS)
  log('Fetching session list...');
  const sessions = await listSessions({ limit: MAX_SESSIONS });
  log(`Found ${sessions.length} sessions`);

  let totalProcessed = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  let totalSessions = 0;

  // 2. For each session, fetch history and ingest
  for (const session of sessions) {
    const { sessionKey } = session;
    totalSessions++;

    try {
      const lastMessageId = state.sessions[sessionKey];
      const afterOpt = (!fullSync && lastMessageId) ? { after: lastMessageId } : undefined;

      const messages = await getSessionHistory(sessionKey, {
        limit: MESSAGES_PER_SESSION,
        ...afterOpt,
      });

      if (messages.length === 0) {
        continue;
      }

      log(`Session ${sessionKey}: ${messages.length} messages to process`);

      let latestId = lastMessageId || '';

      for (const msg of messages) {
        // Skip messages we've already seen (by ID comparison)
        if (!fullSync && lastMessageId && msg.id <= lastMessageId) {
          totalSkipped++;
          continue;
        }

        try {
          const ok = await ingestMessage(msg, sessionKey, 'sync');
          if (ok) {
            totalProcessed++;
          } else {
            totalSkipped++;
          }
        } catch (err) {
          totalErrors++;
          logError(`Error ingesting ${sessionKey}:${msg.id}: ${(err as Error).message}`);
        }

        // Track the latest message ID
        if (msg.id > latestId) {
          latestId = msg.id;
        }
      }

      // Update state with the latest message ID for this session
      if (latestId) {
        state.sessions[sessionKey] = latestId;
      }
    } catch (err) {
      totalErrors++;
      logError(`Failed to process session ${sessionKey}: ${(err as Error).message}`);
      // Continue with next session
    }
  }

  state.lastRun = new Date().toISOString();
  saveState(state);

  log('=== Sync Complete ===');
  log(`Sessions scanned: ${totalSessions}`);
  log(`Messages processed: ${totalProcessed}`);
  log(`Messages skipped: ${totalSkipped}`);
  log(`Errors: ${totalErrors}`);
}

main().catch(err => {
  logError(`Fatal error: ${(err as Error).message}`);
  process.exit(1);
});
