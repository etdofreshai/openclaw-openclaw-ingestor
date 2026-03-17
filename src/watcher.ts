/**
 * Polling-based watcher for OpenClaw sessions.
 * Replaces the filesystem watcher — polls the OpenClaw API periodically
 * and ingests new messages.
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { listSessions, getSessionHistory } from './openclaw-client.js';
import { ingestMessage } from './ingest.js';
import { recordPoll } from './health.js';
import type { SessionInfo } from './openclaw-client.js';

const STATE_FILE = path.join(process.cwd(), '.watcher-state.json');
const POLL_INTERVAL_MS = 60_000; // 60 seconds
const MESSAGES_PER_POLL = 200;

interface WatcherState {
  /** sessionKey → lastMessageId */
  sessions: Record<string, string>;
  /** ISO timestamp of last successful poll */
  lastPoll: string;
}

function log(msg: string): void {
  console.log(`[poller] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[poller] ${new Date().toISOString()} ${msg}`);
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
  return { sessions: {}, lastPoll: '' };
}

function saveState(state: WatcherState): void {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
  } catch (err) {
    logError(`Failed to save state: ${(err as Error).message}`);
  }
}

/**
 * Determine which sessions have been updated since our last poll.
 */
function findUpdatedSessions(
  sessions: SessionInfo[],
  lastPoll: string,
): SessionInfo[] {
  if (!lastPoll) {
    // First run — process all sessions
    return sessions;
  }

  const lastPollTime = new Date(lastPoll).getTime();
  return sessions.filter(s => {
    const updatedMs = s.updatedAt ?? (s.lastMessageAt ? new Date(s.lastMessageAt).getTime() : null);
    if (updatedMs === null) return true; // Unknown — include to be safe
    return updatedMs > lastPollTime;
  });
}

/**
 * Poll once: fetch updated sessions, ingest new messages.
 * Returns counts of processed messages.
 */
async function pollOnce(state: WatcherState): Promise<{ processed: number; errors: number }> {
  let processed = 0;
  let errors = 0;

  const sessions = await listSessions({ limit: 500 });
  const updated = findUpdatedSessions(sessions, state.lastPoll);

  if (updated.length === 0) {
    return { processed: 0, errors: 0 };
  }

  log(`${updated.length} session(s) updated since last poll`);

  for (const session of updated) {
    const sessionKey = session.key ?? session.sessionKey;

    try {
      const lastMessageId = state.sessions[sessionKey];
      const afterOpt = lastMessageId ? { after: lastMessageId } : undefined;

      const messages = await getSessionHistory(sessionKey, {
        limit: MESSAGES_PER_POLL,
        ...afterOpt,
      });

      if (messages.length === 0) continue;

      let latestId = lastMessageId || '';

      for (const msg of messages) {
        // Skip already-seen messages
        if (lastMessageId && msg.id <= lastMessageId) {
          continue;
        }

        try {
          const ok = await ingestMessage(msg, sessionKey, 'poller');
          if (ok) processed++;
        } catch (err) {
          errors++;
          logError(`Error ingesting ${sessionKey}:${msg.id}: ${(err as Error).message}`);
        }

        if (msg.id > latestId) {
          latestId = msg.id;
        }
      }

      if (latestId) {
        state.sessions[sessionKey] = latestId;
      }
    } catch (err) {
      errors++;
      logError(`Failed to process session ${sessionKey}: ${(err as Error).message}`);
      // Continue with next session
    }
  }

  state.lastPoll = new Date().toISOString();
  saveState(state);

  return { processed, errors };
}

/**
 * Start the polling watcher. Returns a cleanup function.
 */
export async function startPoller(): Promise<() => void> {
  const state = loadState();
  let pollTimer: ReturnType<typeof setInterval> | null = null;
  let polling = false;
  let shutdownRequested = false;

  const doPoll = async (): Promise<void> => {
    if (polling || shutdownRequested) return;
    polling = true;

    try {
      const { processed, errors } = await pollOnce(state);
      if (processed > 0 || errors > 0) {
        log(`Poll complete: ${processed} processed, ${errors} errors`);
      }
      recordPoll();
    } catch (err) {
      const msg = (err as Error).message;
      logError(`Poll error: ${msg}`);
      recordPoll(msg);
    } finally {
      polling = false;
    }
  };

  // Initial poll
  log('Running initial poll...');
  await doPoll();
  log(`Initial poll complete. Polling every ${POLL_INTERVAL_MS / 1000}s...`);

  // Start periodic polling
  pollTimer = setInterval(() => {
    void doPoll();
  }, POLL_INTERVAL_MS);

  // Cleanup function
  const cleanup = (): void => {
    shutdownRequested = true;
    log('Shutting down poller...');

    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }

    saveState(state);
    log('Poller shutdown complete.');
  };

  return cleanup;
}
