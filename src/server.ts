/**
 * Combined HTTP server: health + sessions + backfill API.
 * Uses Node's built-in `http` module — no express dependency.
 */

import http from 'http';
import { URL } from 'url';
import { getHealthState } from './health.js';
import { listSessions, getSessionHistory } from './openclaw-client.js';

/* ── Backfill State ──────────────────────────────────── */

interface BackfillStatus {
  running: boolean;
  startedAt: string | null;
  processed: number;
  errors: number;
  completedAt: string | null;
}

const backfillStatus: BackfillStatus = {
  running: false,
  startedAt: null,
  processed: 0,
  errors: 0,
  completedAt: null,
};

/**
 * Called by the backfill trigger to update status.
 */
export function getBackfillStatus(): BackfillStatus {
  return { ...backfillStatus };
}

/**
 * Run a full sync (backfill) in the background.
 * Imports sync logic dynamically to avoid circular deps.
 */
async function runBackfill(): Promise<void> {
  if (backfillStatus.running) return;

  backfillStatus.running = true;
  backfillStatus.startedAt = new Date().toISOString();
  backfillStatus.processed = 0;
  backfillStatus.errors = 0;
  backfillStatus.completedAt = null;

  try {
    // Use the same logic as sync.ts but inline to track progress
    const { listSessions: ls, getSessionHistory: gh } = await import('./openclaw-client.js');
    const { ingestMessage } = await import('./ingest.js');

    const sessions = await ls({ limit: 500 });
    log(`[backfill] Found ${sessions.length} sessions`);

    for (const session of sessions) {
      const sessionKey = session.key ?? session.sessionKey;
      if (!sessionKey) continue;

      try {
        const messages = await gh(sessionKey, { limit: 500 });

        for (const msg of messages) {
          try {
            const ok = await ingestMessage(msg, sessionKey, 'backfill');
            if (ok) backfillStatus.processed++;
          } catch {
            backfillStatus.errors++;
          }
        }
      } catch {
        backfillStatus.errors++;
      }
    }
  } catch (err) {
    log(`[backfill] Fatal error: ${(err as Error).message}`);
    backfillStatus.errors++;
  } finally {
    backfillStatus.running = false;
    backfillStatus.completedAt = new Date().toISOString();
    log(`[backfill] Complete: ${backfillStatus.processed} processed, ${backfillStatus.errors} errors`);
  }
}

/* ── Helpers ──────────────────────────────────────────── */

function log(msg: string): void {
  console.log(`[server] ${new Date().toISOString()} ${msg}`);
}

function json(res: http.ServerResponse, data: unknown, status = 200): void {
  const body = JSON.stringify(data);
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(body);
}

function notFound(res: http.ServerResponse): void {
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Not Found' }));
}

function methodNotAllowed(res: http.ServerResponse): void {
  res.writeHead(405, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Method Not Allowed' }));
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => chunks.push(chunk));
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    req.on('error', reject);
  });
}

/* ── Route Matching ──────────────────────────────────── */

// Match /api/sessions/:sessionKey/messages
const SESSION_MESSAGES_RE = /^\/api\/sessions\/(.+)\/messages$/;

/* ── Server ──────────────────────────────────────────── */

export function startServer(port = 3000): http.Server {
  const server = http.createServer(async (req, res) => {
    // CORS preflight
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      });
      res.end();
      return;
    }

    const parsed = new URL(req.url || '/', `http://localhost:${port}`);
    const pathname = parsed.pathname;

    try {
      // ── Health ──
      if ((pathname === '/api/health' || pathname === '/api/status' || pathname === '/') && req.method === 'GET') {
        const state = getHealthState();
        json(res, {
          status: 'ok',
          uptime: Math.floor((Date.now() - new Date(state.startedAt).getTime()) / 1000),
          startedAt: state.startedAt,
          pollCount: state.pollCount,
          lastPollAt: state.lastPollAt,
          lastError: state.lastError,
        });
        return;
      }

      // ── GET /api/sessions ──
      if (pathname === '/api/sessions' && req.method === 'GET') {
        const sessions = await listSessions({ limit: 500 });
        json(res, sessions);
        return;
      }

      // ── GET /api/sessions/:sessionKey/messages ──
      const msgMatch = SESSION_MESSAGES_RE.exec(pathname);
      if (msgMatch && req.method === 'GET') {
        const sessionKey = decodeURIComponent(msgMatch[1]);
        const limitParam = parsed.searchParams.get('limit');
        const limit = limitParam ? parseInt(limitParam, 10) : 50;
        const messages = await getSessionHistory(sessionKey, { limit });
        json(res, messages);
        return;
      }

      // ── POST /api/backfill ──
      if (pathname === '/api/backfill' && req.method === 'POST') {
        if (backfillStatus.running) {
          json(res, { ok: false, message: 'Backfill already running' }, 409);
          return;
        }
        // Fire and forget
        void runBackfill();
        json(res, { ok: true, message: 'Backfill started' });
        return;
      }

      // ── GET /api/backfill/status ──
      if (pathname === '/api/backfill/status' && req.method === 'GET') {
        json(res, getBackfillStatus());
        return;
      }

      notFound(res);
    } catch (err) {
      log(`Error handling ${req.method} ${pathname}: ${(err as Error).message}`);
      json(res, { error: (err as Error).message }, 500);
    }
  });

  server.listen(port, () => {
    log(`Server listening on :${port}`);
  });

  return server;
}
