/**
 * Minimal HTTP health server.
 * GET /api/health → 200 { status: "ok", uptime, pollCount, lastPollAt }
 * GET /api/status → same
 */

import http from 'http';

export interface HealthState {
  startedAt: string;
  pollCount: number;
  lastPollAt: string | null;
  lastError: string | null;
}

const state: HealthState = {
  startedAt: new Date().toISOString(),
  pollCount: 0,
  lastPollAt: null,
  lastError: null,
};

export function recordPoll(error?: string): void {
  state.pollCount++;
  state.lastPollAt = new Date().toISOString();
  state.lastError = error ?? null;
}

export function startHealthServer(port = 3000): http.Server {
  const server = http.createServer((req, res) => {
    if (req.method !== 'GET') {
      res.writeHead(405);
      res.end('Method Not Allowed');
      return;
    }
    const url = req.url?.split('?')[0];
    if (url === '/api/health' || url === '/api/status' || url === '/') {
      const body = JSON.stringify({
        status: 'ok',
        uptime: Math.floor((Date.now() - new Date(state.startedAt).getTime()) / 1000),
        startedAt: state.startedAt,
        pollCount: state.pollCount,
        lastPollAt: state.lastPollAt,
        lastError: state.lastError,
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(body);
    } else {
      res.writeHead(404);
      res.end('Not Found');
    }
  });

  server.listen(port, () => {
    console.log(`[health] ${new Date().toISOString()} Health server listening on :${port}`);
  });

  return server;
}
