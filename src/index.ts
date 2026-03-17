/**
 * OpenClaw Ingestor — Entry point.
 * Starts the API-based polling watcher.
 */

import { startPoller } from './watcher.js';
import { startHealthServer } from './health.js';

const RETRY_DELAY_MS = 30_000;

function log(msg: string): void {
  console.log(`[main] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[main] ${new Date().toISOString()} ${msg}`);
}

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  const healthPort = parseInt(process.env.PORT || '3000', 10);
  startHealthServer(healthPort);

  log('OpenClaw Ingestor starting...');
  log(`Memory DB API: ${process.env.MEMORY_DATABASE_API_URL || 'https://memory-database.etdofresh.com'}`);
  log(`Write token: ${process.env.MEMORY_DATABASE_API_WRITE_TOKEN ? '***set***' : 'NOT SET'}`);

  // Validate OpenClaw credentials — warn but don't crash
  if (!process.env.OPENCLAW_URL) {
    logError('OPENCLAW_URL is not set — will retry until available');
  } else {
    log(`OpenClaw API: ${process.env.OPENCLAW_URL}`);
  }

  if (!(process.env.OPENCLAW_TOKEN || process.env.OPENCLAW_GATEWAY_TOKEN)) {
    logError('OPENCLAW_TOKEN or OPENCLAW_GATEWAY_TOKEN is not set — will retry until available');
  } else {
    log(`OpenClaw token: ***set***`);
  }

  // Retry loop — keep trying if credentials aren't ready yet
  let cleanup: (() => void) | null = null;

  while (!cleanup) {
    if (!process.env.OPENCLAW_URL || !(process.env.OPENCLAW_TOKEN || process.env.OPENCLAW_GATEWAY_TOKEN)) {
      log(`Waiting ${RETRY_DELAY_MS / 1000}s for OPENCLAW_URL and OPENCLAW_TOKEN to be set...`);
      await sleep(RETRY_DELAY_MS);
      continue;
    }

    try {
      cleanup = await startPoller();
    } catch (err) {
      logError(`Failed to start poller: ${(err as Error).message}`);
      log(`Retrying in ${RETRY_DELAY_MS / 1000}s...`);
      await sleep(RETRY_DELAY_MS);
    }
  }

  // Graceful shutdown
  const shutdown = (): void => {
    log('Received shutdown signal');
    if (cleanup) cleanup();
    process.exit(0);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  log('Ingestor running. Press Ctrl+C to stop.');
}

main().catch(err => {
  console.error(`[main] Fatal error: ${(err as Error).message}`);
  process.exit(1);
});
