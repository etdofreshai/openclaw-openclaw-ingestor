/**
 * OpenClaw Ingestor — Entry point.
 * Starts the real-time JSONL watcher.
 */

import { startWatcher } from './watcher.js';

function log(msg: string): void {
  console.log(`[main] ${new Date().toISOString()} ${msg}`);
}

async function main(): Promise<void> {
  log('OpenClaw Ingestor starting...');
  log(`API URL: ${process.env.MEMORY_DATABASE_API_URL || 'https://memory-database.etdofresh.com'}`);
  log(`Write token: ${process.env.MEMORY_DATABASE_API_WRITE_TOKEN ? '***set***' : 'NOT SET'}`);

  const cleanup = await startWatcher();

  // Graceful shutdown
  const shutdown = (): void => {
    log('Received shutdown signal');
    cleanup();
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
