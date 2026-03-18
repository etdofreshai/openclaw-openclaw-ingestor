/**
 * Health state tracking.
 * State is managed here; the HTTP server is in server.ts.
 */

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

export function getHealthState(): HealthState {
  return state;
}
