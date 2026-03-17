/**
 * OpenClaw Gateway API client.
 * Calls the POST /tools/invoke endpoint to list sessions and fetch history.
 */

const MAX_RETRIES = 3;
const BASE_DELAY_MS = 1000;

export interface SessionInfo {
  sessionKey: string;
  kind: string;
  label?: string;
  lastMessageAt?: string;
}

export interface ContentBlock {
  type: string;
  text?: string;
  source?: {
    type: string;
    media_type?: string;
    data?: string;
    url?: string;
  };
  content?: ContentBlock[] | string;
  name?: string;
  input?: unknown;
  [key: string]: unknown;
}

export interface MessageInfo {
  id: string;
  role: 'user' | 'assistant' | string;
  content: string | ContentBlock[];
  timestamp?: string;
  model?: string;
}

interface InvokeResponse {
  ok: boolean;
  result: unknown;
  error?: string;
}

function log(msg: string): void {
  console.log(`[openclaw-client] ${new Date().toISOString()} ${msg}`);
}

function logError(msg: string): void {
  console.error(`[openclaw-client] ${new Date().toISOString()} ${msg}`);
}

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getConfig(): { url: string; token: string } {
  const url = process.env.OPENCLAW_URL || '';
  const token = process.env.OPENCLAW_TOKEN || process.env.OPENCLAW_GATEWAY_TOKEN || '';
  return { url, token };
}

/**
 * Invoke a tool on the OpenClaw gateway.
 */
export async function invokeTool(
  tool: string,
  args: Record<string, unknown>,
  sessionKey?: string,
): Promise<unknown> {
  const { url, token } = getConfig();
  if (!url) throw new Error('OPENCLAW_URL is not set');
  if (!token) throw new Error('OPENCLAW_TOKEN is not set');

  const body = {
    tool,
    args,
    sessionKey: sessionKey ?? 'main',
  };

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(`${url}/tools/invoke`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify(body),
      });

      if (res.status === 429) {
        const retryAfter = res.headers.get('Retry-After');
        const waitMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : BASE_DELAY_MS * Math.pow(2, attempt);
        log(`Rate limited (429) on ${tool}, waiting ${waitMs}ms (attempt ${attempt}/${MAX_RETRIES})`);
        await sleep(waitMs);
        continue;
      }

      if (res.status >= 500 && attempt < MAX_RETRIES) {
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Server error ${res.status} on ${tool}, retry ${attempt}/${MAX_RETRIES} in ${waitMs}ms`);
        await sleep(waitMs);
        continue;
      }

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`invokeTool(${tool}) failed: ${res.status} — ${text.slice(0, 500)}`);
      }

      const data = await res.json() as InvokeResponse;
      if (!data.ok) {
        throw new Error(`invokeTool(${tool}) returned error: ${data.error ?? JSON.stringify(data).slice(0, 500)}`);
      }

      return data.result;
    } catch (err) {
      if (err instanceof TypeError && attempt < MAX_RETRIES) {
        const waitMs = BASE_DELAY_MS * Math.pow(2, attempt);
        logError(`Network error on ${tool}, retry ${attempt}/${MAX_RETRIES} in ${waitMs}ms: ${(err as Error).message}`);
        await sleep(waitMs);
        continue;
      }
      throw err;
    }
  }

  throw new Error(`invokeTool(${tool}): exhausted retries`);
}

/**
 * List sessions from the OpenClaw gateway.
 */
export async function listSessions(opts?: { limit?: number }): Promise<SessionInfo[]> {
  const limit = opts?.limit ?? 50;
  const result = await invokeTool('sessions_list', {
    limit,
    kinds: ['main', 'subagent'],
    messageLimit: 0,
  });

  // The result may be a string (formatted text) or an array of session objects.
  // We need to handle both cases.
  if (typeof result === 'string') {
    // Parse the text-based session list.
    // Format is typically lines like: "sessionKey | kind | label | lastMessageAt"
    // But it could also be JSON embedded in the string.
    try {
      const parsed = JSON.parse(result);
      if (Array.isArray(parsed)) {
        return parsed as SessionInfo[];
      }
    } catch {
      // Not JSON, try to parse text format
    }

    // Try to extract session info from text lines
    const sessions: SessionInfo[] = [];
    const lines = result.split('\n').filter((l: string) => l.trim());
    for (const line of lines) {
      // Try to match sessionKey patterns like "agent:main:..."
      const match = /\b(agent:\S+)/.exec(line);
      if (match) {
        sessions.push({
          sessionKey: match[1],
          kind: line.includes('subagent') ? 'subagent' : 'main',
        });
      }
    }
    return sessions;
  }

  if (Array.isArray(result)) {
    return result as SessionInfo[];
  }

  // If it's an object with a sessions/items array
  const obj = result as Record<string, unknown>;
  if (Array.isArray(obj.sessions)) return obj.sessions as SessionInfo[];
  if (Array.isArray(obj.items)) return obj.items as SessionInfo[];

  logError(`Unexpected listSessions result type: ${typeof result}`);
  return [];
}

/**
 * Get message history for a session.
 */
export async function getSessionHistory(
  sessionKey: string,
  opts?: { limit?: number; after?: string },
): Promise<MessageInfo[]> {
  const args: Record<string, unknown> = {
    sessionKey,
    limit: opts?.limit ?? 200,
    includeTools: false,
  };
  if (opts?.after) {
    args.after = opts.after;
  }

  const result = await invokeTool('sessions_history', args);

  // Result could be an array of messages or a string
  if (typeof result === 'string') {
    try {
      const parsed = JSON.parse(result);
      if (Array.isArray(parsed)) {
        return parsed as MessageInfo[];
      }
    } catch {
      // Not JSON
    }
    logError(`Unexpected sessions_history string result for ${sessionKey}`);
    return [];
  }

  if (Array.isArray(result)) {
    return result as MessageInfo[];
  }

  // If it's an object with a messages/items array
  const obj = result as Record<string, unknown>;
  if (Array.isArray(obj.messages)) return obj.messages as MessageInfo[];
  if (Array.isArray(obj.items)) return obj.items as MessageInfo[];
  if (Array.isArray(obj.history)) return obj.history as MessageInfo[];

  logError(`Unexpected sessions_history result type for ${sessionKey}: ${typeof result}`);
  return [];
}
