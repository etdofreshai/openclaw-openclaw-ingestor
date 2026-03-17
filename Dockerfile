# OpenClaw Ingestor
# Standalone chat + attachment ingestor for the Memory Database API.
#
# ENV vars:
#   MEMORY_DATABASE_API_URL        — Memory DB API base URL (default: https://memory-database.etdofresh.com)
#   MEMORY_DATABASE_API_WRITE_TOKEN — Bearer token for API writes
#
# Mount the OpenClaw data directory:
#   -v /data/.openclaw:/data/.openclaw:ro

FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

# --- Production stage ---
FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY --from=builder /app/dist ./dist

ENV NODE_ENV=production

CMD ["node", "dist/index.js"]
