# OpenClaw Ingestor
# Pulls chat sessions from the OpenClaw Gateway API and writes them
# to the Memory Database API. No filesystem access required.
#
# ENV vars (required):
#   OPENCLAW_URL                   — OpenClaw gateway URL (e.g. http://openclaw-gateway:3000)
#   OPENCLAW_TOKEN                 — Bearer token for the OpenClaw gateway
#   MEMORY_DATABASE_API_URL        — Memory DB API base URL (default: https://memory-database.etdofresh.com)
#   MEMORY_DATABASE_API_WRITE_TOKEN — Bearer token for Memory DB API writes

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
