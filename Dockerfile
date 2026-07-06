# Reparando WhatsApp worker (Baileys) — build + run
# Node 22: has native WebSocket, required by @supabase/realtime-js.
FROM node:22-slim
WORKDIR /app

# Baileys pulls libsignal from GitHub during install (needs git), and native
# modules need a toolchain. Install them before npm install.
RUN apt-get update && apt-get install -y --no-install-recommends \
      git python3 make g++ ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install deps (incl. dev, needed to compile TypeScript)
COPY package.json ./
RUN npm install

# Build
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

ENV NODE_ENV=production
# No inbound ports — this process only talks OUT to Supabase + WhatsApp.
CMD ["node", "dist/index.js"]
