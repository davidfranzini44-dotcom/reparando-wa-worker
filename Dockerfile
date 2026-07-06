# Reparando WhatsApp worker (Baileys) — build + run
FROM node:20-slim
WORKDIR /app

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
