# Reparando — WhatsApp worker (Baileys)

Always-on service that links each store's WhatsApp by QR and bridges it to the
Reparando SaaS database (Supabase). One WhatsApp session per store (org).

It talks **only outbound** to Supabase + WhatsApp — it exposes **no ports**, so
it can't be reached from the internet. It uses the Supabase **service_role** key,
so treat that key as a secret and never commit it.

## What it does
- Reads `wa_connections` (rows with `provider = 'baileys'`) and keeps a live
  WhatsApp session per store.
- Writes the **QR** (as a data-URL image) + connection **status** back to
  `wa_connections`, which the app shows in Ajustes → WhatsApp.
- Saves the session (`creds`) in the DB, so a restart reconnects without a new QR.
- Inbound messages → `wa_conversations` + `wa_messages` (feeds the WhatsApp inbox).
- Drains `wa_outbox` to send outbound messages, **throttled and rate-limited**
  (min gap between messages, per-minute + per-day caps). Sending itself is
  guarded in the DB (`wa_enqueue`): reply-only to known contacts, no mass broadcast.

## Environment variables
| var | value |
|-----|-------|
| `SUPABASE_URL` | `https://cfotlppderfzdmspsjjn.supabase.co` |
| `SUPABASE_SERVICE_ROLE_KEY` | Supabase → Project settings → API → `service_role` key (secret) |
| `POLL_MS` | optional, default 4000 |
| `PUMP_MS` | optional, default 1500 |

## Deploy — Option A: Oracle Cloud "Always Free" VM ($0, recommended)
1. Create a free Oracle Cloud account → Compute → Instance → shape
   **VM.Standard.A1.Flex** (ARM, Always Free; e.g. 1 OCPU / 6 GB is plenty).
   Pick an Ubuntu 22.04 image. Save the SSH key.
2. SSH in and install Docker:
   ```bash
   sudo apt update && sudo apt install -y docker.io git
   sudo usermod -aG docker $USER && newgrp docker
   ```
3. Put this folder on the box (`git clone` your repo), then:
   ```bash
   cd reparando-wa-worker
   cp .env.example .env && nano .env      # paste SUPABASE_URL + SERVICE_ROLE_KEY
   docker build -t wa-worker .
   docker run -d --name wa-worker --restart=always --env-file .env wa-worker
   docker logs -f wa-worker               # watch it start
   ```
   `--restart=always` keeps it running across reboots/crashes.

## Deploy — Option B: Railway / Render / Fly (paid, easiest)
- Create a new service from this GitHub repo.
- It auto-detects the Dockerfile. Add the two env vars above.
- Choose an always-on instance (Render's free tier **sleeps** — use a paid one).

## Run locally (to test)
```bash
npm install
cp .env.example .env   # fill in
npm run dev
```

## How linking works (end-to-end)
1. Store admin opens **Ajustes → WhatsApp → Vincular por QR**. The app calls
   `wa_baileys_link()` which sets that store's row to `status = 'pending'`.
2. This worker sees it, opens a WhatsApp socket, and writes the QR back.
3. The app polls `wa_connection_status()` and shows the QR. The admin scans it
   from **WhatsApp → Linked devices**.
4. On success the worker sets `status = 'connected'` and the phone number; the
   app shows "Conectado". "Desvincular" sets `status = 'logout'` → the worker
   logs out and clears the session.

## Notes / limits
- **Unofficial WhatsApp.** Keep volume sane and only reply to real customers —
  the built-in caps and reply-only rule are there to reduce ban risk, not remove it.
- Use a **dedicated business number**, not a personal one.
- One process handles all stores; scale vertically (more RAM) as more stores link.
