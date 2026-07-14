/**
 * Reparando SaaS — WhatsApp worker (Baileys, multi-tenant).
 *
 * One always-on process that holds a WhatsApp Web session per store (org) and
 * bridges it to Supabase:
 *   - reads/writes wa_connections (status, qr, creds, phone, heartbeat)
 *   - writes inbound messages to wa_conversations / wa_messages
 *   - drains wa_outbox (throttled, rate-limited) to send outbound messages
 *
 * It authenticates to Supabase with the SERVICE ROLE key (bypasses RLS), so
 * this process must NEVER be exposed publicly — it only talks OUT to Supabase.
 *
 * Env:
 *   SUPABASE_URL                 e.g. https://cfotlppderfzdmspsjjn.supabase.co
 *   SUPABASE_SERVICE_ROLE_KEY    service_role key (secret)
 *   POLL_MS                      connection reconcile interval (default 4000)
 *   PUMP_MS                      outbox drain interval (default 1500)
 */
import makeWASocket, {
  DisconnectReason,
  makeCacheableSignalKeyStore,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
  downloadMediaMessage,
  type WASocket,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import { createClient } from "@supabase/supabase-js";
import QRCode from "qrcode";
import pino from "pino";
import fs from "node:fs";
import path from "node:path";

const log = pino({ level: process.env.LOG_LEVEL || "info" });
const silent = pino({ level: "silent" });

const SUPABASE_URL = must("SUPABASE_URL");
const SERVICE_KEY = must("SUPABASE_SERVICE_ROLE_KEY");
const POLL_MS = int(process.env.POLL_MS, 4000);
const PUMP_MS = int(process.env.PUMP_MS, 1500);
const AVATAR_MS = int(process.env.AVATAR_MS, 20000);
const AUTH_DIR = process.env.AUTH_DIR || "/app/data/auth";
// Re-fetch a contact's profile picture at most this often.
const AVATAR_TTL_MS = 7 * 24 * 3600 * 1000;
// Contacts that came back without a picture are re-checked no more often than
// this, so photos added (or made public) later eventually appear.
const AVATAR_RECHECK_MS = 24 * 3600 * 1000;
const AVATAR_BUCKET = "wa-avatars";
const MEDIA_BUCKET = "wa-media";

const supa = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });

function must(k: string): string {
  const v = process.env[k];
  if (!v) { log.error(`Missing env ${k}`); process.exit(1); }
  return v;
}
function int(v: string | undefined, d: number) { const n = Number(v); return Number.isFinite(n) && n > 0 ? n : d; }
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
// Stable 8-char pairing code (unambiguous chars) reused across refreshes so the
// user always sees the same code to type on their phone.
const PAIR_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
function genPairCode() { let s = ""; for (let i = 0; i < 8; i++) s += PAIR_CHARS[Math.floor(Math.random() * PAIR_CHARS.length)]; return s; }
const jidOf = (phone: string) => `${phone.replace(/[^0-9]/g, "")}@s.whatsapp.net`;
const digits = (s: string | null | undefined) => (s || "").replace(/[^0-9]/g, "");
const textOf = (v: unknown) => String(v ?? "").trim();

function isWhatsAppJid(value: string | null | undefined): boolean {
  const v = textOf(value);
  return /^[^@\s]+@(s\.whatsapp\.net|lid|g\.us|broadcast|newsletter)$/.test(v);
}

function phoneForStorage(rawTo: string, jid: string, existingPhone?: string | null): string {
  if (existingPhone) return existingPhone;
  if (!isWhatsAppJid(rawTo)) return digits(rawTo) || rawTo;
  // LID chats do not always expose the real phone. Store the raw JID handle
  // rather than inventing a phone-style JID from the privacy identifier.
  return digits(jid.split("@")[0]) || rawTo;
}

// Chat addresses we never want to treat as customer conversations.
function isIgnorableJid(jid: string): boolean {
  return jid.endsWith("@g.us")            // groups
    || jid.endsWith("@newsletter")        // channels / newsletters
    || jid.endsWith("@broadcast")         // broadcast lists
    || jid === "status@broadcast";        // status updates
}

/** Best-effort: turn a privacy LID (…@lid) into the real phone for display.
 *  Falls back to the raw digits if WhatsApp doesn't expose a mapping. */
async function displayPhoneFor(sock: WASocket, jid: string): Promise<string> {
  const base = digits(jid.split("@")[0]);
  if (!jid.endsWith("@lid")) return base;
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const repo: any = (sock as any).signalRepository;
    const pn = await repo?.lidMapping?.getPNForLID?.(jid);
    if (pn && typeof pn === "string") { const d = digits(pn.split("@")[0]); if (d) return d; }
  } catch { /* mapping not available on this version */ }
  return base;
}

type Row = Record<string, any>;

// Cache of recently-sent messages so we can answer WhatsApp "retry receipts".
// Without this, recipients get stuck on "Waiting for this message…" because the
// sender can't re-encrypt the message when the recipient requests a resend.
const sentMsgCache = new Map<string, any>();
function cacheSent(id: string | null | undefined, message: any) {
  if (!id || !message) return;
  sentMsgCache.set(id, message);
  if (sentMsgCache.size > 2000) { const first = sentMsgCache.keys().next().value; if (first) sentMsgCache.delete(first); }
}

interface Session {
  orgId: string;
  sock: WASocket | null;
  starting: boolean;
  pairing?: boolean;       // started to request a phone-number pairing code
  lastSendAt: number;      // for min_gap throttle
  cfg: { min_gap_ms: number; per_min_cap: number; daily_cap: number };
}
const sessions = new Map<string, Session>();

/* --------------------------- Baileys auth state ---------------------------- */
function authPath(orgId: string) {
  return path.join(AUTH_DIR, orgId);
}

function clearAuthState(orgId: string) {
  try { fs.rmSync(authPath(orgId), { recursive: true, force: true }); } catch { /* noop */ }
}

async function patch(orgId: string, fields: Row) {
  await supa.from("wa_connections").update({ ...fields, updated_at: new Date().toISOString() }).eq("org_id", orgId);
}

/* ------------------------------- Session boot ------------------------------ */
async function startSession(orgId: string, cfg: Session["cfg"], pairingPhone?: string | null) {
  let s = sessions.get(orgId);
  if (s?.sock || s?.starting) return;
  s = s || { orgId, sock: null, starting: false, lastSendAt: 0, cfg };
  s.cfg = cfg;
  s.starting = true;
  s.pairing = !!pairingPhone;
  sessions.set(orgId, s);

  try {
    const { state, saveCreds } = await useMultiFileAuthState(authPath(orgId));
    const { version } = await fetchLatestBaileysVersion();
    const sock = makeWASocket({
      version,
      auth: {
        creds: state.creds,
        // Cacheable key store: stabilizes Signal session/pre-key reads during
        // encryption — a common cause of undecryptable "Waiting for this message".
        keys: makeCacheableSignalKeyStore(state.keys, silent as any),
      },
      printQRInTerminal: false,
      markOnlineOnConnect: false,
      browser: ["Reparando", "Chrome", "120.0.0"],
      logger: silent as any,
      generateHighQualityLinkPreview: false,
      syncFullHistory: false,
      // Answer retry receipts so recipients don't get stuck on "Waiting for this message".
      getMessage: async (key) => (key?.id ? sentMsgCache.get(key.id) : undefined),
    });
    s.sock = sock;
    s.starting = false;

    // No-QR linking: ask WhatsApp for an 8-char pairing code the user types on
    // their phone (Linked devices → Link with phone number instead).
    if (pairingPhone && !state.creds.registered) {
      setTimeout(async () => {
        try {
          // Reuse the existing code (kept in the DB) so it stays the same across
          // WhatsApp's ~60s refresh cycle; only generate one on a fresh attempt.
          const { data: row } = await supa.from("wa_connections").select("pairing_code").eq("org_id", orgId).single();
          let custom = String(row?.pairing_code || "");
          if (!/^[A-Z0-9]{8}$/.test(custom)) custom = genPairCode();
          let code: string;
          try { code = await sock.requestPairingCode(digits(pairingPhone), custom); }
          catch { code = await sock.requestPairingCode(digits(pairingPhone)); } // fallback: WhatsApp-generated
          await patch(orgId, { status: "pairing", pairing_code: code, qr: null, worker_error: null });
          log.info({ orgId }, "pairing code issued");
        } catch (e) {
          await patch(orgId, { worker_error: String((e as Error)?.message || e) });
          log.error({ orgId, e }, "requestPairingCode failed");
        }
      }, 3000);
    }

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (u) => {
      const { connection, lastDisconnect, qr } = u;
      if (qr && !pairingPhone) {
        const dataUrl = await QRCode.toDataURL(qr, { margin: 1, width: 320 });
        await patch(orgId, { status: "qr", qr: dataUrl, worker_error: null, last_seen_at: new Date().toISOString() });
        log.info({ orgId }, "qr ready");
      }
      if (connection === "connecting") await patch(orgId, { status: "connecting", last_seen_at: new Date().toISOString() });
      if (connection === "open") {
        const phone = digits(sock.user?.id?.split(":")[0]);
        await patch(orgId, {
          status: "connected", qr: null, phone_number: phone || null,
          connected_at: new Date().toISOString(), last_seen_at: new Date().toISOString(), worker_error: null,
          pairing_code: null, pairing_phone: null,
        });
        const live = sessions.get(orgId); if (live) live.pairing = false;
        log.info({ orgId, phone }, "connected");
      }
      if (connection === "close") {
        const code = (lastDisconnect?.error as Boom | undefined)?.output?.statusCode;
        const loggedOut = code === DisconnectReason.loggedOut;
        sessions.delete(orgId);
        try { sock.end(undefined as any); } catch { /* noop */ }
        if (loggedOut) {
          clearAuthState(orgId);
          await patch(orgId, { status: "disconnected", enabled: false, qr: null, creds: null, phone_number: null });
          log.warn({ orgId }, "logged out — cleared session");
        } else if (pairingPhone) {
          // Pairing window timed out waiting for the code — stay in pairing mode
          // and re-arm the SAME code (reconcile restarts the session).
          await patch(orgId, { status: "pairing", qr: null, worker_error: null });
          log.info({ orgId }, "pairing timed out — re-arming code");
        } else {
          await patch(orgId, { status: "connecting", qr: null, worker_error: String(lastDisconnect?.error?.message || code || "close") });
          log.warn({ orgId, code }, "closed — will reconnect");
        }
      }
    });

    sock.ev.on("messages.upsert", async (up) => {
      if (up.type !== "notify") return;
      for (const m of up.messages) {
        try {
          const jid = m.key.remoteJid;
          if (m.key.fromMe || !jid) continue;
          if (isIgnorableJid(jid)) continue; // groups / channels / broadcasts / status
          const phone = await displayPhoneFor(sock, jid);
          const text = m.message?.conversation
            || m.message?.extendedTextMessage?.text
            || m.message?.imageMessage?.caption
            || m.message?.videoMessage?.caption
            || "";
          const name = m.pushName || null;
          const media = await saveMedia(orgId, sock, m);
          const body = text || media?.label || "";
          await recordInbound(orgId, sock, phone, jid, name, body, media?.url ?? null, media?.type ?? null);
        } catch (e) { log.error({ orgId, e }, "inbound handling failed"); }
      }
    });
  } catch (e) {
    s.starting = false;
    sessions.delete(orgId);
    await patch(orgId, { status: "connecting", worker_error: String((e as Error)?.message || e) });
    log.error({ orgId, e }, "startSession failed");
  }
}

async function stopSession(orgId: string) {
  const s = sessions.get(orgId);
  if (s?.sock) { try { s.sock.end(undefined as any); } catch { /* noop */ } }
  sessions.delete(orgId);
}

async function logoutSession(orgId: string) {
  const s = sessions.get(orgId);
  try { await s?.sock?.logout(); } catch { /* noop */ }
  await stopSession(orgId);
  clearAuthState(orgId);
  await patch(orgId, { status: "disconnected", enabled: false, qr: null, creds: null, phone_number: null });
  log.info({ orgId }, "unlinked");
}

/* ---------------------------- Profile pictures ----------------------------- */
// Fetch a contact's WhatsApp profile picture and store a stable copy in Supabase
// Storage (WhatsApp's own CDN URLs expire). Throttled by wa_avatar_synced_at.
// Best-effort: contacts who hide their picture keep the app's letter fallback.
async function syncAvatar(orgId: string, sock: WASocket, jid: string, convId: string, force = false) {
  try {
    if (!force) {
      const { data } = await supa.from("wa_conversations").select("wa_avatar_synced_at").eq("id", convId).single();
      const last = data?.wa_avatar_synced_at ? new Date(data.wa_avatar_synced_at).getTime() : 0;
      if (last && Date.now() - last < AVATAR_TTL_MS) return;
    }
    let publicUrl: string | undefined;
    try {
      const picUrl = await sock.profilePictureUrl(jid, "image");
      if (picUrl) {
        const res = await fetch(picUrl);
        if (res.ok) {
          const buf = Buffer.from(await res.arrayBuffer());
          const path = `${orgId}/${convId}.jpg`;
          const { error: upErr } = await supa.storage.from(AVATAR_BUCKET).upload(path, buf, { contentType: "image/jpeg", upsert: true });
          if (!upErr) {
            const base = supa.storage.from(AVATAR_BUCKET).getPublicUrl(path).data.publicUrl;
            publicUrl = `${base}?t=${Date.now()}`; // cache-bust on refresh
          }
        }
      }
    } catch { /* no picture / privacy / not reachable — keep any existing avatar */ }
    // Stamp synced_at always (throttle); only overwrite the URL when we got one.
    await supa.from("wa_conversations").update({
      wa_avatar_url: publicUrl, // undefined leaves the existing value untouched
      wa_avatar_synced_at: new Date().toISOString(),
    }).eq("id", convId);
  } catch (e) { log.warn({ orgId, e }, "avatar sync failed"); }
}

/* ------------------------------ Inbound media ------------------------------ */
// Download inbound media (voice notes, images, video, docs, stickers) and
// re-host it in Storage so the inbox can play/show it. null for text-only.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function saveMedia(orgId: string, sock: WASocket, m: any): Promise<{ url: string; type: string; label: string } | null> {
  const msg = m.message;
  if (!msg) return null;
  let type: string, mimetype: string | undefined, ext: string, label: string;
  if (msg.audioMessage) { type = "audio"; mimetype = msg.audioMessage.mimetype; ext = "ogg"; label = msg.audioMessage.ptt ? "🎤 Nota de voz" : "🎵 Audio"; }
  else if (msg.imageMessage) { type = "image"; mimetype = msg.imageMessage.mimetype; ext = "jpg"; label = "📷 Foto"; }
  else if (msg.videoMessage) { type = "video"; mimetype = msg.videoMessage.mimetype; ext = "mp4"; label = "🎥 Video"; }
  else if (msg.stickerMessage) { type = "sticker"; mimetype = msg.stickerMessage.mimetype; ext = "webp"; label = "🖼️ Sticker"; }
  else if (msg.documentMessage) { type = "document"; mimetype = msg.documentMessage.mimetype; ext = msg.documentMessage.fileName?.split(".").pop() || "bin"; label = "📄 " + (msg.documentMessage.fileName || "Documento"); }
  else return null;
  try {
    const buf = (await downloadMediaMessage(m, "buffer", {}, { logger: silent as any, reuploadRequest: sock.updateMediaMessage })) as Buffer;
    const id = (m.key?.id as string) || String(Date.now());
    const objPath = `${orgId}/${id}.${ext}`;
    const contentType = (mimetype || "").split(";")[0] || "application/octet-stream";
    const { error } = await supa.storage.from(MEDIA_BUCKET).upload(objPath, buf, { contentType, upsert: true });
    if (error) { log.warn({ orgId, error }, "media upload failed"); return null; }
    const url = `${supa.storage.from(MEDIA_BUCKET).getPublicUrl(objPath).data.publicUrl}?t=${Date.now()}`;
    return { url, type, label };
  } catch (e) { log.warn({ orgId, e }, "media download failed"); return null; }
}

/* ------------------------------ Inbound writes ----------------------------- */
async function recordInbound(orgId: string, sock: WASocket, phone: string, jid: string, name: string | null, text: string, mediaUrl: string | null = null, mediaType: string | null = null) {
  // find/create conversation
  const { data: convs } = await supa.from("wa_conversations").select("id, unread")
    .eq("org_id", orgId).eq("wa_phone", phone).limit(1);
  let convId = convs?.[0]?.id as string | undefined;
  if (!convId) {
    const { data: ins } = await supa.from("wa_conversations").insert({
      org_id: orgId, wa_phone: phone, wa_jid: jid, wa_name: name, status: "open", unread: 1,
      last_message_at: new Date().toISOString(), last_text: text, last_direction: "in",
    }).select("id").single();
    convId = ins?.id;
  } else {
    await supa.from("wa_conversations").update({
      wa_jid: jid, wa_name: name || undefined, last_message_at: new Date().toISOString(),
      last_text: text, last_direction: "in", unread: (convs![0].unread || 0) + 1,
    }).eq("id", convId);
  }
  if (convId) {
    await supa.from("wa_messages").insert({
      org_id: orgId, conversation_id: convId, direction: "in", body: text, status: "received",
      media_url: mediaUrl, media_type: mediaType,
    });
    void syncAvatar(orgId, sock, jid, convId); // best-effort, throttled
  }
}

/* ------------------------------- Outbox pump ------------------------------- */
async function pumpOutbox() {
  for (const [orgId, s] of sessions) {
    if (!s.sock) continue;
    const now = Date.now();
    if (now - s.lastSendAt < s.cfg.min_gap_ms) continue; // throttle per org

    // rate caps (defense-in-depth; wa_enqueue also checks)
    const sinceMin = new Date(now - 60_000).toISOString();
    const [{ count: minCount }, { count: dayCount }] = await Promise.all([
      supa.from("wa_messages").select("id", { count: "exact", head: true })
        .eq("org_id", orgId).eq("direction", "out").gte("created_at", sinceMin),
      supa.from("wa_messages").select("id", { count: "exact", head: true })
        .eq("org_id", orgId).eq("direction", "out").gte("created_at", new Date(new Date().toDateString()).toISOString()),
    ]);
    if ((minCount ?? 0) >= s.cfg.per_min_cap || (dayCount ?? 0) >= s.cfg.daily_cap) continue;

    // claim one queued row atomically
    const { data: claim } = await supa.rpc("wa_claim_outbox", { p_org: orgId });
    const row = Array.isArray(claim) ? claim[0] : claim;
    if (!row) continue;

    try {
      // Prefer the exact chat address we already know (handles LID chats and lets
      // us skip the reachability check for people who already messaged us).
      const rawTo = textOf(row.to_phone);
      const rawDigits = digits(rawTo);
      const rawIsJid = isWhatsAppJid(rawTo);
      let convoRows: Row[] | null = null;
      if (rawIsJid) {
        const res = await supa.from("wa_conversations").select("id, wa_jid, wa_phone")
          .eq("org_id", orgId).eq("wa_jid", rawTo).limit(1);
        convoRows = res.data as Row[] | null;
      }
      if (!convoRows?.length && rawDigits) {
        const res = await supa.from("wa_conversations").select("id, wa_jid, wa_phone")
          .eq("org_id", orgId).eq("wa_phone", rawDigits).limit(1);
        convoRows = res.data as Row[] | null;
      }
      if (!convoRows?.length && rawTo && !rawIsJid) {
        const res = await supa.from("wa_conversations").select("id, wa_jid, wa_phone")
          .eq("org_id", orgId).eq("wa_phone", rawTo).limit(1);
        convoRows = res.data as Row[] | null;
      }
      let convId = convoRows?.[0]?.id as string | undefined;
      let jid = (convoRows?.[0]?.wa_jid as string | undefined) || (rawIsJid ? rawTo : undefined);
      const existingPhone = convoRows?.[0]?.wa_phone as string | undefined;

      if (!jid) {
        const phone = rawDigits || rawTo;
        const exists = await s.sock.onWhatsApp(phone).catch(() => []);
        if (!exists || !exists[0]?.exists) {
          await supa.from("wa_outbox").update({ status: "failed", error: "número sin WhatsApp", attempts: (row.attempts || 0) + 1 }).eq("id", row.id);
          continue;
        }
        jid = exists[0].jid || jidOf(phone);
      }

      await s.sock.presenceSubscribe(jid).catch(() => undefined);
      await s.sock.sendPresenceUpdate("available", jid).catch(() => undefined);
      await sleep(250);

      const sent = await s.sock.sendMessage(jid, { text: row.body || "" }, { useUserDevicesCache: false } as any);
      cacheSent(sent?.key?.id, sent?.message); // enable retry-receipt resends
      s.lastSendAt = Date.now();
      await supa.from("wa_outbox").update({ status: "sent", sent_at: new Date().toISOString() }).eq("id", row.id);

      // mirror into the inbox thread
      if (!convId) {
        const { data: ins } = await supa.from("wa_conversations").insert({
          org_id: orgId, wa_phone: phoneForStorage(rawTo, jid, existingPhone), wa_jid: jid, status: "open", unread: 0,
          last_message_at: new Date().toISOString(), last_text: row.body, last_direction: "out",
        }).select("id").single();
        convId = ins?.id;
      } else {
        await supa.from("wa_conversations").update({ wa_jid: jid, last_message_at: new Date().toISOString(), last_text: row.body, last_direction: "out" }).eq("id", convId);
      }
      if (convId) await supa.from("wa_messages").insert({ org_id: orgId, conversation_id: convId, direction: "out", body: row.body, status: "sent", created_by: row.created_by });
    } catch (e) {
      await supa.from("wa_outbox").update({ status: "failed", error: String((e as Error)?.message || e), attempts: (row.attempts || 0) + 1 }).eq("id", row.id);
      log.error({ orgId, e }, "send failed");
    }
  }
}

/* ------------------------------ Avatar sweep ------------------------------- */
// Backfill profile pictures for conversations that don't have one yet (e.g.
// chats started from the app, or existing chats from before this feature).
async function avatarSweep() {
  for (const [orgId, s] of sessions) {
    if (!s.sock) continue;
    // 1) Conversations never checked yet (new chats, or from before this feature).
    const { data: fresh } = await supa.from("wa_conversations")
      .select("id, wa_jid, wa_phone")
      .eq("org_id", orgId).is("wa_avatar_synced_at", null).limit(8);
    const batch = [...((fresh as Row[]) ?? [])];
    // 2) Spare budget → re-check contacts still without a picture (oldest first),
    //    so a photo added later (or a transient failure) eventually shows up.
    if (batch.length < 8) {
      const staleBefore = new Date(Date.now() - AVATAR_RECHECK_MS).toISOString();
      const { data: stale } = await supa.from("wa_conversations")
        .select("id, wa_jid, wa_phone")
        .eq("org_id", orgId).is("wa_avatar_url", null)
        .lt("wa_avatar_synced_at", staleBefore)
        .order("wa_avatar_synced_at", { ascending: true })
        .limit(8 - batch.length);
      batch.push(...((stale as Row[]) ?? []));
    }
    for (const c of batch) {
      const jid = (c.wa_jid as string) || jidOf(c.wa_phone as string);
      await syncAvatar(orgId, s.sock, jid, c.id as string, true);
      await sleep(400); // gentle on WhatsApp
    }
  }
}

/* ------------------------------- Reconcile -------------------------------- */
async function reconcile() {
  const { data, error } = await supa.from("wa_connections")
    .select("org_id, provider, enabled, status, pairing_phone, min_gap_ms, per_min_cap, daily_cap")
    .eq("provider", "baileys");
  if (error) { log.error({ error }, "reconcile query failed"); return; }

  const seen = new Set<string>();
  for (const c of (data as Row[]) ?? []) {
    seen.add(c.org_id);
    const cfg = { min_gap_ms: c.min_gap_ms ?? 3500, per_min_cap: c.per_min_cap ?? 10, daily_cap: c.daily_cap ?? 250 };
    if (c.status === "logout") { await logoutSession(c.org_id); continue; }
    if (!c.enabled) { await stopSession(c.org_id); continue; }
    // Phone-number pairing requested → (re)start a fresh session that asks for a
    // pairing code. Guard on the session's pairing flag so we don't restart it
    // every tick (the worker flips status to "pairing" once the code is issued).
    if (c.status === "pair_requested") {
      if (!sessions.get(c.org_id)?.pairing) {
        await stopSession(c.org_id);
        await startSession(c.org_id, cfg, c.pairing_phone);
      } else { sessions.get(c.org_id)!.cfg = cfg; }
      continue;
    }
    // pending/qr/pairing/connecting/connected → ensure a live session; heartbeat if connected
    if (!sessions.has(c.org_id)) await startSession(c.org_id, cfg, c.status === "pairing" ? c.pairing_phone : null);
    else {
      const s = sessions.get(c.org_id)!; s.cfg = cfg;
      if (c.status === "connected") await patch(c.org_id, { last_seen_at: new Date().toISOString() });
    }
  }
  // stop sessions whose row disappeared
  for (const orgId of [...sessions.keys()]) if (!seen.has(orgId)) await stopSession(orgId);
}

async function main() {
  log.info("wa-worker starting");
  // loops
  const loop = async (fn: () => Promise<void>, ms: number) => {
    for (;;) { try { await fn(); } catch (e) { log.error({ e }, "loop error"); } await sleep(ms); }
  };
  loop(reconcile, POLL_MS);
  loop(pumpOutbox, PUMP_MS);
  loop(avatarSweep, AVATAR_MS);
}

main();
