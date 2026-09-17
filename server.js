/**
 * zen-proxy — thin OpenAI-compatible proxy in front of OpenCode Zen free models.
 * Pure Node.js (>=20), zero dependencies.
 *
 * Upstream (https://opencode.ai/zen/v1) is already OpenAI-compatible, but the
 * free tier only accepts requests carrying the headers a real opencode client
 * sends (x-opencode-session / -project / -request / -client + opencode UA).
 * This proxy injects those, filters to free models, and streams everything
 * else through untouched.
 *
 * Key failover: ZEN_API_KEYS is tried in order (default "public"). Any upstream
 * error (rate limit, unknown model, auth, 5xx…) moves the request to the next
 * key within the same request. A key that hit a key-level failure (429/5xx,
 * network) also gets a cooldown so later requests skip straight to the working
 * key until the cooldown lapses; request-level errors (bad model name etc.)
 * never cool a key down.
 *
 * Model list: zen exposes no pricing, so the proxy reads the same catalog
 * opencode itself uses (models.opencode.ai/api.json, provider "opencode")
 * and /v1/models lists models whose cost.input AND cost.output are 0 —
 * the exact rule opencode applies to strip paid models. It's a hint, not a
 * gate: chat requests pass through untouched and zen decides what a key may
 * call, so brand-new models work the moment zen supports them.
 *
 * Identity strategy (SESSION_MODE):
 *   "derived"     deterministic time-bucketed ids (default):
 *                 session = HMAC(seed, "ses:<UTC date>")   -> same day, same id
 *                 project = HMAC(seed, "proj:<ISO week>")  -> same week, same id
 *                 seed = OPENCODE_SECRET || RENDER_SERVICE_ID || random-at-boot,
 *                 so each deployment gets its OWN stable identity — two
 *                 instances sharing a proxy key never collide.
 *   "sticky"      one ses_ per conversation key, TTL-refreshed
 *   "per-request" fresh ses_ every call
 *
 * Env:
 *   PORT                 listen port                    (default 8787)
 *   PROXY_KEY            require `Authorization: Bearer <key>` / `X-Proxy-Key`
 *                        from clients. UNSET = open proxy (only sane on localhost)
 *   OPENCODE_SECRET      seed for derived ids; unset on Render = RENDER_SERVICE_ID
 *                        (unique per service); locally = random per boot
 *   ZEN_UPSTREAM         upstream base URL              (default https://opencode.ai/zen/v1)
 *   ZEN_API_KEYS         comma list tried in order      (default "public";
 *                        falls back to ZEN_API_KEY if set)
 *   FAILOVER_COOLDOWN_MS skip a key that just failed for this long
 *                                                       (default 60000, 0 = off)
 *   CATALOG_URL          model catalog source           (default https://models.opencode.ai/api.json)
 *   CATALOG_PROVIDER     provider id in catalog         (default "opencode")
 *   CATALOG_TTL_MS       catalog refresh interval       (default 3600000)
 *   SESSION_MODE         "derived" | "sticky" | "per-request"   (default "derived")
 *   SESSION_TTL_MS       sticky idle TTL                (default 1800000)
 *   OPENCODE_PROJECT_ID  static project id (sticky/per-request modes only)
 *   OPENCODE_CLIENT      x-opencode-client tag          (default "tui")
 *   OPENCODE_CHANNEL     retained compatibility setting (default "dev")
 *   OPENCODE_VERSION     UA version                     (default "1.18.31")
 *   TIMEOUT_MS           upstream timeout               (default 600000)
 */

import http from "node:http"
import crypto from "node:crypto"
import { Readable } from "node:stream"

const UPSTREAM = (process.env.ZEN_UPSTREAM ?? "https://opencode.ai/zen/v1").replace(/\/+$/, "")
const PORT = Number(process.env.PORT ?? 8787)
const PROXY_KEY = process.env.PROXY_KEY?.trim() || undefined
const KEYS = (process.env.ZEN_API_KEYS ?? process.env.ZEN_API_KEY ?? "public")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
const COOLDOWN_MS = Number(process.env.FAILOVER_COOLDOWN_MS ?? 60_000)
const CATALOG_URL = (process.env.CATALOG_URL ?? "https://models.opencode.ai/api.json").replace(/\/+$/, "")
const CATALOG_PROVIDER = process.env.CATALOG_PROVIDER ?? "opencode"
const CATALOG_TTL_MS = Number(process.env.CATALOG_TTL_MS ?? 3_600_000)
const CLIENT = process.env.OPENCODE_CLIENT ?? "tui"
const CHANNEL = process.env.OPENCODE_CHANNEL ?? "dev"
const VERSION = process.env.OPENCODE_VERSION ?? "1.18.31"
const SESSION_MODE = ["derived", "sticky", "per-request"].includes(process.env.SESSION_MODE)
  ? process.env.SESSION_MODE
  : "derived"
const SESSION_TTL_MS = Number(process.env.SESSION_TTL_MS ?? 30 * 60_000)
const TIMEOUT_MS = Number(process.env.TIMEOUT_MS ?? 600_000)
const ALLOW_MODELS = process.env.ALLOW_MODELS?.trim() || undefined
const EXTRA_MODELS = (process.env.EXTRA_MODELS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)

const ALLOW_EXPLICIT = ALLOW_MODELS && ALLOW_MODELS !== "*" ? ALLOW_MODELS.split(",").map((s) => s.trim()) : undefined
const BODY_LIMIT = 10 * 1024 * 1024
// UA mimics a real opencode install (per-route, captured from opencode 1.3.0):
// chat/completions goes through @ai-sdk/openai-compatible, /responses through
// @ai-sdk/openai. OPENCODE_USER_AGENT overrides both if you need an exact match.
// empirically zen gives the generous free-tier quota only to opencode-looking
// Zen expects the same two-segment UA used by current opencode LLM requests.
// OPENCODE_USER_AGENT overrides it when testing a specific installed version.
const UA_OPENCODE = `opencode/${VERSION}`
const UA_SDK_CHAT = process.env.OPENCODE_USER_AGENT?.trim() || UA_OPENCODE
const UA_SDK_RESPONSES = process.env.OPENCODE_USER_AGENT?.trim() || UA_OPENCODE
/** key-level failures worth cooling a key down for; other errors just fail over */
const KEY_COOLDOWN_STATUS = (s) => s === 429 || s >= 500

// ---------------------------------------------------------------------------
// ids
// ---------------------------------------------------------------------------

// Same ID shape as opencode's @opencode-ai/schema/identifier:
// 26 chars = 12 hex chars (timestamp + counter) + 14 random base62 chars.
const ID_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
let lastIDTimestamp = 0
let idCounter = 0

function identifier(descending = false) {
  const timestamp = Date.now()
  if (timestamp !== lastIDTimestamp) {
    lastIDTimestamp = timestamp
    idCounter = 0
  }
  idCounter++
  const current = BigInt(timestamp) * 0x1000n + BigInt(idCounter)
  const value = descending ? ~current : current
  let time = ""
  for (let i = 0; i < 6; i++) {
    time += Number((value >> BigInt(40 - 8 * i)) & 0xffn).toString(16).padStart(2, "0")
  }
  let random = ""
  for (let i = 0; i < 14; i++) random += ID_CHARS[(Math.random() * ID_CHARS.length) | 0]
  return time + random
}

function rid(prefix, { descending = false } = {}) {
  return `${prefix}_${identifier(descending)}`
}

// identity seed: unique per instance. OPENCODE_SECRET wins if set; otherwise
// Render's per-service RENDER_SERVICE_ID (stable across redeploys, differs per
// service); locally a random seed. PROXY_KEY intentionally NOT used — instances
// sharing a proxy key must not share identity.
const SECRET =
  process.env.OPENCODE_SECRET?.trim() || process.env.RENDER_SERVICE_ID || rid("seed")
const SEED_SOURCE = process.env.OPENCODE_SECRET?.trim()
  ? "env"
  : process.env.RENDER_SERVICE_ID
    ? "render-service"
    : "random-boot"

/** same UTC day -> same id */
function utcDay(d = new Date()) {
  return d.toISOString().slice(0, 10)
}

/** same ISO-8601 week (Mon-based, UTC) -> same id */
function isoWeek(d = new Date()) {
  const t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()))
  const day = t.getUTCDay() || 7
  t.setUTCDate(t.getUTCDate() + 4 - day)
  const yearStart = new Date(Date.UTC(t.getUTCFullYear(), 0, 1))
  const week = Math.ceil(((t - yearStart) / 86_400_000 + 1) / 7)
  return `${t.getUTCFullYear()}-W${String(week).padStart(2, "0")}`
}

/** deterministic bucketed ID with the same shape as opencode IDs. */
function derivedID(prefix, scope) {
  const mac = crypto.createHmac("sha256", SECRET).update(scope).digest()
  // opencode IDs are 12 hex chars + 14 base62 chars after the prefix.
  const time = mac.subarray(0, 6).toString("hex")
  let random = ""
  for (let i = 0; i < 14; i++) random += ID_CHARS[mac[6 + i] % ID_CHARS.length]
  return `${prefix}_${time}${random}`
}

const STATIC_PROJECT_ID = process.env.OPENCODE_PROJECT_ID?.trim() || rid("proj")

function projectIDFor() {
  return SESSION_MODE === "derived" ? derivedID("proj", `proj:${isoWeek()}`) : STATIC_PROJECT_ID
}

const sessions = new Map() // sticky mode: conversation key -> { id, at }

function sessionFor(key = "default") {
  if (SESSION_MODE === "derived") return derivedID("ses", `ses:${utcDay()}`)
  if (SESSION_MODE === "per-request") return rid("ses", { descending: true })
  const now = Date.now()
  if (sessions.size > 500) {
    for (const [k, v] of sessions) if (now - v.at > SESSION_TTL_MS) sessions.delete(k)
  }
  const hit = sessions.get(key)
  if (hit && now - hit.at <= SESSION_TTL_MS) {
    hit.at = now
    return hit.id
  }
  const fresh = { id: rid("ses"), at: now }
  sessions.set(key, fresh)
  return fresh.id
}

// ---------------------------------------------------------------------------
// free-model catalog — same URL, provider, and cost==0 rule opencode uses
// ---------------------------------------------------------------------------

let freeSet = new Set() // empty = catalog not loaded yet
let catalogLoadedAt = 0

async function refreshCatalog() {
  try {
    const res = await fetch(CATALOG_URL, {
      headers: { "user-agent": UA_OPENCODE, accept: "application/json" },
      signal: AbortSignal.timeout(30_000),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const catalog = await res.json()
    const provider = catalog?.[CATALOG_PROVIDER]
    if (!provider?.models) throw new Error(`provider "${CATALOG_PROVIDER}" not found in catalog`)
    const free = new Set()
    for (const [id, m] of Object.entries(provider.models)) {
      const cost = m?.cost
      if ((cost?.input ?? 1) === 0 && (cost?.output ?? 1) === 0) {
        free.add(id)
        if (id.startsWith(`${CATALOG_PROVIDER}/`)) free.add(id.slice(CATALOG_PROVIDER.length + 1))
      }
    }
    freeSet = free
    catalogLoadedAt = Date.now()
    console.log(`catalog loaded: ${free.size} free / ${Object.keys(provider.models).length} models from ${CATALOG_URL}`)
  } catch (e) {
    console.warn(`catalog refresh failed (${e.message}); keeping ${freeSet.size} cached entries`)
  }
}

function modelAllowed(id) {
  // display-only filter for /v1/models; chat is never blocked here
  if (ALLOW_MODELS === "*") return true
  if (ALLOW_EXPLICIT) return ALLOW_EXPLICIT.includes(id)
  if (EXTRA_MODELS.includes(id)) return true
  if (freeSet.size > 0) return freeSet.has(id)
  return /-free$/.test(id) // heuristic until catalog arrives
}

refreshCatalog()
setInterval(refreshCatalog, CATALOG_TTL_MS).unref()

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function authorized(req) {
  if (!PROXY_KEY) return true
  return req.headers.authorization === `Bearer ${PROXY_KEY}` || req.headers["x-proxy-key"] === PROXY_KEY
}

function zenHeaders(sessionID, apiKey, ua = UA_SDK_CHAT) {
  return {
    authorization: `Bearer ${apiKey}`,
    "content-type": "application/json",
    "x-opencode-project": projectIDFor(),
    "x-opencode-session": sessionID,
    "x-opencode-request": rid("msg"),
    "x-opencode-client": CLIENT,
    "user-agent": ua,
  }
}

function maskKey(k) {
  return k.length <= 10 ? k : `${k.slice(0, 6)}…${k.slice(-4)}`
}

/** keys eligible right now (cooldown passed); if all cooling down, try all */
function pickKeys() {
  const now = Date.now()
  const ok = KEYS.map((k, i) => i).filter((i) => (cooldown.get(i) ?? 0) <= now)
  return ok.length > 0 ? ok : KEYS.map((_, i) => i)
}

const cooldown = new Map() // key index -> retry-after timestamp

/**
 * POST/GET against upstream with key failover.
 * Any non-OK response moves to the next key; the failed key is put on cooldown
 * only for key-level failures (429/5xx), so one bad model name doesn't shun a
 * healthy key. Returns the last attempt's Response (or null if network-dead).
 */
async function upstreamFetch(path, { method = "GET", body, sessionID, ac, timeoutMs = TIMEOUT_MS, label = path, ua }) {
  const candidates = pickKeys()
  let lastRes = null
  for (let n = 0; n < candidates.length; n++) {
    const i = candidates[n]
    const signal = AbortSignal.any([ac.signal, AbortSignal.timeout(timeoutMs)])
    try {
      const res = await fetch(`${UPSTREAM}${path}`, {
        method,
        headers: zenHeaders(sessionID, KEYS[i], ua),
        body,
        signal,
      })
      if (res.ok || n === candidates.length - 1) return res
      const detail = await res.text().catch(() => "")
      console.warn(`key #${i} (${maskKey(KEYS[i])}) failed ${label}: ${res.status} ${detail.slice(0, 160)}`)
      if (COOLDOWN_MS > 0 && KEY_COOLDOWN_STATUS(res.status)) cooldown.set(i, Date.now() + COOLDOWN_MS)
      lastRes = res
    } catch (e) {
      if (ac.signal.aborted) throw e // client gone / global timeout — don't fail over
      console.warn(`key #${i} (${maskKey(KEYS[i])}) network error ${label}: ${e.message}`)
      if (COOLDOWN_MS > 0) cooldown.set(i, Date.now() + COOLDOWN_MS)
      lastRes = null
    }
  }
  return lastRes // every candidate failed and last body was consumed — caller handles null
}

const CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers": "authorization, content-type, x-conversation-id, x-proxy-key",
}

function json(res, body, status = 200) {
  res.writeHead(status, { "content-type": "application/json", ...CORS })
  res.end(JSON.stringify(body))
}

function oaiError(res, status, message, type = "invalid_request_error") {
  json(res, { error: { message, type, code: null } }, status)
}

function log(method, path, model, status, ms, extra = "") {
  console.log(
    `${new Date().toISOString()} ${method} ${path}${model ? ` model=${model}` : ""} -> ${status} ${ms | 0}ms${extra ? ` ${extra}` : ""}`,
  )
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0
    req.on("data", (c) => {
      size += c.length
      if (size > BODY_LIMIT) {
        reject(new Error("body too large"))
        req.destroy()
        return
      }
      chunks.push(c)
    })
    req.on("end", () => resolve(Buffer.concat(chunks)))
    req.on("error", reject)
  })
}

// ---------------------------------------------------------------------------
// routes
// ---------------------------------------------------------------------------

async function listModels(res) {
  const t0 = performance.now()
  const upstream = await upstreamFetch("/models", {
    sessionID: sessionFor("models"),
    ac: new AbortController(),
    timeoutMs: 30_000,
  })
  if (!upstream) return oaiError(res, 502, "zen-proxy: all upstream keys failed", "api_error")
  if (!upstream.ok || ALLOW_MODELS === "*") {
    res.writeHead(upstream.status, {
      ...CORS,
      "content-type": upstream.headers.get("content-type") ?? "application/json",
    })
    Readable.fromWeb(upstream.body).pipe(res)
    return
  }
  const data = await upstream.json()
  const filtered = (data.data ?? []).filter((m) => modelAllowed(m.id))
  log("GET", "/v1/models", undefined, 200, performance.now() - t0, `count=${filtered.length}`)
  json(res, { object: "list", data: filtered })
}

async function chatCompletions(req, res) {
  const t0 = performance.now()

  let body
  try {
    body = JSON.parse((await readBody(req)).toString("utf8"))
  } catch {
    return oaiError(res, 400, "request body must be valid JSON (<=10MB)")
  }

  const model = typeof body?.model === "string" ? body.model : undefined
  if (!model) return oaiError(res, 400, "`model` is required")
  // no allowlist here on purpose: /v1/models is only a hint of what should work;
  // zen is the enforcer (unknown/non-free models get its own error passthrough).
  // this way brand-new models work the moment zen supports them.

  // conversation key only matters in sticky mode
  const conversationKey = req.headers["x-conversation-id"] || (typeof body.user === "string" && body.user) || "default"
  const sessionID = sessionFor(conversationKey)

  const ac = new AbortController()
  res.on("close", () => ac.abort()) // client went away mid-stream -> stop upstream

  let upstream
  try {
    upstream = await upstreamFetch("/chat/completions", {
      method: "POST",
      body: JSON.stringify(body),
      sessionID,
      ac,
      label: `chat(${model})`,
    })
  } catch (e) {
    if (res.writableEnded) return
    const reason = e.message === "upstream timeout" ? "upstream timeout" : "upstream unreachable"
    log("POST", "/v1/chat/completions", model, 502, performance.now() - t0, reason)
    return oaiError(res, 502, `zen-proxy: ${reason}`, "api_error")
  }

  if (!upstream) {
    log("POST", "/v1/chat/completions", model, 502, performance.now() - t0, "all keys failed")
    return oaiError(res, 502, "zen-proxy: all upstream keys failed", "api_error")
  }

  log(
    "POST",
    "/v1/chat/completions",
    model,
    upstream.status,
    performance.now() - t0,
    process.env.DEBUG_IDS ? `sid=${sessionID.slice(4, 14)}` : "",
  )

  const headers = { ...CORS, "content-type": upstream.headers.get("content-type") ?? "application/json" }
  const retry = upstream.headers.get("retry-after")
  if (retry) headers["retry-after"] = retry
  for (const [k, v] of upstream.headers) if (k.startsWith("x-ratelimit")) headers[k] = v

  res.writeHead(upstream.status, headers)
  Readable.fromWeb(upstream.body).pipe(res) // SSE streams straight through
}

/** generic pass-through for any other /v1/* path (e.g. /v1/responses) */
async function passthrough(req, res, upstreamPath) {
  const t0 = performance.now()
  let raw
  try {
    raw = await readBody(req)
  } catch {
    return oaiError(res, 400, "request body too large (<=10MB)")
  }
  const sessionID = sessionFor(req.headers["x-conversation-id"] || "default")
  const ua = upstreamPath.startsWith("/responses") ? UA_SDK_RESPONSES : UA_SDK_CHAT

  const ac = new AbortController()
  res.on("close", () => ac.abort())

  try {
    // zen rejects unknown tool types (e.g. codex clients sending image_generation
    // to models that only support functions). on such a 400, drop the offending
    // tool type and retry instead of failing the request.
    for (let attempt = 0; ; attempt++) {
      const upstream = await upstreamFetch(upstreamPath, {
        method: req.method,
        body: raw.length > 0 ? raw : undefined,
        sessionID,
        ac,
        label: upstreamPath,
        ua,
      })
      if (!upstream) {
        log(req.method, upstreamPath, undefined, 502, performance.now() - t0, "all keys failed")
        return oaiError(res, 502, "zen-proxy: all upstream keys failed", "api_error")
      }

      let retry = false
      if (!upstream.ok && upstream.status === 400 && attempt < 4 && raw.length > 0) {
        const text = await upstream.text().catch(() => "")
        const bad = text.match(/Unsupported tool type: '([^']+)'/)
        if (bad) {
          const stripped = stripToolType(raw, bad[1])
          if (stripped) {
            raw = stripped
            retry = true
            log(req.method, upstreamPath, undefined, 400, performance.now() - t0, `retry w/o tool type=${bad[1]}`)
          }
        }
        if (retry) continue
        // body already consumed — replay it as the response
        res.writeHead(upstream.status, { ...CORS, "content-type": "application/json" })
        return res.end(text)
      }

      log(req.method, upstreamPath, undefined, upstream.status, performance.now() - t0)
      const headers = { ...CORS, "content-type": upstream.headers.get("content-type") ?? "application/json" }
      res.writeHead(upstream.status, headers)
      Readable.fromWeb(upstream.body).pipe(res)
      return
    }
  } catch (e) {
    if (!res.writableEnded) return oaiError(res, 502, `zen-proxy: ${e.message === "upstream timeout" ? "upstream timeout" : "upstream unreachable"}`, "api_error")
  }
}

/** remove tools[].type === badType from a JSON body; null when nothing changed */
function stripToolType(raw, badType) {
  try {
    const obj = JSON.parse(raw.toString("utf8"))
    if (!Array.isArray(obj.tools)) return null
    const filtered = obj.tools.filter((t) => t?.type !== badType)
    if (filtered.length === obj.tools.length) return null
    obj.tools = filtered
    return Buffer.from(JSON.stringify(obj))
  } catch {
    return null
  }
}

async function handle(req, res) {
  const path = new URL(req.url, "http://x").pathname.replace(/\/+$/, "") || "/"

  if (req.method === "OPTIONS") {
    res.writeHead(204, CORS)
    return res.end()
  }

  if (path === "/healthz") return json(res, { ok: true })

  if (path === "/" && req.method === "GET") {
    return json(res, {
      name: "zen-proxy",
      session_mode: SESSION_MODE,
      auth: PROXY_KEY ? "required" : "open",
      keys: KEYS.length,
      allow_models: ALLOW_MODELS ?? (freeSet.size > 0 ? `free-only (${freeSet.size} in catalog)` : "free-only (heuristic)"),
    })
  }

  // inspect the identity currently being injected (auth still applies)
  if (path === "/debug/ids" && req.method === "GET") {
    if (!authorized(req)) return oaiError(res, 401, "invalid or missing proxy key", "authentication_error")
    return json(res, {
      mode: SESSION_MODE,
      utc_day: utcDay(),
      iso_week: isoWeek(),
      project_id: projectIDFor(),
      session_id: sessionFor("default"),
      request_id: rid("msg"),
      seed_source: SEED_SOURCE,
      keys: KEYS.map((k, i) => ({ index: i, key: maskKey(k), cooling_until: cooldown.get(i) ?? null })),
      catalog_free: freeSet.size,
      catalog_loaded_at: catalogLoadedAt ? new Date(catalogLoadedAt).toISOString() : null,
    })
  }

  if (!authorized(req)) return oaiError(res, 401, "invalid or missing proxy key", "authentication_error")

  if (path === "/v1/models" && req.method === "GET") {
    try {
      return await listModels(res)
    } catch {
      if (!res.writableEnded) return oaiError(res, 502, "zen-proxy: upstream unreachable", "api_error")
      return
    }
  }

  if (path === "/v1/chat/completions" && req.method === "POST") return chatCompletions(req, res)

  // everything else under /v1/* (responses, completions, …) is raw passthrough
  // with the same key failover — zen is the single source of truth for what works
  if (path.startsWith("/v1/")) return passthrough(req, res, path.slice(3)) // /v1/x -> /x

  return oaiError(res, 404, `no route for ${req.method} ${path}`)
}

const server = http.createServer((req, res) => {
  handle(req, res).catch((e) => {
    console.error("handler error:", e)
    if (!res.writableEnded) oaiError(res, 500, "zen-proxy: internal error", "api_error")
  })
})

server.listen(PORT, () => {
  console.log(
    `zen-proxy :${PORT} upstream=${UPSTREAM} session=${SESSION_MODE} auth=${PROXY_KEY ? "on" : "OFF"} keys=[${KEYS.map(maskKey).join(", ")}] allow=${ALLOW_MODELS ?? "free-only"}`,
  )
})

process.on("SIGTERM", () => server.close(() => process.exit(0)))
process.on("SIGINT", () => server.close(() => process.exit(0)))
