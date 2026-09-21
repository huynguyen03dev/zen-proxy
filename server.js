/**
 * zen-proxy — thin OpenAI-compatible proxy in front of OpenCode Zen free models.
 * Pure Node.js (>=20), zero dependencies.
 *
 * Upstream (https://opencode.ai/zen/v1) is OpenAI-compatible, but the free tier
 * expects an OpenCode-shaped identity and, for anonymous public access, an
 * agent-shaped streaming request. This proxy injects identity/affinity headers,
 * shapes public bodies, bridges Anthropic Messages, and streams/transcodes the
 * supported protocols.
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
 *   FAILOVER_COOLDOWN_MS base key/proxy failure cooldown (default 15000, 0 = off)
 *   RETRY_MAX_ATTEMPTS   max upstream attempts/request  (default number of keys)
 *   ANONYMOUS_SHAPING    shape public requests as agent requests (default true)
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
const COOLDOWN_MS = Number(process.env.FAILOVER_COOLDOWN_MS ?? 15_000)
const RETRY_MAX_ATTEMPTS = Math.max(1, Number(process.env.RETRY_MAX_ATTEMPTS ?? 0) || KEYS.length)
const ANONYMOUS_SHAPING = process.env.ANONYMOUS_SHAPING !== "false"
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
const EXTRA_MODELS = [
  // Jev is free on Zen but is a System One model and is not yet present in
  // models.opencode.ai's pricing catalog, so keep it visible in /v1/models.
  "jev-1.13-free",
  ...(process.env.EXTRA_MODELS ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
]

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
const KEY_COOLDOWN_STATUS = (s) => s === 401 || s === 403 || s === 429 || s >= 500
const RETRYABLE_STATUS = (s) => s === 401 || s === 403 || s === 429 || s >= 500
const CORE_AGENT_TOOLS = ["bash", "edit", "glob", "grep", "read"]

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

const CANONICAL_SESSION = /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/

/** Preserve a real OpenCode session; canonicalize foreign client IDs. */
function canonicalSessionID(signal) {
  if (typeof signal === "string" && CANONICAL_SESSION.test(signal)) return signal
  return derivedID("ses", `client:${signal || "default"}`)
}

function projectIDFor(input = {}) {
  const supplied = input.projectID || input.project_id
  if (typeof supplied === "string" && supplied.trim()) return supplied.trim()
  return SESSION_MODE === "derived" ? derivedID("proj", `proj:${isoWeek()}`) : STATIC_PROJECT_ID
}

const sessions = new Map() // sticky mode: conversation key -> { id, at }

function sessionFor(key = "default", supplied) {
  if (supplied) return canonicalSessionID(supplied)
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
  const fresh = { id: rid("ses", { descending: true }), at: now }
  sessions.set(key, fresh)
  return fresh.id
}

function requestIDs(req, body = {}) {
  const metadata = body?.metadata && typeof body.metadata === "object" ? body.metadata : {}
  const rawSession =
    req?.headers?.["x-opencode-session"] ||
    req?.headers?.["x-session-affinity"] ||
    req?.headers?.["x-session-id"] ||
    req?.headers?.["conversation-id"] ||
    req?.headers?.["x-conversation-id"] ||
    body?.conversation_id ||
    metadata.session_id ||
    (typeof body?.user === "string" ? body.user : undefined)
  const sessionID = sessionFor(rawSession || "default", rawSession)
  const rawProject = req?.headers?.["x-opencode-project"] || metadata.project_id
  const projectID = projectIDFor({ projectID: rawProject })
  const requestID = CANONICAL_MESSAGE.test(req?.headers?.["x-opencode-request"])
    ? req.headers["x-opencode-request"]
    : rid("msg")
  const parentSessionID = req?.headers?.["x-parent-session-id"] || metadata.parent_session_id
  return { sessionID, projectID, requestID, parentSessionID }
}

const CANONICAL_MESSAGE = /^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$/

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
  return req.headers.authorization === `Bearer ${PROXY_KEY}` || req.headers["x-proxy-key"] === PROXY_KEY || req.headers["x-api-key"] === PROXY_KEY
}

function zenHeaders(sessionID, apiKey, ua = UA_SDK_CHAT, options = {}) {
  const headers = {
    authorization: `Bearer ${apiKey}`,
    "content-type": "application/json",
    accept: "application/json, text/event-stream",
    "x-opencode-project": options.projectID || projectIDFor(),
    "x-opencode-session": sessionID,
    "x-opencode-request": options.requestID || rid("msg"),
    "x-opencode-client": CLIENT,
    "x-session-affinity": sessionID,
    "X-Session-Id": sessionID,
    "user-agent": ua,
  }
  if (options.parentSessionID) headers["x-parent-session-id"] = options.parentSessionID
  return headers
}

function maskKey(k) {
  return k.length <= 10 ? k : `${k.slice(0, 6)}…${k.slice(-4)}`
}

function toolName(protocol, item) {
  if (!item || typeof item !== "object") return ""
  if (protocol === "chat") return item.function?.name || ""
  return item.name || ""
}

function anonymousTool(protocol, name) {
  const parameters = { type: "object", properties: {} }
  if (protocol === "chat") {
    return { type: "function", function: { name, description: `Agent tool ${name}`, parameters } }
  }
  if (protocol === "anthropic") {
    return { name, description: `Agent tool ${name}`, input_schema: parameters }
  }
  return { type: "function", name, description: `Agent tool ${name}`, parameters }
}

/** Normalize public/free requests to the agent-shaped stream Zen expects. */
function shapeAnonymousBody(raw, protocol = "chat") {
  // System One is a typed-decision protocol, not an agent chat request:
  // it must keep its state/questions body intact and never receive stream/tools.
  if (protocol === "systemone" || !ANONYMOUS_SHAPING || raw == null) return raw
  try {
    const payload = JSON.parse(Buffer.isBuffer(raw) ? raw.toString("utf8") : String(raw))
    if (!payload || typeof payload !== "object" || Array.isArray(payload)) return raw
    let changed = false
    if (payload.stream !== true) {
      payload.stream = true
      changed = true
    }
    const existing = Array.isArray(payload.tools) ? payload.tools : []
    const present = new Set(existing.map((item) => toolName(protocol, item)).filter(Boolean))
    const missing = CORE_AGENT_TOOLS.filter((name) => !present.has(name))
    if (!Array.isArray(payload.tools) || missing.length > 0) {
      payload.tools = [...existing, ...missing.map((name) => anonymousTool(protocol, name))]
      changed = true
    }
    return changed ? JSON.stringify(payload) : raw
  } catch {
    return raw
  }
}

function parseRetryAfter(value) {
  if (!value) return 0
  const seconds = Number.parseInt(String(value).trim(), 10)
  if (Number.isFinite(seconds) && seconds > 0) return seconds * 1000
  const timestamp = Date.parse(value)
  return Number.isFinite(timestamp) ? Math.max(0, timestamp - Date.now()) : 0
}

const cooldown = new Map() // key index -> { until, failures }

function markKeyFailure(index, status = 0, retryAfter = 0) {
  if (COOLDOWN_MS <= 0 || !(status === 0 || KEY_COOLDOWN_STATUS(status))) return
  const previous = cooldown.get(index) || { until: 0, failures: 0 }
  const failures = previous.failures + 1
  const exponential = Math.min(COOLDOWN_MS * 2 ** Math.min(failures - 1, 3), COOLDOWN_MS * 8)
  cooldown.set(index, { until: Date.now() + Math.max(exponential, retryAfter), failures })
}

function markKeySuccess(index) {
  cooldown.delete(index)
}

function affinityHash(value) {
  const digest = crypto.createHash("sha256").update(String(value || "default")).digest()
  return digest.readUInt32BE(0)
}

/** Stable session affinity, while skipping keys currently cooling down. */
function pickKeys(sessionID = "default") {
  const now = Date.now()
  const available = KEYS.map((_, i) => i).filter((i) => (cooldown.get(i)?.until ?? 0) <= now)
  const pool = available.length > 0 ? available : KEYS.map((_, i) => i).sort((a, b) => (cooldown.get(a)?.until ?? 0) - (cooldown.get(b)?.until ?? 0)).slice(0, 1)
  const publicIndex = pool.findIndex((i) => KEYS[i] === "public")
  if (publicIndex >= 0) return [pool[publicIndex], ...pool.slice(0, publicIndex), ...pool.slice(publicIndex + 1)]
  const start = pool.length ? affinityHash(sessionID) % pool.length : 0
  return pool.slice(start).concat(pool.slice(0, start))
}

/**
 * Request-scoped retry engine. Retryable auth/rate/server/transport failures
 * rotate keys; deterministic request errors return immediately. The timeout is
 * shared by all attempts, and public attempts receive anonymous body shaping.
 */
async function upstreamFetch(path, {
  method = "GET", body, protocol = "chat", sessionID, projectID, requestID, parentSessionID,
  ac, timeoutMs = TIMEOUT_MS, label = path, ua,
}) {
  const candidates = pickKeys(sessionID)
  const deadline = Date.now() + timeoutMs
  const maxAttempts = Math.min(RETRY_MAX_ATTEMPTS, candidates.length)
  let lastNetworkError = null
  for (let n = 0; n < maxAttempts; n++) {
    const i = candidates[n]
    const remaining = deadline - Date.now()
    if (remaining <= 0) break
    const signal = AbortSignal.any([ac.signal, AbortSignal.timeout(remaining)])
    const upstreamBody = KEYS[i] === "public" ? shapeAnonymousBody(body, protocol) : body
    try {
      const res = await fetch(`${UPSTREAM}${path}`, {
        method,
        headers: zenHeaders(sessionID, KEYS[i], ua, { projectID, requestID, parentSessionID }),
        body: upstreamBody,
        signal,
      })
      if (res.ok) {
        markKeySuccess(i)
        res.__zenKeyIndex = i
        res.__zenAnonymous = KEYS[i] === "public" && ANONYMOUS_SHAPING
        return res
      }
      if (!RETRYABLE_STATUS(res.status) || n === maxAttempts - 1) return res
      const detail = await res.clone().text().catch(() => "")
      const retryAfter = parseRetryAfter(res.headers.get("retry-after"))
      console.warn(`key #${i} (${maskKey(KEYS[i])}) failed ${label}: ${res.status} ${detail.slice(0, 160)}`)
      markKeyFailure(i, res.status, retryAfter)
      if (res.body) await res.body.cancel().catch(() => {})
    } catch (e) {
      if (ac.signal.aborted) throw e
      lastNetworkError = e
      console.warn(`key #${i} (${maskKey(KEYS[i])}) network error ${label}: ${e.message}`)
      markKeyFailure(i, 0)
    }
  }
  if (lastNetworkError) throw lastNetworkError
  return null
}

const CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, POST, OPTIONS",
  "access-control-allow-headers": "authorization, content-type, x-api-key, anthropic-version, anthropic-beta, x-conversation-id, x-proxy-key, x-opencode-session, x-opencode-project, x-opencode-request, x-parent-session-id, x-session-affinity, x-session-id",
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
// protocol bridge and anonymous stream collapse
// ---------------------------------------------------------------------------

function textFromContent(content) {
  if (typeof content === "string") return content
  if (!Array.isArray(content)) return ""
  return content.filter((part) => part?.type === "text").map((part) => part.text || "").join("")
}

function anthropicContentToOpenAI(content) {
  if (typeof content === "string") return content
  if (!Array.isArray(content)) return content == null ? "" : String(content)
  const parts = content.map((part) => {
    if (part?.type === "text") return { type: "text", text: part.text || "" }
    if (part?.type === "image" && part.source?.type === "base64") return { type: "image_url", image_url: { url: `data:${part.source.media_type || "image/png"};base64,${part.source.data}` } }
    if (part?.type === "image" && part.source?.type === "url") return { type: "image_url", image_url: { url: part.source.url } }
    return null
  }).filter(Boolean)
  return parts.length === 1 && parts[0].type === "text" ? parts[0].text : parts
}

function anthropicToChat(input) {
  const messages = []
  if (input.system) messages.push({ role: "system", content: anthropicContentToOpenAI(input.system) })
  for (const message of Array.isArray(input.messages) ? input.messages : []) {
    const blocks = Array.isArray(message.content) ? message.content : [{ type: "text", text: message.content ?? "" }]
    const normal = blocks.filter((b) => b?.type !== "tool_use" && b?.type !== "tool_result")
    const toolUses = blocks.filter((b) => b?.type === "tool_use")
    const toolResults = blocks.filter((b) => b?.type === "tool_result")
    if (message.role === "assistant") {
      const out = { role: "assistant", content: normal.length ? anthropicContentToOpenAI(normal) : null }
      if (toolUses.length) out.tool_calls = toolUses.map((b) => ({ id: b.id || rid("call"), type: "function", function: { name: b.name, arguments: JSON.stringify(b.input ?? {}) } }))
      messages.push(out)
    } else {
      if (normal.length) messages.push({ role: message.role || "user", content: anthropicContentToOpenAI(normal) })
      for (const b of toolResults) messages.push({ role: "tool", tool_call_id: b.tool_use_id, content: textFromContent(b.content) })
    }
  }
  const body = { model: input.model, messages, max_tokens: input.max_tokens ?? input.max_tokens_to_sample, stream: input.stream === true }
  for (const key of ["temperature", "top_p", "metadata", "user"]) if (input[key] !== undefined) body[key] = input[key]
  if (input.stop_sequences !== undefined) body.stop = input.stop_sequences
  if (Array.isArray(input.tools)) body.tools = input.tools.map((tool) => ({ type: "function", function: { name: tool.name, description: tool.description || "", parameters: tool.input_schema || { type: "object", properties: {} } } }))
  return body
}

function chatToAnthropic(data, model) {
  const choice = data?.choices?.[0] || {}
  const message = choice.message || {}
  const content = []
  if (typeof message.content === "string" && message.content) content.push({ type: "text", text: message.content })
  for (const call of message.tool_calls || []) {
    let input = {}
    try { input = JSON.parse(call.function?.arguments || "{}") } catch {}
    content.push({ type: "tool_use", id: call.id || rid("call"), name: call.function?.name || "tool", input })
  }
  const usage = data?.usage || {}
  const finish = choice.finish_reason
  return { id: data?.id || rid("msg"), type: "message", role: "assistant", model, content, stop_reason: finish === "tool_calls" ? "tool_use" : finish === "length" ? "max_tokens" : "end_turn", stop_sequence: null, usage: { input_tokens: usage.prompt_tokens || 0, output_tokens: usage.completion_tokens || 0 } }
}

function sseJSONEvents(text) {
  const events = []
  let event = "message"
  let data = []
  const flush = () => {
    if (!data.length) return
    const raw = data.join("\\n")
    if (raw !== "[DONE]") try { events.push({ event, data: JSON.parse(raw) }) } catch {}
    event = "message"
    data = []
  }
  for (const line of text.split(/\\r?\\n/)) {
    if (!line) { flush(); continue }
    if (line.startsWith("event:")) event = line.slice(6).trim()
    else if (line.startsWith("data:")) data.push(line.slice(5).trimStart())
  }
  flush()
  return events
}

async function collapseUpstreamSSE(response, protocol, model) {
  const events = sseJSONEvents(await response.text())
  if (protocol === "responses") {
    const completed = events.find((e) => (e.event === "response.completed" || e.data?.type === "response.completed") && e.data?.response)
    if (completed) return completed.data.response
    let text = ""
    for (const e of events) if (e.event === "response.output_text.delta" || e.data?.type === "response.output_text.delta") text += e.data?.delta || ""
    return { id: rid("resp"), object: "response", model, output: [{ type: "message", role: "assistant", content: [{ type: "output_text", text, annotations: [] }] }] }
  }
  const base = { id: rid("chatcmpl"), object: "chat.completion", model, choices: [{ index: 0, message: { role: "assistant", content: "" }, finish_reason: "stop" }], usage: {} }
  for (const e of events) {
    const choice = e.data?.choices?.[0]
    if (choice?.delta?.content) base.choices[0].message.content += choice.delta.content
    if (choice?.delta?.reasoning) base.choices[0].message.reasoning = (base.choices[0].message.reasoning || "") + choice.delta.reasoning
    if (choice?.finish_reason) base.choices[0].finish_reason = choice.finish_reason
    if (e.data?.usage) base.usage = e.data.usage
  }
  return base
}

function writeAnthropicEvent(res, event, data) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
}

async function streamChatAsAnthropic(upstream, res, model, requestID) {
  const contentType = upstream.headers.get("content-type") || ""
  const raw = await upstream.text()
  if (!contentType.includes("text/event-stream")) {
    const converted = chatToAnthropic(JSON.parse(raw), model)
    writeAnthropicEvent(res, "message_start", { type: "message_start", message: { ...converted, content: [], stop_reason: null } })
    for (const block of converted.content || []) {
      writeAnthropicEvent(res, "content_block_start", { type: "content_block_start", index: 0, content_block: block.type === "text" ? { type: "text", text: "" } : block })
      if (block.type === "text") writeAnthropicEvent(res, "content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: block.text } })
      writeAnthropicEvent(res, "content_block_stop", { type: "content_block_stop", index: 0 })
    }
    writeAnthropicEvent(res, "message_delta", { type: "message_delta", delta: { stop_reason: converted.stop_reason, stop_sequence: null }, usage: converted.usage })
    writeAnthropicEvent(res, "message_stop", { type: "message_stop" })
    return res.end()
  }
  const events = sseJSONEvents(raw)
  const messageID = requestID || rid("msg")
  writeAnthropicEvent(res, "message_start", { type: "message_start", message: { id: messageID, type: "message", role: "assistant", model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 0, output_tokens: 0 } } })
  let block = false
  let outputTokens = 0
  for (const e of events) {
    const choice = e.data?.choices?.[0]
    const delta = choice?.delta
    if (delta?.content) {
      if (!block) { writeAnthropicEvent(res, "content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }); block = true }
      outputTokens++
      writeAnthropicEvent(res, "content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: delta.content } })
    }
    if (choice?.finish_reason) {
      if (block) writeAnthropicEvent(res, "content_block_stop", { type: "content_block_stop", index: 0 })
      writeAnthropicEvent(res, "message_delta", { type: "message_delta", delta: { stop_reason: choice.finish_reason === "length" ? "max_tokens" : "end_turn", stop_sequence: null }, usage: { output_tokens: outputTokens } })
    }
  }
  writeAnthropicEvent(res, "message_stop", { type: "message_stop" })
  res.end()
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

  const ids = requestIDs(req, body)
  const sessionID = ids.sessionID

  const ac = new AbortController()
  res.on("close", () => ac.abort()) // client went away mid-stream -> stop upstream

  let upstream
  try {
    upstream = await upstreamFetch("/chat/completions", {
      method: "POST",
      body: JSON.stringify(body),
      protocol: "chat",
      sessionID,
      projectID: ids.projectID,
      requestID: ids.requestID,
      parentSessionID: ids.parentSessionID,
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

  if (upstream.__zenAnonymous && body.stream !== true && upstream.headers.get("content-type")?.includes("text/event-stream")) {
    const collapsed = await collapseUpstreamSSE(upstream, "chat", model)
    return json(res, collapsed, upstream.status)
  }

  log(
    "POST",
    "/v1/chat/completions",
    model,
    upstream.status,
    performance.now() - t0,
    process.env.DEBUG_IDS ? `sid=${sessionID.slice(4, 14)} key-affinity` : "",
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
  let parsedBody = {}
  try {
    parsedBody = raw.length > 0 ? JSON.parse(raw.toString("utf8")) : {}
  } catch {
    return oaiError(res, 400, "request body must be valid JSON (<=10MB)")
  }
  const ids = requestIDs(req, parsedBody)
  const sessionID = ids.sessionID
  const protocol = upstreamPath.startsWith("/responses")
    ? "responses"
    : upstreamPath.startsWith("/systemone")
      ? "systemone"
      : "chat"
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
        protocol,
        sessionID,
        projectID: ids.projectID,
        requestID: ids.requestID,
        parentSessionID: ids.parentSessionID,
        ac,
        label: upstreamPath,
        ua,
      })
      if (!upstream) {
        log(req.method, upstreamPath, undefined, 502, performance.now() - t0, "all keys failed")
        return oaiError(res, 502, "zen-proxy: all upstream keys failed", "api_error")
      }

      if (upstream.__zenAnonymous && parsedBody.stream !== true && upstream.headers.get("content-type")?.includes("text/event-stream")) {
        const collapsed = await collapseUpstreamSSE(upstream, protocol, parsedBody.model)
        return json(res, collapsed, upstream.status)
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

async function anthropicMessages(req, res) {
  const t0 = performance.now()
  let input
  try {
    input = JSON.parse((await readBody(req)).toString("utf8"))
  } catch {
    return oaiError(res, 400, "request body must be valid JSON (<=10MB)")
  }
  if (typeof input?.model !== "string") return oaiError(res, 400, "`model` is required")
  const body = anthropicToChat(input)
  const ids = requestIDs(req, input)
  const ac = new AbortController()
  res.on("close", () => ac.abort())
  let upstream
  try {
    upstream = await upstreamFetch("/chat/completions", {
      method: "POST",
      body: JSON.stringify(body),
      protocol: "chat",
      sessionID: ids.sessionID,
      projectID: ids.projectID,
      requestID: ids.requestID,
      parentSessionID: ids.parentSessionID,
      ac,
      label: `messages(${input.model})`,
      ua: UA_SDK_CHAT,
    })
  } catch (e) {
    return oaiError(res, 502, `zen-proxy: ${e.message}`, "api_error")
  }
  if (!upstream) return oaiError(res, 502, "zen-proxy: all upstream keys failed", "api_error")
  if (!upstream.ok) {
    const text = await upstream.text().catch(() => "")
    res.writeHead(upstream.status, { ...CORS, "content-type": upstream.headers.get("content-type") || "application/json" })
    return res.end(text)
  }
  const isStream = input.stream === true
  if (isStream) {
    res.writeHead(200, { ...CORS, "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache" })
    return streamChatAsAnthropic(upstream, res, input.model, ids.requestID)
  }
  const data = upstream.headers.get("content-type")?.includes("text/event-stream")
    ? await collapseUpstreamSSE(upstream, "chat", input.model)
    : await upstream.json()
  log("POST", "/v1/messages", input.model, 200, performance.now() - t0)
  return json(res, chatToAnthropic(data, input.model))
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
  if (path === "/v1/messages" && req.method === "POST") return anthropicMessages(req, res)

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
