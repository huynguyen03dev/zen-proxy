# zen-proxy

Thin OpenAI-compatible proxy in front of [OpenCode Zen](https://opencode.ai) free models.
One Node.js file (>=20), zero dependencies. Injects the headers the free tier requires
(`x-opencode-session/-project/-request/-client` + opencode User-Agent, key `public`),
streams SSE passthrough, blocks non-free models.

## Key failover

`ZEN_API_KEYS` là danh sách key thử theo thứ tự (mặc định `"public"`). **Bất kỳ
lỗi nào** từ upstream (429, 401 với model paid, model không tồn tại, 5xx, lỗi
mạng…) đều tự thử key tiếp theo **trong cùng request đó**. Riêng lỗi cấp key
(`429` / `5xx` / lỗi mạng) mới đặt cooldown (`FAILOVER_COOLDOWN_MS`, mặc định
60s) — lỗi cấp request (vd tên model sai) không làm key bị cool. Hết cooldown
thì quay lại thử key trước.

```bash
ZEN_API_KEYS="public,sk-your-real-key" node server.js   # public trước, key thật dự phòng
ZEN_API_KEYS="sk-your-real-key" node server.js          # chỉ dùng key thật
```

Vì chat là pass-through: gọi model paid khi chain có key thật sẽ tự fail-over
sang key thật và **tính tiền** — key `public` chỉ đủ quyền cho model free.

## Model list (display only — chat is never blocked here)

Zen exposes no pricing, so the proxy reads the same catalog opencode itself
loads — `models.opencode.ai/api.json`, provider `opencode` — and `/v1/models`
lists models with `cost.input === 0 && cost.output === 0` (the exact rule
opencode applies for keyless users), intersected with the **live** zen list so
entries the server dropped (e.g. `grok-code`) disappear automatically. It's a
**hint, not a gate**: chat requests pass through untouched and zen decides what
a key may call, so a brand-new model works the moment zen supports it — no
redeploy, no wait. Overrides shape the display list only:

| Var | Effect |
| --- | --- |
| `ALLOW_MODELS="a,b,c"` | explicit display list, checked first |
| `ALLOW_MODELS="*"` | show the full live list |
| `EXTRA_MODELS="x,y"` | extra ids shown in free-only mode |
| fallback | `-free` suffix heuristic until the catalog loads |

## Identity strategy (SESSION_MODE)

| Mode | session id (`x-opencode-session`) | project id (`x-opencode-project`) |
| --- | --- | --- |
| `derived` *(default)* | `HMAC(seed, "ses:<UTC date>")` — same UTC day → same id | `HMAC(seed, "proj:<ISO week>")` — same week → same id |
| `sticky` | one per conversation (`X-Conversation-Id` header / body `user`), 30 min idle TTL | static, random at boot or `OPENCODE_PROJECT_ID` |
| `per-request` | fresh every call | static |

`x-opencode-request` is always fresh. The seed is **unique per deployment**:
`OPENCODE_SECRET` (if set) → `RENDER_SERVICE_ID` (Render auto-provides, unique
per service, survives redeploys) → random at boot. Two instances sharing a
proxy key never share identity. Inspect live values at `GET /debug/ids`
(includes `seed_source`).


## Run locally

```bash
node server.js                               # :8787, open auth, free-only, derived ids
PROXY_KEY=dev123 node server.js              # require Authorization: Bearer dev123
OPENCODE_SECRET=myseed node server.js        # stable day/week ids across restarts
DEBUG_IDS=1 node server.js                   # log session id per chat request
```

## Deploy on Render

1. Push this folder to a GitHub repo.
2. Render → **New +** → **Blueprint** → pick the repo (uses `render.yaml`).
3. `PROXY_KEY` is auto-generated — find it under the service's Environment tab.
4. Point your OpenAI client at `https://<service>.onrender.com/v1`.

## Config

| Var | Default | Meaning |
| --- | --- | --- |
| `PORT` | `8787` | listen port |
| `PROXY_KEY` | unset (= open) | clients must send `Authorization: Bearer <key>` or `X-Proxy-Key` |
| `OPENCODE_SECRET` | `RENDER_SERVICE_ID` / random | manual seed override — on Render leave it unset and each service gets its own stable identity |
| `ZEN_UPSTREAM` | `https://opencode.ai/zen/v1` | upstream base URL |
| `ZEN_API_KEYS` | `public` | key chain thử theo thứ tự, phân cách bởi dấu phẩy (fallback: `ZEN_API_KEY`) |
| `FAILOVER_COOLDOWN_MS` | `60000` | thời gian bỏ qua key vừa fail (0 = tắt) |
| `CATALOG_URL` | `https://models.opencode.ai/api.json` | catalog source (same as opencode) |
| `CATALOG_PROVIDER` | `opencode` | provider id inside the catalog |
| `CATALOG_TTL_MS` | `3600000` | catalog refresh interval |
| `SESSION_MODE` | `derived` | `derived` / `sticky` / `per-request` — see table above |
| `DEBUG_IDS` | off | log session id on each chat request |
| `SESSION_TTL_MS` | `1800000` | sticky session idle TTL |
| `OPENCODE_PROJECT_ID` | random at boot | stable `x-opencode-project` |
| `OPENCODE_CLIENT` | `tui` | `x-opencode-client` + last UA segment |
| `OPENCODE_CHANNEL` / `OPENCODE_VERSION` | `dev` / `1.18.31` | UA `opencode/<channel>/<version>/<client>` |
| `TIMEOUT_MS` | `600000` | upstream request timeout (client disconnects abort upstream) |

## Use it

`/v1/chat/completions` + `/v1/models` are first-class; **any other `/v1/*` path**
(e.g. `/v1/responses` for reasoning models like muse-spark, `/v1/completions`)
is raw pass-through with the same key failover — some zen models only answer on
the OpenAI Responses API, so point Responses-aware clients at the proxy root.

```bash
export BASE=https://<service>.onrender.com/v1
export KEY="Bearer <PROXY_KEY>"

curl $BASE/models -H "Authorization: $KEY"

curl $BASE/chat/completions -H "Authorization: $KEY" -H "Content-Type: application/json" \
  -d '{"model":"mimo-v2.5-free","messages":[{"role":"user","content":"hi"}]}'

# streaming works (SSE passthrough)
curl -N $BASE/chat/completions -H "Authorization: $KEY" -H "Content-Type: application/json" \
  -d '{"model":"mimo-v2.5-free","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

## Rate-limit A/B test plan

Goal: learn whether the free-tier throttle is keyed on IP (Render IP differs
from your home IP) or on injected identity (session/project fingerprint).

1. **Baseline (home IP, direct):** hammer `https://opencode.ai/zen/v1/chat/completions`
   directly with the header set (no proxy), N=30 requests, no delay. Log status codes.
2. **Home IP via proxy:** same N through a locally-running proxy.
3. **Render IP:** same N through the deployed proxy. Repeat once after 15 min
   idle (cold start) and once warm.
4. Compare 429/error rates. Watch `retry-after` / `x-ratelimit-*` response
   headers (the proxy forwards them verbatim).

Interpretation:
- Render clean while home throttled → IP-keyed (at least partly).
- Both throttle the same → identity/behavior-keyed; try `SESSION_MODE=sticky`
  vs `per-request` and different `OPENCODE_PROJECT_ID` to probe further.
- Render throttled *harder* → datacenter IP flagged (Cloudflare risk scoring);
  home IP is the better vantage point.

## Notes

- Render free instances sleep after ~15 min idle; first request pays a cold start.
- Render outbound IPs are shared per region — a "fresh" IP may already have history.
- The upstream free tier is intended for the opencode client; wrapping it for
  other tools likely violates its terms of service. Use for personal testing.
