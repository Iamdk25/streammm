# streammm / streamaid

Low-latency **native-res 30 fps screen streaming** (capped at 4K, never upscaled): capture on your machine, play in any browser, fan out through **Cloudflare Workers + Durable Objects**.

The origin is a **Rust** binary (`cargo run`). It encodes with ffmpeg (VideoToolbox on macOS, libx264 elsewhere) and delivers **WebSocket** fMP4 — not HTTP progressive download. That is the difference between localhost looking fine and Cloudflare looking like a stalled VOD.

## Why WebSocket (not `/stream.mp4` through a tunnel)

`cloudflared` buffers proxied HTTP unless `Content-Type: text/event-stream`. A live `video/mp4` body is held until the connection dies. WebSocket (`/stream.ws`, and the Worker `/publish` → `/watch` path) is not buffered that way.

The Worker also means **one upload** from home (the publisher) and **N viewers** at the edge.

## Quick start (this computer)

```bash
# ffmpeg on PATH. macOS: Screen Recording permission for the capturing app.
cargo run --release
# open http://127.0.0.1:8080
```

On this Mac the origin is a **LaunchAgent** (`com.streamaid.origin`). It stays up across terminal/chat sessions and login, and only stops when you unload it:

```bash
# start (and keep running)
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.streamaid.origin.plist

# stop — this is the off switch
launchctl bootout gui/$(id -u)/com.streamaid.origin
```

Killing the process is not enough — `KeepAlive` will restart it. Logs: `logs/origin.out.log`. If capture is black after a reboot, allow **Streamaid** under System Settings → Privacy & Security → Screen Recording.

`./config.json` is created with defaults if missing (gitignored).

| Default | Value |
|---------|--------|
| Encoder | H.264 (`ffmpeg`), High profile, no B-frames |
| Bitrate | 20 Mbps CBR, 0.5 s VBV (up to 50 Mbps in Settings) |
| GOP | 15 frames (~0.5 s join / live edge) |
| Size | cap **3840×4320**, **never upscale**, lanczos only if the display is larger |
| fps | 30 |
| Snapshot | every **8 s** (`snapshot_interval_secs`, 5–3600 s) while the capture produces new media, ≤1920 px wide, JPEG q6, published to the Worker for analysis (a stalled capture publishes nothing) |

## Global watch link (OTP PIN)

1. Keep the origin running on the capture machine.
2. Deploy the Worker (below).
3. Set origin `cloudflare.publish_url` / `watch_url` (or the Settings fields in the UI). The **publish** URL still includes `STREAM_TOKEN` (that is the stream-key secret; do not put it on the public watch page).
4. Share the Worker homepage with **no token in the URL**:

```
https://<your-worker>.workers.dev/
```

The host UI (`http://127.0.0.1:8080`) shows a live **6-digit PIN** (about 5 minutes; regenerate from Settings / `POST /api/otp`). The remote person types that PIN on the Worker page or localhost login overlay. Redeem issues a short-lived `streamaid_session` cookie. Wrong PINs are 401 and rate-limited.

`STREAM_TOKEN` stays the publisher secret (`npx wrangler secret put STREAM_TOKEN`). It is not the human watch password.

The Worker homepage is a player with PIN unlock, **Have AI use this computer**, and a screen analysis + Ask sidebar. `/watch` is the viewer WebSocket (session); `/publish` is ingest from the origin; `/health` is public.

## Remote computer use

Remote control is **off by default**. On the origin UI, enable:

- **Allow remote computer use** — the remote watcher can click / move / scroll / type on the live video (normalized 0–1 coordinates). Kill switch: uncheck it.
- **Allow AI computer use** — the same watcher can submit a task in **Have AI use this computer**. The origin loops fresh-snapshot JPEG → vision model JSON actions → the same injector: every step after the first waits for a new snapshot, so a step never re-plans on a stale frame and a stalled capture stops the loop instead of burning calls. **Cancel AI** on the origin UI (`POST /api/computer-use/cancel`) stops a running loop.

macOS: grant **Streamaid** Accessibility (and Input Monitoring if prompted) in System Settings → Privacy & Security, in addition to Screen Recording. CI tests use a fake injector; they never post real HID events.

One remote controller at a time. Local host input / the kill switch always wins.

Control path: viewer WebSocket JSON → Durable Object → publisher WebSocket → origin injector. Type-4 LLM snapshots still never go to viewers.

Analysis does not call DeepSeek until you attach a key:

```bash
cd cloudflare
npx wrangler secret put DEEPSEEK_API_KEY
```

Until then the APIs return `503` with setup instructions. The origin already publishes a JPEG snapshot (`type 4`) every `snapshot_interval_secs` (default 8 s) while capturing. Viewers never receive those snapshots — they stay in the Durable Object for the model.

## Cloudflare deploy

```bash
cd cloudflare
npm install
npx wrangler login          # once
npx wrangler secret put STREAM_TOKEN
npx wrangler deploy
```

Then on the origin (UI Settings, or `POST /api/config`):

```json
{
  "cloudflare": {
    "publish_url": "wss://<your-worker>.workers.dev/publish?token=<STREAM_TOKEN>",
    "watch_url": "wss://<your-worker>.workers.dev/watch"
  }
}
```

Restart the origin after changing publish URL if it was already connected.

Optional: `cloudflare/cloudflared.yml` if you want the **UI** on a tunnel hostname. Do not send HTTP `/stream.mp4` through the tunnel.

## Streams (origin)

| Path | Role |
|------|------|
| `GET /stream.ws` | **Primary.** Binary: `1` = fMP4 init, `2` = fragment, `3` = JPEG, `4` = LLM snapshot (origin → Worker only) |
| `GET /stream.mp4` | LAN fallback, `Transfer-Encoding: chunked` (buffered on Tunnel) |
| `GET /stream.mjpeg` | MJPEG compatibility |

Player live-edge is ~0.45 s with `playbackRate` catch-up. It appends by type byte, not by sniffing `ftyp` in a TCP chunk.

## API (origin)

```
GET  /                UI (shows live PIN + kill switches)
GET  /api/status      capture/stream JSON
GET  /api/config      full config        POST /api/config   apply+save
GET  /api/otp         host: current PIN   POST /api/otp     regenerate PIN
POST /api/otp/redeem  `{ "pin": "123456" }` → session cookie
POST /api/computer-use `{ "task": "…" }`  (403 unless AI control enabled)
GET  /api/events      SSE
GET  /api/capture-devices
```

Host token: `Authorization: Bearer`, `?token=`, or `streamaid_token` cookie (config, PIN mint). Viewer session: `streamaid_session` cookie or `?session=` (watch + computer-use). Empty token keeps open local-dev mode.

## Screen analysis (Cloudflare Worker)

The Durable Object (`StreamRoom`) stores the latest JPEG snapshot, runs vision on it, and serves Q&A on the same watch page.

| Path | Role |
|------|------|
| `GET /api/analysis` | Latest summary, on-screen Q&A, history, `{configured, has_snapshot, model}` |
| `GET /api/llm-status` | Whether `DEEPSEEK_API_KEY` is set and a snapshot has arrived |
| `POST /api/analyze-now` | Force a vision pass now |
| `POST /api/ask` | `{ "question": "…" }` → `{ answer, confidence, reasoning }` |

Model defaults (override with wrangler `vars`):

- `DEEPSEEK_BASE_URL` = `https://api.deepseek.com`
- `DEEPSEEK_MODEL` = `deepseek-v4-flash-vision-exp`

Same-origin player polls `/api/analysis` and also accepts a WebSocket JSON message `{ "type": "analysis", "data": … }` when a pass finishes.

Credit guards: an automatic pass reuses the last result when the snapshot is byte-identical to the one already analyzed (no paid call for a static screen); `/api/analyze-now` forces a pass but is limited to one per 5 s; each analysis stores the model's reported token `usage` and logs it.

## Tests

```bash
cargo test                 # unit + HTTP API + player contract + stress
cargo test --test stress   # 30 fps hub budget, 16 WS clients, real 1080p30 ffmpeg encode
cd cloudflare && npm test  # Durable Object: 401, fan-out, late join, publisher replace, snap isolation, 503 without DeepSeek key
```

## Layout

```
src/            Rust origin (encode, hub, HTTP/WS, publisher)
web/            local UI
cloudflare/     Worker + StreamRoom Durable Object + public player
tests/          cargo tests (HTTP API, player contract, 1080p30 stress)
```
