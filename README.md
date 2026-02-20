# Future Newspaper with Codex Spark-style Real-time Rendering

This project now includes a lightweight Node backend and WebSocket pipeline so article pages are rendered in near real-time when a user clicks.

## How to view
Run locally:
```bash
npm install
npm start
```

Then open:
`http://localhost:45678`

If 45678 is occupied, the server automatically increments by 17 and tries the next port.

## What’s included
- Backend service with API + WebSocket endpoints:
  - `GET /api/edition?years=5`
  - `GET /api/article/:id?years=5`
  - `POST /api/article/:id/render?years=5`
  - `WS /ws`
- Front page with clickable article links
- Live article stream on click:
  - progress updates
  - streamed chunks
  - final full article payload
- Simulated Codex Spark renderer pipeline (replaceable with a real provider easily)
- Article pages include:
  - “Signals from 2026” sidebar
  - “Market snapshot” sidebar
  - Image prompt per article

## Notes
- All generated copy is still speculative and illustrative.
- This implementation first tries a Codex-style Spark path when configured, then safely falls back to the local deterministic renderer.
- Port selection is uncommon by default (`45678`) and auto-adjusts if needed.
- You can force real rendering with:
  - `SPARK_MODE=spark`
  - `SPARK_WS_URL=wss://...`
  - `SPARK_AUTH_TOKEN=...`
  - optional: `SPARK_AUTH_HEADER=Authorization`
  - optional: `SPARK_AUTH_PREFIX=Bearer`
  - optional: `SPARK_FALLBACK_TO_MOCK=false` (to fail fast if Spark is unavailable)

## Daily curation (Sonnet → Spark guidance)
The pipeline can run a daily "curation" step that:
- Uses a higher-quality model (configured as "Sonnet 4.6") to rewrite headlines/deks and produce short per-story directions.
- Prewrites only a small number of key stories per edition (stored in the normal render cache so they load instantly).
- Leaves secondary stories as lightweight directives so the fast model (Codex Spark) writes them on click.

Configure via environment variables:
- `SONNET_MODE=mock` (default) or `SONNET_MODE=anthropic` or `SONNET_MODE=openai`
- `SONNET_MODEL=sonnet-4.6`
- `SONNET_API_KEY=...` (required for `anthropic` / `openai`)
- `SONNET_API_URL=...` (optional override; defaults to the provider's standard endpoint)
- `SONNET_SYSTEM_PROMPT=...` (optional; overrides the provider system prompt)
- `SONNET_KEY_STORIES_PER_EDITION=1` (how many stories per +year edition to prewrite)
- `SONNET_MAX_TOKENS=4500`
- `SONNET_TIMEOUT_MS=60000`

Run the daily worker (schedules by `PIPELINE_DAILY_HHMM`, default `05:30` local):
```bash
npm run worker
```

Manual trigger (server must be running):
```bash
curl -X POST "http://localhost:57965/api/admin/curate?force=true"
```

Admin dashboard (prompt + traces, browser-editable):
`/api/admin/dashboard`

Recommended daily cron target (refresh then curate; idempotent once/day unless forced):
```bash
curl -X POST "http://localhost:57965/api/admin/daily"
```

### Vercel env via CLI
If deploying to Vercel, you can set env vars via the CLI:
```bash
vercel env add SONNET_MODE production
vercel env add SONNET_MODEL production
vercel env add SONNET_API_KEY production
vercel env add SONNET_KEY_STORIES_PER_EDITION production
vercel env add SONNET_MAX_TOKENS production
vercel env add SONNET_TIMEOUT_MS production
```
Then redeploy (or trigger a new deployment) so the env vars apply.

The incoming message contract is expected to support progress/chunk/complete streams. The adapter maps these fields:
- `type: render.progress` with `phase` + `percent`
- `type: render.chunk` with `delta`
- `type: render.complete` with `article` (or `body`)
- `type: render.error` with `error`

If your provider differs, update `server/server.js` in `normalizeProviderEvent(...)`.
- In production, fetch live probabilities from:
  - Kalshi Exchange API: https://docs.kalshi.com/
  - Polymarket APIs (Gamma/CLOB): https://docs.polymarket.com/
