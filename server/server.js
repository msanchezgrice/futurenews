import { createHash } from 'node:crypto';
import fs from 'node:fs';
import http from 'node:http';
import net from 'node:net';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { WebSocket, WebSocketServer } from 'ws';

import { FutureTimesPipeline } from './pipeline/pipeline.js';
import { clampYears, formatDay, normalizeDay } from './pipeline/utils.js';
import { buildEditionCurationPrompt, getOpusCurationConfigFromEnv } from './pipeline/curation.js';
import { getRuntimeConfigInfo, readRuntimeConfig, readOpusRuntimeConfig, updateOpusRuntimeConfig } from './pipeline/runtimeConfig.js';
import { decorateArticlePayload, decorateEditionPayload } from './future_images/decorators.js';
import { refreshIdeas } from './future_images/ideas.js';
import { enqueueSectionHeroJobs, enqueueSingleIdeaJob, enqueueSingleStoryHeroJob, runImageWorker } from './future_images/jobs.js';
import { getImagesAdminState } from './future_images/state.js';
import { renderImagesAdminHtml } from './future_images/ui.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');

const DEFAULT_HOST = process.env.HOST || '127.0.0.1';
const PORT_START = Number(process.env.PORT || 57965);
const PORT_FALLBACK_STEP = Number(process.env.PORT_FALLBACK_STEP || 53);
const PORT_MAX_TRIES = Number(process.env.PORT_MAX_TRIES || 48);

// Articles are rendered by Anthropic Sonnet API — no Spark/mock renderers
const EDITION_YEARS = 5; // Only +5y edition for now

const PIPELINE_REFRESH_MS = Number(process.env.PIPELINE_REFRESH_MS || 1000 * 60 * 60);
const AUTO_CURATE_DEFAULT = process.env.OPUS_AUTO_CURATE !== 'false';
const MAX_BODY_CHUNK_BYTES = 740;
const JOB_TTL_MS = 1000 * 60 * 10;

// ── Admin auth ──
// Check env, then runtime config
const _envAdminSecret = process.env.ADMIN_SECRET || process.env.FT_ADMIN_SECRET || '';
const ADMIN_SECRET = _envAdminSecret || (() => {
  try {
    const cfg = readRuntimeConfig();
    return cfg && typeof cfg === 'object' ? String(cfg.adminSecret || '') : '';
  } catch { return ''; }
})();
const ADMIN_COOKIE_NAME = 'ft_admin';
const ADMIN_COOKIE_MAX_AGE = 60 * 60 * 24 * 30; // 30 days

function checkAdminAuth(req, url) {
  if (!ADMIN_SECRET) return true; // no secret configured = open access (local dev)
  // Cron-friendly auth: Authorization: Bearer <secret>
  const auth = String(req.headers.authorization || '').trim();
  const matchAuth = auth.match(/^Bearer\s+(.+)$/i);
  if (matchAuth && String(matchAuth[1] || '').trim() === ADMIN_SECRET) return true;
  // Check query param
  const qsSecret = url.searchParams.get('secret') || '';
  if (qsSecret === ADMIN_SECRET) return 'set-cookie'; // valid, set cookie
  // Check cookie
  const cookies = String(req.headers.cookie || '');
  const match = cookies.match(new RegExp(`(?:^|;)\\s*${ADMIN_COOKIE_NAME}=([^;]+)`));
  if (match && match[1] === ADMIN_SECRET) return true;
  return false;
}

// ── Cron auth ──
// Vercel Cron optionally signs scheduled requests with:
//   Authorization: Bearer <CRON_SECRET>
// When unset, cron routes are open (local dev).
const CRON_SECRET = String(process.env.CRON_SECRET || '').trim();

function checkCronAuth(req, url) {
  if (!CRON_SECRET && !ADMIN_SECRET) return true;
  const auth = String(req.headers.authorization || '').trim();
  const matchAuth = auth.match(/^Bearer\s+(.+)$/i);
  const token = matchAuth ? String(matchAuth[1] || '').trim() : '';
  if (token && ADMIN_SECRET && token === ADMIN_SECRET) return true;
  if (token && CRON_SECRET && token === CRON_SECRET) return true;
  const qs = String(url.searchParams.get('cronSecret') || '').trim();
  if (qs && CRON_SECRET && qs === CRON_SECRET) return true;
  return false;
}

function sendAdminLogin(res) {
  const html = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Admin Login - The Future Times</title>
<style>
  body{font-family:-apple-system,system-ui,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;background:#f9f9f9}
  .box{background:#fff;border:1px solid #ddd;border-radius:8px;padding:32px;max-width:360px;width:100%;text-align:center}
  h1{font-size:18px;margin:0 0 16px}
  input{width:100%;padding:10px;border:1px solid #ccc;border-radius:4px;font-size:14px;box-sizing:border-box;margin-bottom:12px}
  button{width:100%;padding:10px;background:#111;color:#fff;border:none;border-radius:4px;font-size:14px;cursor:pointer}
  button:hover{background:#333}
  .err{color:#c62828;font-size:13px;margin-top:8px;display:none}
</style></head><body>
<div class="box">
  <h1>The Future Times — Admin</h1>
  <form id="f">
    <input type="password" id="s" placeholder="Admin password" autofocus/>
    <button type="submit">Sign in</button>
    <div class="err" id="e">Invalid password</div>
  </form>
</div>
<script>
document.getElementById('f').addEventListener('submit', function(e) {
  e.preventDefault();
  var s = document.getElementById('s').value.trim();
  if (!s) return;
  var url = new URL(location.href);
  url.searchParams.set('secret', s);
  location.href = url.toString();
});
// Show error if redirected back with bad secret
if (location.search.includes('auth=failed')) document.getElementById('e').style.display='block';
</script></body></html>`;
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
}

const mimeByExt = new Map([
  ['.html', 'text/html; charset=utf-8'],
  ['.css', 'text/css; charset=utf-8'],
  ['.js', 'application/javascript; charset=utf-8'],
  ['.svg', 'image/svg+xml'],
  ['.json', 'application/json; charset=utf-8'],
  ['.png', 'image/png'],
  ['.jpg', 'image/jpeg'],
  ['.jpeg', 'image/jpeg'],
  ['.gif', 'image/gif'],
  ['.webp', 'image/webp'],
  ['.ico', 'image/x-icon']
]);

const pipeline = new FutureTimesPipeline({
  rootDir: ROOT_DIR,
  dbFile: process.env.PIPELINE_DB_FILE || path.resolve(ROOT_DIR, 'data', 'future-times.sqlite'),
  sourcesFile: process.env.PIPELINE_SOURCES_FILE || path.resolve(ROOT_DIR, 'config', 'sources.json')
});
pipeline.init();

const jobStore = new Map(); // key -> job
const cacheByKey = new Map(); // storyId -> rendered article
const socketToSubscriptions = new Map(); // socket -> Set<subscription>
let activePort = null;

startPipelineScheduler();

function startPipelineScheduler() {
  const autoCurateEnabled = () => {
    const raw = String(process.env.OPUS_AUTO_CURATE || '').trim().toLowerCase();
    if (!raw) return AUTO_CURATE_DEFAULT;
    return raw !== 'false' && raw !== '0' && raw !== 'off' && raw !== 'no';
  };
  const tick = () => {
    const day = formatDay();
    return pipeline
      .refresh({ day, force: false })
      .then(() => (autoCurateEnabled() ? pipeline.curateDay(day, { force: false }) : null))
      .catch(() => {});
  };

  // Best-effort, non-blocking refresh (and curation) on boot.
  void tick();
  const timer = setInterval(() => void tick(), PIPELINE_REFRESH_MS);
  if (timer.unref) timer.unref();
}

function keyFor(storyId, story = null) {
  const id = story && story.storyId ? String(story.storyId) : String(storyId || '');
  const curationAt = story && story.curation ? String(story.curation.generatedAt || '') : '';
  try {
    return pipeline.buildRenderCacheKey(id, { curationGeneratedAt: curationAt });
  } catch {
    return id;
  }
}

function splitToChunks(text, size) {
  const normalized = String(text || '');
  if (!normalized) return [];
  if (normalized.length <= size) return [normalized];
  const chunks = [];
  for (let i = 0; i < normalized.length; i += size) {
    chunks.push(normalized.slice(i, i + size));
  }
  return chunks;
}

function randomDelay(minMs, maxMs) {
  return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
}

async function sleep(ms) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function safeSend(socket, payload) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return false;
  try {
    socket.send(JSON.stringify(payload));
    return true;
  } catch {
    return false;
  }
}

function broadcastToJobSubscribers(job, payload) {
  for (const subscription of Array.from(job.subscribers)) {
    const sent = safeSend(subscription.socket, {
      ...payload,
      requestId: subscription.requestId,
      articleId: job.storyId
    });
    if (!sent) {
      job.subscribers.delete(subscription);
    }
  }
}

function subscribeSocketToJob(job, socket, requestId) {
  const subscription = { socket, requestId, jobKey: job.key };
  job.subscribers.add(subscription);
  const socketSubs = socketToSubscriptions.get(socket) || new Set();
  socketSubs.add(subscription);
  socketToSubscriptions.set(socket, socketSubs);
}

function removeSocket(socket) {
  const socketSubs = socketToSubscriptions.get(socket);
  if (!socketSubs) return;
  for (const sub of socketSubs) {
    const job = jobStore.get(sub.jobKey);
    if (job) job.subscribers.delete(sub);
  }
  socketToSubscriptions.delete(socket);
}

function finalizeJobCleanup(job) {
  const timer = setTimeout(() => {
    if (jobStore.has(job.key) && job.complete) {
      jobStore.delete(job.key);
    }
  }, JOB_TTL_MS);
  if (timer.unref) timer.unref();
}

function describeYearsForward(yearsForward) {
  const y = Number(yearsForward) || 0;
  if (y === 0) return 'today';
  if (y === 1) return 'one year';
  if (y === 2) return 'two years';
  if (y === 3) return 'three years';
  if (y === 5) return 'five years';
  if (y === 10) return 'ten years';
  return `${y} years`;
}

function stableHash32(value) {
  const hex = createHash('sha256').update(String(value || ''), 'utf8').digest('hex').slice(0, 8);
  return Number.parseInt(hex, 16) >>> 0;
}

function pickStable(items, seed) {
  if (!Array.isArray(items) || !items.length) return null;
  const idx = stableHash32(seed) % items.length;
  return items[idx];
}

function stripAuthorSuffix(title) {
  return String(title || '')
    .replace(/\s*\|\s*[^|]{2,120}$/g, '')
    .replace(/\s+-\s+[^-]{2,120}$/g, '')
    .trim();
}

function cleanTheme(text) {
  let theme = String(text || '').replace(/\s+/g, ' ').trim();
  if (!theme) return '';
  theme = stripAuthorSuffix(theme);
  theme = theme.replace(/\?+$/g, '').replace(/[.:]\s*$/g, '').trim();
  theme = theme.replace(/^(Opinion|Analysis|Explainer|Editorial)\s*:\s*/i, '').trim();

  const saysEnding = theme.match(
    /^(.+?)\s+(?:says|said)\s+(?:it(?:'s| is)|they(?:'re| are)|he(?:'s| is)|she(?:'s| is))\s+ending\s+(.+)$/i
  );
  if (saysEnding) {
    theme = saysEnding[2] || theme;
  }

  const saysWillEnd = theme.match(/^(.+?)\s+(?:says|said)\s+(?:it|they|he|she)\s+will\s+end\s+(.+)$/i);
  if (saysWillEnd) {
    theme = saysWillEnd[2] || theme;
  }

  const action = theme.match(
    /^(.+?)\s+(Ends|Blocks|Bans|Approves|Cuts|Raises|Launches|Sues|Orders|Pauses|Resumes|Expands|Narrows|Rewrites)\s+(.+)$/i
  );
  if (action) {
    theme = action[3] || theme;
  }

  // If we still have an actor prefix, prefer the object clause.
  theme = theme.replace(/^.*?\bfrom\s+/i, '');
  theme = theme.replace(/^the\s+/i, '').trim();
  return theme;
}

function extractPlaceHint(text) {
  const raw = String(text || '');
  if (!raw) return '';
  const match = raw.match(/\b(?:in|across|near|outside|from)\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){0,2})\b/);
  if (!match) return '';
  let candidate = String(match[1] || '').replace(/\s+/g, ' ').trim();
  candidate = candidate.replace(/[.,;:]+$/g, '').trim();
  const blacklist = new Set([
    'The',
    'A',
    'An',
    'In',
    'On',
    'At',
    'Of',
    'And',
    'For',
    'With',
    'From',
    'After',
    'Before',
    'As',
    'More',
    'New'
  ]);
  const parts = candidate.split(' ').filter(Boolean);
  if (parts.length >= 2 && blacklist.has(parts[1])) {
    candidate = parts[0];
  }
  if (!candidate || blacklist.has(candidate)) return '';
  return candidate.length <= 40 ? candidate : '';
}

function ordinal(n) {
  const num = Number(n);
  if (!Number.isFinite(num)) return '';
  const mod10 = num % 10;
  const mod100 = num % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${num}th`;
  if (mod10 === 1) return `${num}st`;
  if (mod10 === 2) return `${num}nd`;
  if (mod10 === 3) return `${num}rd`;
  return `${num}th`;
}

function buildForecastBody(story) {
  const curation = story && story.curation && typeof story.curation === 'object' ? story.curation : null;

  // If Opus/Sonnet curated a full draft article, use it directly
  const draftBody = curation?.draftArticle?.body || curation?.draftBody || '';
  if (draftBody && draftBody.length > 120) {
    return draftBody;
  }

  // No draft available — return empty. The real article will be rendered on click
  // by calling the Anthropic API (Sonnet) with the curator directions.
  return '';
}

function buildSeedArticleFromStory(story) {
  const pack = story.evidencePack || {};
  const curation = story && story.curation && typeof story.curation === 'object' ? story.curation : null;

  const editionDate = pack.editionDate || '';
  const title = curation?.curatedTitle || curation?.draftArticle?.title || '';
  const dek = curation?.curatedDek || curation?.draftArticle?.dek || '';
  const meta = editionDate ? `${story.section} • ${editionDate}` : String(story.section || '').trim();
  const body = buildForecastBody(story);
  const confidence = Number(curation?.confidence) || 0;

  return {
    id: story.storyId,
    section: story.section,
    title,
    dek,
    meta,
    image: story.image || '',
    body,
    confidence,
    prompt: title ? `Editorial photo illustration prompt: ${title}. Documentary realism. Dated ${editionDate}.` : '',
    editionDate,
    generatedAt: new Date().toISOString(),
    curationGeneratedAt: story?.curation?.generatedAt || null,
    yearsForward: story.yearsForward
  };
}

async function runAnthropicRenderer(job, story, seedArticle) {
  const rendered = { ...seedArticle };
  const opusCfg = readOpusRuntimeConfig() || {};
  const apiKey = process.env.ANTHROPIC_API_KEY || opusCfg.apiKey || '';
  if (!apiKey) {
    throw new Error('ANTHROPIC_API_KEY not set — cannot render article');
  }

  const userPrompt = buildArticlePrompt(seedArticle, story);

  broadcastToJobSubscribers(job, { type: 'render.progress', phase: 'Generating article with Sonnet...', percent: 15 });

  const model = process.env.ARTICLE_MODEL || 'claude-sonnet-4-5-20250929';
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120000);

  try {
    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({
        model,
        max_tokens: 4096,
        temperature: 0.6,
        stream: true,
        messages: [{ role: 'user', content: userPrompt }]
      }),
      signal: controller.signal
    });

    if (!resp.ok) {
      const errText = await resp.text().catch(() => '');
      throw new Error(`Anthropic API ${resp.status}: ${errText.slice(0, 200)}`);
    }

    broadcastToJobSubscribers(job, { type: 'render.progress', phase: 'Writing article...', percent: 30 });

    // Stream the response
    let fullBody = '';
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      // Parse SSE events
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        const data = line.slice(6).trim();
        if (data === '[DONE]') continue;
        try {
          const evt = JSON.parse(data);
          if (evt.type === 'content_block_delta' && evt.delta?.type === 'text_delta') {
            const text = evt.delta.text || '';
            fullBody += text;
            broadcastToJobSubscribers(job, { type: 'render.chunk', delta: text });
          }
        } catch { /* skip non-JSON SSE lines */ }
      }

      // Update progress based on length
      const pct = Math.min(90, 30 + Math.floor((fullBody.length / 3000) * 60));
      broadcastToJobSubscribers(job, { type: 'render.progress', phase: 'Writing article...', percent: pct });
    }

    rendered.body = fullBody.trim() || rendered.body;
    broadcastToJobSubscribers(job, { type: 'render.progress', phase: 'Article complete', percent: 100 });
  } finally {
    clearTimeout(timeout);
  }

  job.complete = true;
  job.status = 'complete';
  job.result = rendered;
  cacheByKey.set(job.key, rendered);
  pipeline.storeRendered(job.storyId, rendered, { curationGeneratedAt: job.curationGeneratedAt });

  broadcastToJobSubscribers(job, { type: 'render.complete', article: rendered });
  finalizeJobCleanup(job);
}

function parseProviderFrame(raw) {
  const line = String(raw || '').trim();
  if (!line) return [];
  const normalized = [];

  for (const chunk of line.split('\n')) {
    const trimmed = chunk.trim();
    if (!trimmed) continue;
    if (trimmed.startsWith('data:')) {
      const data = trimmed.replace(/^data:\s*/, '');
      if (data === '[DONE]') continue;
      try {
        normalized.push(JSON.parse(data));
        continue;
      } catch {
        normalized.push({ type: 'render.chunk', delta: `${data}\n` });
        continue;
      }
    }
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
      try {
        normalized.push(JSON.parse(trimmed));
        continue;
      } catch {
        normalized.push({ type: 'render.chunk', delta: `${trimmed}\n` });
        continue;
      }
    }
    normalized.push({ type: 'render.chunk', delta: `${trimmed}\n` });
  }

  return normalized;
}

function normalizeProviderEvent(rawEvent) {
  const event = typeof rawEvent === 'object' && rawEvent !== null ? rawEvent : {};
  const progressType = event.type === 'progress' || event.type === 'render.progress' || event.type === 'status';
  const chunkType = event.type === 'chunk' || event.type === 'render.chunk' || event.type === 'token' || event.type === 'content';
  const completeType = event.type === 'complete' || event.type === 'render.complete' || event.type === 'done' || event.type === 'finished';
  const errorType = event.type === 'error' || event.type === 'render.error' || event.error || event.err;

  if (errorType) {
    const errorText = typeof event.error === 'string' ? event.error : event.message || event.err || 'Render failed';
    return { type: 'render.error', error: errorText };
  }

  if (progressType) {
    const percent = Number(event.percent || event.pct || event.progress || event.complete);
    return {
      type: 'render.progress',
      percent: Number.isFinite(percent) ? percent : undefined,
      phase: event.phase || event.stage || event.label || event.message
    };
  }

  if (chunkType) {
    const delta = event.delta || event.content || event.text || (event.choices && event.choices[0]?.delta?.content) || '';
    if (delta) return { type: 'render.chunk', delta: String(delta) };
  }

  if (event.choices && event.choices[0]?.text) {
    return { type: 'render.chunk', delta: String(event.choices[0].text) };
  }

  if (event.article || event.payload || event.result || event.output || event.data) {
    return { type: 'render.article', article: event.article || event.payload || event.result || event.output || event.data };
  }

  if (event.body) {
    return { type: 'render.article', article: { ...event, body: String(event.body) } };
  }

  if (completeType) {
    return { type: 'render.complete', article: event.article || event.result || event.output || event.data || { body: '' } };
  }

  return {};
}

function buildArticlePrompt(seedArticle, story) {
  const editionDate = story.evidencePack?.editionDate || seedArticle.editionDate;
  const baselineDay = normalizeDay(story.day) || formatDay();
  const baselineYear = baselineDay.slice(0, 4) || '2026';
  const yearsForward = story.yearsForward;
  const targetYear = Number(baselineYear) + (Number.isFinite(Number(yearsForward)) ? Number(yearsForward) : 0);
  const timeAnchor = editionDate ? `${editionDate} (${targetYear || 'future'})` : `${targetYear || 'future'}`;

  const curation = story && story.curation && typeof story.curation === 'object' ? story.curation : null;
  const topicTitle = String(curation?.topicTitle || curation?.topicSeed || '').trim();
  const directions = String(curation?.sparkDirections || '').trim();
  const eventSeed = String(curation?.futureEventSeed || '').trim();

  return [
    `You are writing an article for The Future Times, published on ${timeAnchor}.`,
    `Write it as a REAL news report from ${targetYear}. You are a journalist in ${targetYear} reporting on events happening NOW.`,
    ``,
    `CRITICAL RULES:`,
    `- Write about what is HAPPENING in ${targetYear}, not about what happened in ${baselineYear}.`,
    `- Do NOT frame as "anniversary" or "looking back" pieces.`,
    `- The baseline citations from ${baselineYear} are BACKGROUND CONTEXT only.`,
    `- Write PREDICTIONS and PLAUSIBLE OUTCOMES: policies, technologies, markets, specific numbers.`,
    `- Write as a journalist who has never heard of the ${baselineYear} article.`,
    `- Do NOT reference or link to the original ${baselineYear} news stories in your article.`,
    ``,
    `Do not describe the article as a projection, simulation, or prompt output.`,
    `Output 4-8 narrative paragraphs (NYT-style prose, no markdown headers, no bullet lists).`,
    `Do NOT include a Sources section — sources are handled separately.`,
    ``,
    topicTitle ? `Topic: ${topicTitle}` : '',
    directions ? `Curator directions: ${directions}` : '',
    eventSeed ? `Future event seed: ${eventSeed}` : '',
    ``,
    `Headline: ${seedArticle.title}`,
    seedArticle.dek ? `Dek: ${seedArticle.dek}` : '',
    ``,
    `Write the full article body now.`
  ].filter(Boolean).join('\n');
}

async function renderArticleContent(job, story, seedArticle) {
  // If there's already a full body (from Opus draftArticle), just finalize
  if (seedArticle.body && seedArticle.body.trim().length > 200) {
    job.complete = true;
    job.status = 'complete';
    job.result = seedArticle;
    cacheByKey.set(job.key, seedArticle);
    pipeline.storeRendered(job.storyId, seedArticle, { curationGeneratedAt: job.curationGeneratedAt });
    broadcastToJobSubscribers(job, { type: 'render.complete', article: seedArticle });
    finalizeJobCleanup(job);
    return;
  }

  // Use Anthropic Sonnet API to generate the article
  await runAnthropicRenderer(job, story, seedArticle);
}

async function runRenderJob(job) {
  let story = job.story || pipeline.getStory(job.storyId);
  if (!story) {
    // If the edition isn't built yet but the link is shareable, build for the day embedded in the story id.
    const match = String(job.storyId || '').match(/^ft-(\d{4}-\d{2}-\d{2})-y(\d+)/);
    if (match) {
      const day = match[1];
      await pipeline.ensureDayBuilt(day);
      story = pipeline.getStory(job.storyId);
    }
  }

  if (!story) {
    throw new Error(`Unknown story: ${job.storyId}`);
  }

  const seedArticle = buildSeedArticleFromStory(story);
  await renderArticleContent(job, story, seedArticle);
}

function getActiveJob(key) {
  return jobStore.get(key) || null;
}

function getArticleStatus(storyId, story = null) {
  const key = keyFor(storyId, story);
  const mem = cacheByKey.get(key);
  if (mem) return { status: 'ready', article: mem };
  const dbCached = pipeline.getRenderedVariant(storyId, { curationGeneratedAt: story?.curation?.generatedAt || '' });
  if (dbCached) {
    cacheByKey.set(key, dbCached);
    return { status: 'ready', article: dbCached };
  }
  const job = getActiveJob(key);
  if (job) {
    return { status: job.status === 'complete' ? 'ready' : 'streaming', article: job.result, startedAt: job.startedAt };
  }
  return { status: 'queued' };
}

function startRenderJob(storyId, story = null) {
  const resolvedStory = story || pipeline.getStory(storyId);
  const key = keyFor(storyId, resolvedStory);
  const cached =
    cacheByKey.get(key) || pipeline.getRenderedVariant(storyId, { curationGeneratedAt: resolvedStory?.curation?.generatedAt || '' });
  if (cached) {
    cacheByKey.set(key, cached);
    return { status: 'cached', key, article: cached, job: null };
  }

  const existing = getActiveJob(key);
  if (existing) {
    return { status: 'running', key, article: null, job: existing };
  }

  if (!resolvedStory) {
    return { status: 'not_found', key, article: null, job: null };
  }

  const job = {
    key,
    storyId,
    story: resolvedStory,
    curationGeneratedAt: resolvedStory?.curation?.generatedAt || null,
    status: 'running',
    subscribers: new Set(),
    startedAt: Date.now(),
    complete: false,
    result: null,
    error: null
  };

  jobStore.set(key, job);
  setImmediate(() => {
    runRenderJob(job).catch((err) => {
      job.status = 'error';
      job.error = err?.message || 'Render failed';
      broadcastToJobSubscribers(job, { type: 'render.error', error: job.error });
      finalizeJobCleanup(job);
    });
  });

  return { status: 'started', key, article: null, job };
}

function normalizePath(urlPath) {
  let decoded;
  try {
    decoded = decodeURIComponent(urlPath);
  } catch {
    return '/index.html';
  }
  if (!decoded.startsWith('/')) return '/index.html';
  if (decoded === '/') return '/index.html';
  if (decoded.startsWith('/api/')) return '/api';
  return decoded;
}

function ensureSafeFilePath(requestedPath) {
  const normalized = path.normalize(requestedPath);
  const fsPath = path.resolve(ROOT_DIR, '.' + normalized);
  if (!fsPath.startsWith(ROOT_DIR + path.sep)) return null;
  return fsPath;
}

function serveFile(res, fsPath) {
  fs.readFile(fsPath, (err, data) => {
    if (err) {
      if (err.code === 'ENOENT' || err.code === 'EISDIR') {
        sendNotFound(res);
      } else {
        send500(res, err);
      }
      return;
    }
    const ext = path.extname(fsPath).toLowerCase();
    const contentType = mimeByExt.get(ext) || 'application/octet-stream';
    const cache = ext === '.html' || ext === '.js' || ext === '.css' ? 'no-store' : 'public, max-age=3600';
    res.writeHead(200, { 'content-type': contentType, 'cache-control': cache });
    res.end(data);
  });
}

function sendJson(res, payload, status = 200) {
  const json = JSON.stringify(payload);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(json);
}

function sendJsonPretty(res, payload, status = 200) {
  const json = JSON.stringify(payload, null, 2);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
  res.end(json);
}

function sendHtml(res, html, status = 200) {
  res.writeHead(status, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
  res.end(html);
}

function sendNotFound(res, message = 'Not found') {
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end(message);
}

function send405(res, allowed) {
  res.writeHead(405, { allow: allowed });
  res.end('Method not allowed');
}

function send500(res, err) {
  res.writeHead(500, { 'content-type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ error: err?.message || 'Server error' }));
}

async function readRequestBody(req, limitBytes = 1024 * 1024) {
  const limit = Math.max(1024, Math.min(5 * 1024 * 1024, Number(limitBytes) || 1024 * 1024));
  const chunks = [];
  let total = 0;
  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    total += buf.length;
    if (total > limit) {
      throw new Error('request_body_too_large');
    }
    chunks.push(buf);
  }
  return Buffer.concat(chunks).toString('utf8');
}

async function readJsonBody(req) {
  const raw = await readRequestBody(req, 1024 * 1024);
  if (!raw.trim()) return null;
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error('invalid_json_body');
  }
}

function parseRequest(reqUrl) {
  const url = new URL(reqUrl, `http://${DEFAULT_HOST}:${activePort || PORT_START}`);
  const pathname = url.pathname.replace(/\/+$/, '') || '/';
  const years = clampYears(url.searchParams.get('years'));
  const day = normalizeDay(url.searchParams.get('day'));
  return { url, pathname, years, day };
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function clientAcceptsHtml(req) {
  const accept = String(req?.headers?.accept || '');
  if (!accept) return false;
  return accept.includes('text/html') && !accept.includes('application/json');
}

function hostFromUrl(url) {
  try {
    return new URL(String(url || '')).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function parseStoryIdFromPath(pathname) {
  const raw = String(pathname || '').replace(/^\/api\/article\//, '');
  const segment = raw.split('?')[0].split('/')[0];
  try {
    return decodeURIComponent(segment);
  } catch {
    return '';
  }
}

function renderDaySignalHtml(snapshot) {
  const day = snapshot?.day || '';
  const generatedAt = snapshot?.generatedAt || '';
  const counts = snapshot?.counts || {};

  const topics = Array.isArray(snapshot?.topics) ? snapshot.topics : [];
  const signals = Array.isArray(snapshot?.signals) ? snapshot.signals : [];
  const rawItems = Array.isArray(snapshot?.rawItems) ? snapshot.rawItems : [];
  const sources = Array.isArray(snapshot?.sources) ? snapshot.sources : [];
  const editions = Array.isArray(snapshot?.editions) ? snapshot.editions : [];

  const topicsBySection = new Map();
  for (const t of topics) {
    const section = String(t.section || 'Other');
    if (!topicsBySection.has(section)) topicsBySection.set(section, []);
    topicsBySection.get(section).push(t);
  }

  const signalsBySection = new Map();
  for (const s of signals) {
    const section = String(s.section || 'Other');
    if (!signalsBySection.has(section)) signalsBySection.set(section, []);
    signalsBySection.get(section).push(s);
  }

  const preferredSections = ['U.S.', 'World', 'Business', 'Technology', 'AI', 'Arts', 'Lifestyle', 'Opinion'];
  const sectionSet = new Set([...preferredSections, ...topicsBySection.keys(), ...signalsBySection.keys()]);
  const sections = [
    ...preferredSections.filter((s) => sectionSet.has(s)),
    ...Array.from(sectionSet).filter((s) => !preferredSections.includes(s))
  ].filter(Boolean);

  const typeCounts = new Map();
  for (const s of signals) {
    const type = String(s.type || 'unknown');
    typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
  }
  const typeCountLine = Array.from(typeCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([type, n]) => `${type}: ${n}`)
    .join(' • ');

  const sectionId = (section) => `sec-${String(section || 'other').toLowerCase().replace(/[^a-z0-9]+/g, '-')}`;

  const renderBriefBullets = (brief) => {
    const lines = String(brief || '')
      .split('\n')
      .map((l) => l.replace(/^\s*-\s+/, '').trim())
      .filter(Boolean)
      .slice(0, 6);
    if (!lines.length) return '<div class="muted small">No brief available.</div>';
    return `<ul class="bullets">${lines.map((l) => `<li>${escapeHtml(l)}</li>`).join('')}</ul>`;
  };

  const renderEvidenceLinks = (links) => {
    const list = Array.isArray(links) ? links : [];
    if (!list.length) return '<div class="muted small">No evidence links.</div>';
    return `<ul class="links">${list
      .slice(0, 8)
      .map((e) => {
        const url = String(e.url || '');
        const host = hostFromUrl(url);
        return `<li><a href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(e.title || url || 'Source')}</a>${host ? ` <span class="muted">(${escapeHtml(host)})</span>` : ''}</li>`;
      })
      .join('')}</ul>`;
  };

  const renderSignalList = (rows, max = 14) => {
    const items = Array.isArray(rows) ? rows.slice(0, max) : [];
    if (!items.length) return '<div class="muted small">No signals.</div>';
    return `<ol class="signalList">${items
      .map((s) => {
        const url = String(s.url || '');
        const host = hostFromUrl(url);
        const title = s.type === 'market' ? String(s.title || '').replace(/\?+$/g, '') : String(s.title || '');
        const summary = String(s.summary || '').replace(/\s+/g, ' ').trim();
        const published = s.publishedAt || '';
        return `<li>
          <div class="row">
            <span class="badge">${escapeHtml(s.section || '')}</span>
            <span class="badge"><code>${escapeHtml(s.type || '')}</code></span>
            ${published ? `<span class="muted small">${escapeHtml(published)}</span>` : ''}
          </div>
          <div class="tight">
            ${url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(title || 'Signal')}</a>` : escapeHtml(title || 'Signal')}
            ${host ? ` <span class="muted small">(${escapeHtml(host)})</span>` : ''}
          </div>
          ${summary ? `<div class="muted small">${escapeHtml(summary.slice(0, 220))}</div>` : ''}
        </li>`;
      })
      .join('')}</ol>`;
  };

  const renderSignalTable = (rows, maxRows = 120) => {
    const items = Array.isArray(rows) ? rows.slice(0, maxRows) : [];
    const remainder = Array.isArray(rows) && rows.length > items.length ? rows.length - items.length : 0;
    return `
      ${remainder ? `<div class="muted small" style="margin-bottom:10px">Showing top ${items.length}. ${remainder} more available in JSON.</div>` : ''}
      <table>
        <thead>
          <tr>
            <th>Type</th>
            <th>Title</th>
            <th>Summary</th>
            <th>Published</th>
            <th>Score</th>
          </tr>
        </thead>
        <tbody>
          ${items
            .map((s) => {
              const url = String(s.url || '');
              const host = hostFromUrl(url);
              const title = s.type === 'market' ? String(s.title || '').replace(/\?+$/g, '') : String(s.title || '');
              const score = s.score == null ? '' : Number(s.score).toFixed(3);
              return `
                <tr>
                  <td><code>${escapeHtml(s.type || '')}</code></td>
                  <td>
                    ${url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(title || 'Signal')}</a>` : escapeHtml(title || 'Signal')}
                    ${host ? `<div class="muted small">${escapeHtml(host)}</div>` : ''}
                  </td>
                  <td class="muted">${escapeHtml(String(s.summary || '').slice(0, 320))}</td>
                  <td class="muted small">${escapeHtml(s.publishedAt || '')}</td>
                  <td class="muted small">${escapeHtml(score)}</td>
                </tr>
              `;
            })
            .join('')}
        </tbody>
      </table>
    `;
  };

  const topSignals = signals.slice(0, 16);
  const marketSignals = signals.filter((s) => s.type === 'market').slice(0, 10);
  const econSignals = signals.filter((s) => s.type === 'econ').slice(0, 8);

  const sourcesWithError = sources.filter((s) => s.last_error).length;
  const sourcesOk = Math.max(0, sources.length - sourcesWithError);

  const jsonHref = `/api/day-signal?day=${encodeURIComponent(day)}&format=json`;
  const prettyHref = `/api/day-signal?day=${encodeURIComponent(day)}&format=pretty`;
  const backHref = day ? `/index.html?day=${encodeURIComponent(day)}` : '/';
  const polymarketHref = `/api/admin/polymarket?day=${encodeURIComponent(day)}&format=html`;
  const adminHref = `/api/admin/curation?day=${encodeURIComponent(day)}&format=html`;

  const rawPreviewMax = 160;
  const rawPreview = rawItems.slice(0, rawPreviewMax);
  const rawRemainder = rawItems.length > rawPreview.length ? rawItems.length - rawPreview.length : 0;

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Future Times Signal Pack - ${escapeHtml(day)}</title>
  <style>
    :root{
      --bg:#ffffff;
      --card:#ffffff;
      --fg:#111111;
      --muted:#555555;
      --border:#e5e5e5;
      --link:#0b4f8a;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      --serif: ui-serif, Georgia, Cambria, "Times New Roman", Times, serif;
    }
    html,body{height:100%;}
    body{
      margin:0;
      background:var(--bg);
      color:var(--fg);
      font: 16px/1.55 var(--serif);
    }
    header{
      background:var(--card);
      border-top:6px solid #111;
      border-bottom:1px solid #111;
      padding:18px 18px 14px;
    }
    main{max-width:1240px;margin:0 auto;padding:18px;}
    h1{margin:0 0 6px;font-size:28px;letter-spacing:.06em;text-transform:uppercase;}
    h2{margin:26px 0 10px;font-size:20px;}
    h3{margin:18px 0 10px;font-size:16px;font-family:var(--sans);letter-spacing:.01em;}
    h4{margin:10px 0 6px;font-size:15px;font-family:var(--sans);}
    a{color:var(--link);text-decoration:none;}
    a:hover{text-decoration:underline;}
    .muted{color:var(--muted);}
    .small{font-size:12px;font-family:var(--sans);}
    .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;}
    .badge{
      display:inline-block;
      font-family:var(--mono);
      font-size:12px;
      border:1px solid var(--border);
      background:#f3f3f3;
      padding:2px 8px;
      border-radius:999px;
    }
    .card{
      background:var(--card);
      border:1px solid var(--border);
      border-radius:0;
      padding:12px 12px 10px;
      margin:10px 0;
      overflow:hidden;
    }
    .tight{margin-top:8px;}
    .kpi{margin-top:6px;}
    .kpi strong{font-family:var(--mono);}
    .divider{height:1px;background:var(--border);margin:14px 0 0;}
    .cards{
      display:grid;
      grid-template-columns: 1fr;
      gap: 14px;
      margin-top: 14px;
    }
    @media (min-width: 980px){
      .cards{grid-template-columns: 1.2fr 1fr 1fr;}
    }
    nav.toc{
      margin-top: 12px;
      font-family: var(--sans);
      font-size: 13px;
      display:flex;
      flex-wrap:wrap;
      gap:10px;
    }
    nav.toc a{
      border:1px solid var(--border);
      background:#fbfbfb;
      padding:6px 10px;
      border-radius:999px;
      color:inherit;
    }
    .sectionGrid{
      display:grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    @media (min-width: 980px){
      .sectionGrid{grid-template-columns: 1.15fr 0.85fr;}
    }
    details{margin-top:10px;}
    summary{cursor:pointer;font-weight:700;font-family:var(--sans);}
    ul.links{margin:8px 0 0 18px;padding:0;font: 14px/1.4 var(--sans);}
    ul.bullets{margin:10px 0 0 18px;padding:0;font: 14px/1.4 var(--sans);}
    ol.signalList{margin:10px 0 0 18px;padding:0;font: 14px/1.4 var(--sans);}
    ol.signalList li{margin-bottom:10px;}
    table{width:100%;border-collapse:collapse;font: 13px/1.35 var(--sans);}
    th,td{padding:8px 8px;border-top:1px solid var(--border);vertical-align:top;text-align:left;}
    th{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);background:#fcfcfc;}
    code{font-family:var(--mono);font-size:12px;}
    .section-ai{border-left:4px solid #6f42c1;background:linear-gradient(135deg,#faf5ff 0%,#fff 100%);}
    .section-ai h2{color:#6f42c1;}
    .section-header{display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:8px;}
    .section-header .section-count{font-family:var(--sans);font-size:12px;color:var(--muted);}
    .section-divider{height:2px;background:linear-gradient(to right,var(--border),transparent);margin:0 0 8px;}
  </style>
</head>
<body>
  <header>
    <div class="row" style="justify-content:space-between">
      <div>
        <h1>Future Times Signal Pack</h1>
        <div class="muted">Day <strong>${escapeHtml(day)}</strong> &mdash; Generated <span class="small">${escapeHtml(generatedAt)}</span></div>
      </div>
      <div class="row">
        <button class="badge" id="runDailyBtn" style="cursor:pointer;background:#e8f5e9;border-color:#4caf50;color:#2e7d32;font-weight:600" title="Fetch sources, process signals, cluster topics, build editions">&#9654; Run Daily</button>
        <button class="badge" id="forceRebuildBtn" style="cursor:pointer;background:#fff3e0;border-color:#ff9800;color:#e65100;font-weight:600" title="Force re-fetch + re-curate even if already done today">&#x21bb; Force Rebuild</button>
        <span id="runStatus" class="muted small"></span>
      </div>
      <div class="row" style="margin-top:6px">
        <a class="badge" href="${escapeHtml(backHref)}">Front page</a>
        <a class="badge" href="${escapeHtml(adminHref)}">Admin</a>
        <a class="badge" href="${escapeHtml(polymarketHref)}" style="border-color:#6f42c1;color:#6f42c1">Polymarket Outcomes</a>
        <a class="badge" href="${escapeHtml(jsonHref)}">JSON</a>
        <a class="badge" href="${escapeHtml(prettyHref)}">Pretty JSON</a>
      </div>
    </div>

    <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:12px">
      <div class="badge" style="background:#f0f4f8;border-color:#cdd9e5"><strong>${escapeHtml(counts.rawItems ?? rawItems.length)}</strong> raw items</div>
      <div class="badge" style="background:#f0f4f8;border-color:#cdd9e5"><strong>${escapeHtml(counts.signals ?? signals.length)}</strong> signals</div>
      <div class="badge" style="background:#f0f4f8;border-color:#cdd9e5"><strong>${escapeHtml(counts.topics ?? topics.length)}</strong> topics</div>
      <div class="badge" style="background:#f0f4f8;border-color:#cdd9e5"><strong>${escapeHtml(counts.editions ?? editions.length)}</strong> editions</div>
      ${marketSignals.length ? `<div class="badge" style="background:#fff8f0;border-color:#d4a373"><strong>${escapeHtml(marketSignals.length)}</strong> market signals</div>` : ''}
      ${econSignals.length ? `<div class="badge" style="background:#f0fff4;border-color:#8fbc8f"><strong>${escapeHtml(econSignals.length)}</strong> econ indicators</div>` : ''}
      ${typeCountLine ? `<div class="muted small" style="align-self:center">${escapeHtml(typeCountLine)}</div>` : ''}
    </div>

    <div class="divider"></div>

    <nav class="toc" aria-label="Jump to section">
      <a href="#at-a-glance" style="font-weight:600">Overview</a>
      ${sections.map((s) => {
        const aiStyle = s === 'AI' ? ' style="border-color:#6f42c1;color:#6f42c1;font-weight:600"' : '';
        return `<a href="#${escapeHtml(sectionId(s))}"${aiStyle}>${escapeHtml(s)}</a>`;
      }).join('')}
      <a href="#sources">Sources</a>
      <a href="#editions">Editions</a>
      <a href="#raw-items">Raw items</a>
    </nav>
  </header>

  <main>
    <section id="at-a-glance">
      <h2>At a glance</h2>
      <div class="cards">
        <div class="card">
          <h3>Key topics</h3>
          <div class="muted small">Top 2 topics per section (clustered from signals).</div>
          <ul class="bullets">
            ${sections
              .map((section) => {
                const list = (topicsBySection.get(section) || []).slice(0, 2);
                if (!list.length) return `<li><strong>${escapeHtml(section)}:</strong> <span class="muted">no topics</span></li>`;
                return `<li><strong>${escapeHtml(section)}:</strong> ${list.map((t) => escapeHtml(String(t.label || '').slice(0, 110))).join(' • ')}</li>`;
              })
              .join('')}
          </ul>
        </div>
        <div class="card">
          <h3>Top signals</h3>
          <div class="muted small">Highest-scoring inputs across all sources.</div>
          ${renderSignalList(topSignals, topSignals.length)}
        </div>
        <div class="card">
          <h3>Prediction Markets &amp; Indicators</h3>
          <div class="muted small">Market odds and macro data used as forecasting inputs. <a href="${escapeHtml(polymarketHref)}" style="color:#6f42c1;font-weight:600">View full Polymarket outcomes &rarr;</a></div>
          ${marketSignals.length ? `
          <h4 class="tight" style="margin-top:12px">Top Polymarket questions</h4>
          <ul class="bullets">${marketSignals
            .map((s) => `<li>${escapeHtml(String(s.title || '').replace(/\?+$/g, ''))} <span class="muted">(${escapeHtml(String(s.summary || '').slice(0, 50))})</span></li>`)
            .join('')}</ul>` : '<div class="muted small">No market signals.</div>'}
          ${econSignals.length ? `
          <h4 class="tight" style="margin-top:12px">Econ indicators</h4>
          <ul class="bullets">${econSignals
            .map((s) => `<li><strong>${escapeHtml(s.title || '')}</strong> <span class="muted">${escapeHtml(String(s.summary || '').slice(0, 90))}</span></li>`)
            .join('')}</ul>` : ''}
          <div class="row" style="margin-top:12px">
            <span class="badge" style="background:#e8f5e9">sources ok ${escapeHtml(sourcesOk)}</span>
            ${sourcesWithError ? `<span class="badge" style="background:#fce4ec">sources error ${escapeHtml(sourcesWithError)}</span>` : ''}
          </div>
        </div>
      </div>
    </section>

    ${sections
      .map((section) => {
        const sectionTopics = topicsBySection.get(section) || [];
        const sectionSignals = signalsBySection.get(section) || [];
        const isAI = section === 'AI';
        const sectionClass = isAI ? ' section-ai' : '';
        return `
          <section id="${escapeHtml(sectionId(section))}" class="card${sectionClass}" style="margin-top:20px;padding:16px;">
            <div class="section-header">
              <h2 style="margin:0">${escapeHtml(section)}</h2>
              <span class="section-count">${sectionTopics.length} topics &middot; ${sectionSignals.length} signals</span>
            </div>
            <div class="section-divider"></div>
            <div class="sectionGrid">
              <div>
                <h3>Topics</h3>
                ${sectionTopics.length
                  ? sectionTopics
                      .map((t, idx) => {
                        const score = t.score == null ? '' : Number(t.score).toFixed(3);
                        const links = Array.isArray(t.evidenceLinks) ? t.evidenceLinks : [];
                        const openAttr = idx < 2 ? ' open' : '';
                        return `
                          <details class="card"${openAttr}>
                            <summary>${escapeHtml(t.label || 'Topic')}${score ? ` <span class="muted small">(score ${escapeHtml(score)})</span>` : ''}</summary>
                            ${renderBriefBullets(t.brief)}
                            <div class="muted small" style="margin-top:10px">Evidence links (${links.length})</div>
                            ${renderEvidenceLinks(links)}
                          </details>
                        `;
                      })
                      .join('')
                  : `<div class="muted small">No topics for this section.</div>`}
              </div>
              <div>
                <h3>Top signals</h3>
                <div class="muted small">Top ${Math.min(14, sectionSignals.length)} signals by score for this section.</div>
                ${renderSignalList(sectionSignals, 14)}
                <details class="card">
                  <summary>All signals (${sectionSignals.length})</summary>
                  <div style="margin-top:10px">${renderSignalTable(sectionSignals, 120)}</div>
                </details>
              </div>
            </div>
          </section>
        `;
      })
      .join('')}

    <section id="sources">
      <h2>Sources</h2>
      <div class="muted small">Fetch health and last error status for each configured source.</div>
      <div class="card">
        <table>
          <thead>
            <tr>
              <th>Source</th>
              <th>Type</th>
              <th>Section</th>
              <th>Last fetched</th>
              <th>Status</th>
              <th>Items</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody>
            ${sources
              .map((s) => {
                const sourceUrl = String(s.url || '');
                return `
                  <tr>
                    <td>
                      <div><code>${escapeHtml(s.source_id || '')}</code></div>
                      <div>${sourceUrl ? `<a href="${escapeHtml(sourceUrl)}" target="_blank" rel="noopener">${escapeHtml(s.name || '')}</a>` : escapeHtml(s.name || '')}</div>
                    </td>
                    <td class="muted">${escapeHtml(s.type || '')}</td>
                    <td class="muted">${escapeHtml(s.section || '')}</td>
                    <td class="muted small">${escapeHtml(s.last_fetched_at || '')}</td>
                    <td class="muted small">${escapeHtml(s.last_status ?? '')}</td>
                    <td class="muted small">${escapeHtml(s.last_item_count ?? '')}</td>
                    <td class="muted small">${escapeHtml(s.last_error || '')}</td>
                  </tr>
                `;
              })
              .join('')}
          </tbody>
        </table>
      </div>
    </section>

    <section id="editions">
      <h2>Editions built</h2>
      <div class="card">
        <table>
          <thead>
            <tr>
              <th>Years forward</th>
              <th>Version</th>
              <th>Generated</th>
            </tr>
          </thead>
          <tbody>
            ${editions
              .map((e) => {
                return `
                  <tr>
                    <td><code>+${escapeHtml(e.years_forward ?? '')}</code></td>
                    <td class="muted"><code>${escapeHtml(e.version || '')}</code></td>
                    <td class="muted small">${escapeHtml(e.generated_at || '')}</td>
                  </tr>
                `;
              })
              .join('')}
          </tbody>
        </table>
      </div>
    </section>

    <section id="raw-items">
      <h2>Raw items (ingested)</h2>
      <div class="muted small">Showing ${rawPreview.length}${rawRemainder ? ` of ${rawItems.length} (use JSON for the full list)` : ''}.</div>
      <details class="card">
        <summary>Show raw item preview</summary>
        <div style="margin-top:10px">
          <table>
            <thead>
              <tr>
                <th>Source</th>
                <th>Title</th>
                <th>Section hint</th>
                <th>Published</th>
              </tr>
            </thead>
            <tbody>
              ${rawPreview
                .map((r) => {
                  const url = String(r.url || '');
                  const host = hostFromUrl(url);
                  return `
                    <tr>
                      <td class="muted"><code>${escapeHtml(r.sourceId || '')}</code></td>
                      <td>
                        ${url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(r.title || '')}</a>` : escapeHtml(r.title || '')}
                        ${host ? `<div class="muted small">${escapeHtml(host)}</div>` : ''}
                      </td>
                      <td class="muted">${escapeHtml(r.sectionHint || '')}</td>
                      <td class="muted small">${escapeHtml(r.publishedAt || '')}</td>
                    </tr>
                  `;
                })
                .join('')}
            </tbody>
          </table>
        </div>
      </details>
    </section>
  </main>
  <script>
  (function(){
    const statusEl = document.getElementById('runStatus');
    const runBtn = document.getElementById('runDailyBtn');
    const forceBtn = document.getElementById('forceRebuildBtn');
    const day = ${JSON.stringify(day)};

    async function runDaily(force) {
      const label = force ? 'Force rebuilding' : 'Running daily pipeline';
      statusEl.textContent = label + '...';
      runBtn.disabled = true;
      forceBtn.disabled = true;
      try {
        const qs = force ? '?forceRefresh=true&forceCuration=true' : '';
        const resp = await fetch('/api/admin/daily' + qs + (qs ? '&' : '?') + 'day=' + encodeURIComponent(day), { method: 'POST' });
        const data = await resp.json();
        if (data && data.ok) {
          statusEl.textContent = 'Done! Reloading...';
          statusEl.style.color = '#2e7d32';
          setTimeout(() => location.reload(), 1200);
        } else {
          statusEl.textContent = 'Error: ' + (data.error || 'unknown');
          statusEl.style.color = '#c62828';
        }
      } catch(e) {
        statusEl.textContent = 'Network error: ' + e.message;
        statusEl.style.color = '#c62828';
      } finally {
        runBtn.disabled = false;
        forceBtn.disabled = false;
      }
    }

    runBtn.addEventListener('click', () => runDaily(false));
    forceBtn.addEventListener('click', () => {
      if (confirm('Force re-fetch all sources and re-run curation? This may take a few minutes.')) {
        runDaily(true);
      }
    });
  })();
  </script>
</body>
</html>`;
}

function renderAdminTraceHtml(day, events) {
  const list = Array.isArray(events) ? events : [];
  const backHref = day ? `/api/admin/curation?day=${encodeURIComponent(day)}&format=html` : '/api/admin/curation?format=html';
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin Trace - ${escapeHtml(day || '')}</title>
  <style>
    :root{--bg:#ffffff;--fg:#111;--muted:#555;--border:#e5e5e5;--link:#0b4f8a;--mono: ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;--sans: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;}
    body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 var(--sans);}
    header{border-top:6px solid #111;border-bottom:1px solid #111;padding:16px 18px;}
    main{max-width:1240px;margin:0 auto;padding:18px;}
    h1{margin:0 0 6px;font-size:20px;letter-spacing:.02em;}
    .muted{color:var(--muted);}
    a{color:var(--link);text-decoration:none;}
    a:hover{text-decoration:underline;}
    .badge{display:inline-block;font-family:var(--mono);font-size:12px;border:1px solid var(--border);background:#f3f3f3;padding:2px 8px;border-radius:999px;}
    table{width:100%;border-collapse:collapse;margin-top:12px;}
    th,td{padding:8px 8px;border-top:1px solid var(--border);vertical-align:top;text-align:left;}
    th{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);background:#fcfcfc;}
    pre{margin:0;font-family:var(--mono);font-size:12px;white-space:pre-wrap;}
  </style>
</head>
<body>
  <header>
    <div style="display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;align-items:center">
      <div>
        <h1>Admin Trace</h1>
        <div class="muted">Day <strong>${escapeHtml(day || '')}</strong> - ${escapeHtml(list.length)} events</div>
      </div>
      <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap">
        <a class="badge" href="${escapeHtml(backHref)}">Back</a>
        <a class="badge" href="/api/admin/trace?day=${escapeHtml(encodeURIComponent(day || ''))}&format=json">JSON</a>
      </div>
    </div>
  </header>
  <main>
    <table>
      <thead>
        <tr>
          <th>Time (ISO)</th>
          <th>Event</th>
          <th>Payload</th>
        </tr>
      </thead>
      <tbody>
        ${list
          .map((e) => {
            const payload = e && e.payload ? JSON.stringify(e.payload, null, 2) : '';
            return `
              <tr>
                <td class="muted"><code>${escapeHtml(String(e.ts || ''))}</code></td>
                <td><code>${escapeHtml(String(e.type || ''))}</code></td>
                <td>${payload ? `<pre>${escapeHtml(payload)}</pre>` : `<span class="muted">—</span>`}</td>
              </tr>
            `;
          })
          .join('')}
      </tbody>
    </table>
  </main>
</body>
</html>`;
}

function renderAdminCurationHtml({ day, yearsForward, snapshot, trace, dayCuration, storyCurations }) {
  const sectionsOrder = ['U.S.', 'World', 'Business', 'Technology', 'Arts', 'Lifestyle', 'Opinion'];
  const yearsList = Array.from({ length: 11 }, (_, i) => i);
  const selectedYear = yearsForward == null ? null : Number(yearsForward);
  const traceList = Array.isArray(trace) ? trace : [];
  const stories = Array.isArray(storyCurations) ? storyCurations : [];
  const snap = snapshot && typeof snapshot === 'object' ? snapshot : null;
  const topics = Array.isArray(snap?.topics) ? snap.topics : [];
  const signals = Array.isArray(snap?.signals) ? snap.signals : [];

  const topicsBySection = new Map();
  for (const t of topics) {
    const section = String(t.section || 'Other');
    if (!topicsBySection.has(section)) topicsBySection.set(section, []);
    topicsBySection.get(section).push(t);
  }
  for (const [k, list] of topicsBySection.entries()) {
    topicsBySection.set(
      k,
      (list || []).sort((a, b) => (Number(b.score) || 0) - (Number(a.score) || 0))
    );
  }

  const storyYears = new Map();
  for (const s of stories) {
    const y = Number(s.yearsForward);
    if (!storyYears.has(y)) storyYears.set(y, []);
    storyYears.get(y).push(s);
  }
  for (const [y, list] of storyYears.entries()) {
    storyYears.set(
      y,
      (list || []).slice().sort((a, b) => {
        const as = String(a.section || '');
        const bs = String(b.section || '');
        if (as !== bs) return as.localeCompare(bs);
        return (Number(a.rank) || 0) - (Number(b.rank) || 0);
      })
    );
  }

  const yearsToRender = selectedYear == null ? yearsList : [selectedYear];
  const dayLabel = String(day || '');
  const dayC = dayCuration && typeof dayCuration === 'object' ? dayCuration : null;
  const provider = dayC ? String(dayC.provider || '') : '';
  const model = dayC ? String(dayC.model || '') : '';
  const curationAt = dayC ? String(dayC.generatedAt || '') : '';
  const curationError = dayC ? String(dayC.error || '') : '';

  const navLinks = yearsList
    .map((y) => {
      const active = selectedYear === y ? ' style="background:#111;color:#fff;border-color:#111"' : '';
      const href = `/api/admin/curation?format=html&day=${encodeURIComponent(dayLabel)}&years=${encodeURIComponent(String(y))}`;
      return `<a class="pill"${active} href="${escapeHtml(href)}">+${escapeHtml(String(y))}y</a>`;
    })
    .join('');

  const evidenceHref = `/api/day-signal?day=${encodeURIComponent(dayLabel)}&format=html`;
  const frontHref = `/index.html?years=${encodeURIComponent(String(selectedYear == null ? 5 : selectedYear))}&day=${encodeURIComponent(dayLabel)}`;
  const traceHref = `/api/admin/trace?day=${encodeURIComponent(dayLabel)}&format=html`;
  const jsonHref = `/api/admin/curation?day=${encodeURIComponent(dayLabel)}&format=json${selectedYear == null ? '' : `&years=${encodeURIComponent(String(selectedYear))}`}`;

  const topSignals = signals.slice(0, 12);
  const marketSignals = signals.filter((s) => s.type === 'market').slice(0, 8);
  const econSignals = signals.filter((s) => s.type === 'econ').slice(0, 8);

  const renderSignalMiniList = (items) => {
    const list = Array.isArray(items) ? items : [];
    if (!list.length) return `<div class="muted">—</div>`;
    return `<ol class="mini">
      ${list
        .map((s) => {
          const url = String(s.url || '');
          const title = String(s.title || '').slice(0, 140);
          return `<li>${url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener">${escapeHtml(title)}</a>` : escapeHtml(title)}</li>`;
        })
        .join('')}
    </ol>`;
  };

  const renderBullets = (list) => {
    const arr = Array.isArray(list) ? list.map((x) => String(x || '').trim()).filter(Boolean) : [];
    if (!arr.length) return `<div class="muted">—</div>`;
    return `<ul class="bullets">${arr.map((x) => `<li>${escapeHtml(x)}</li>`).join('')}</ul>`;
  };

  const renderYearBlock = (y) => {
    const yearStories = storyYears.get(y) || [];
    if (!yearStories.length) {
      return `<section class="card"><h2>+${escapeHtml(String(y))}y</h2><div class="muted">No curated stories stored for this year.</div></section>`;
    }
    const plan = dayC && dayC.payload && dayC.payload.editions ? dayC.payload.editions[String(y)] : null;
    const promptRec = dayC && dayC.prompt && dayC.prompt.editions ? dayC.prompt.editions[String(y)] : null;
    const systemPrompt = dayC && dayC.prompt ? String(dayC.prompt.systemPrompt || '') : '';
    const promptText = promptRec ? String(promptRec.prompt || '') : '';
    const editionDate = String(plan?.editionDate || yearStories[0]?.plan?.editionDate || '');
    const thesis = String(plan?.editionThesis || '').trim();
    const thinkingTrace = Array.isArray(plan?.thinkingTrace) ? plan.thinkingTrace : [];
    const keyStoryIds = Array.isArray(plan?.keyStoryIds) ? plan.keyStoryIds : [];
    const hero = yearStories.find((s) => s.plan && s.plan.hero) || null;

    const bySection = new Map();
    for (const s of yearStories) {
      const section = String(s.section || 'Other');
      if (!bySection.has(section)) bySection.set(section, []);
      bySection.get(section).push(s);
    }
    for (const [section, list] of bySection.entries()) {
      bySection.set(section, (list || []).slice().sort((a, b) => (Number(a.rank) || 0) - (Number(b.rank) || 0)));
    }

    const renderStory = (s) => {
      const p = s.plan || {};
      const title = String(p.curatedTitle || s.storyId || '').trim() || s.storyId;
      const dek = String(p.curatedDek || '').trim();
      const topicTitle = String(p.topicTitle || '').trim();
      const key = Boolean(p.key);
      const isHero = Boolean(p.hero);
      const dirs = String(p.sparkDirections || '').trim();
      const eventSeed = String(p.futureEventSeed || '').trim();
      const outline = Array.isArray(p.outline) ? p.outline : [];
      const extrap = Array.isArray(p.extrapolationTrace) ? p.extrapolationTrace : [];
      const rationale = Array.isArray(p.rationale) ? p.rationale : [];
      const draft = s.article && typeof s.article === 'object' ? s.article : null;
      const draftBody = draft ? String(draft.body || '').trim() : '';

      const articleHref = `/article.html?id=${encodeURIComponent(String(s.storyId || ''))}&years=${encodeURIComponent(String(y))}&day=${encodeURIComponent(dayLabel)}`;
      const confidence = Number(p.confidence) || 0;

      const badges = [
        key ? `<span class="badge">key</span>` : '',
        isHero ? `<span class="badge hero">hero</span>` : '',
        topicTitle ? `<span class="badge">${escapeHtml(topicTitle)}</span>` : '',
        confidence ? `<span class="badge" style="border-color:${confidence >= 80 ? '#2d7d46' : confidence >= 60 ? '#b8860b' : '#c0392b'};color:${confidence >= 80 ? '#2d7d46' : confidence >= 60 ? '#b8860b' : '#c0392b'}">${confidence}% conf</span>` : ''
      ]
        .filter(Boolean)
        .join(' ');

      return `
        <details class="story">
          <summary>
            <span class="mono">${escapeHtml(String(s.section || ''))} #${escapeHtml(String(s.rank || ''))}</span>
            <span class="title">${escapeHtml(title)}</span>
            <span class="badges">${badges}</span>
          </summary>
          ${dek ? `<div class="dek">${escapeHtml(dek)}</div>` : `<div class="muted">No curated dek.</div>`}
          <div class="row">
            <a class="pill" href="${escapeHtml(articleHref)}" target="_blank" rel="noopener">Open article</a>
            <span class="muted mono">storyId ${escapeHtml(String(s.storyId || ''))}</span>
          </div>
          <div class="grid2">
            <div>
              <h4>Future event seed</h4>
              <div class="monoBox">${eventSeed ? escapeHtml(eventSeed) : '<span class="muted">—</span>'}</div>
              <h4>Curator directions</h4>
              <div class="monoBox">${dirs ? escapeHtml(dirs) : '<span class="muted">—</span>'}</div>
            </div>
            <div>
              <h4>Outline</h4>
              ${renderBullets(outline)}
              <h4>Extrapolation trace</h4>
              ${renderBullets(extrap)}
              <h4>Rationale</h4>
              ${renderBullets(rationale)}
            </div>
          </div>
          ${
            draftBody
              ? `
                <details style="margin-top:10px">
                  <summary class="mono" style="cursor:pointer">Prewritten draft (key story)</summary>
                  <div class="monoBox" style="max-height:540px;overflow:auto;margin-top:8px">${escapeHtml(draftBody)}</div>
                </details>
              `
              : ''
          }
        </details>
      `;
    };

    const sections = [...new Set([...sectionsOrder, ...bySection.keys()])].filter(Boolean);
    return `
      <section class="card">
        <div class="row" style="justify-content:space-between">
          <div>
            <h2>+${escapeHtml(String(y))}y <span class="muted">(${escapeHtml(editionDate || '—')})</span></h2>
            ${thesis ? `<div class="thesis">${escapeHtml(thesis)}</div>` : `<div class="muted">No edition thesis yet (rerun curation after adding thinking traces).</div>`}
          </div>
          <div class="row">
            ${hero ? `<span class="badge hero">hero ${escapeHtml(String(hero.storyId || '').slice(0, 42))}…</span>` : ''}
            ${keyStoryIds.length ? `<span class="badge">keys ${escapeHtml(String(keyStoryIds.length))}</span>` : ''}
            <a class="pill" href="/api/edition?years=${escapeHtml(encodeURIComponent(String(y)))}&day=${escapeHtml(encodeURIComponent(dayLabel))}" target="_blank" rel="noopener">Edition JSON</a>
            <a class="pill" href="/index.html?years=${escapeHtml(encodeURIComponent(String(y)))}&day=${escapeHtml(encodeURIComponent(dayLabel))}" target="_blank" rel="noopener">Open front page</a>
          </div>
        </div>
        <div style="margin-top:10px">
          <h3>Thinking trace</h3>
          ${renderBullets(thinkingTrace)}
        </div>
        ${
          promptText || systemPrompt
            ? `
              <details style="margin-top:10px">
                <summary class="mono" style="cursor:pointer">Prompt used (system + edition)</summary>
                ${systemPrompt ? `<h4>System prompt</h4><div class="monoBox">${escapeHtml(systemPrompt)}</div>` : ''}
                ${promptText ? `<h4>Edition prompt</h4><div class="monoBox" style="max-height:420px;overflow:auto">${escapeHtml(promptText)}</div>` : ''}
              </details>
            `
            : ''
        }
        ${sections
          .map((section) => {
            const list = bySection.get(section) || [];
            if (!list.length) return '';
            return `
              <div class="sectionBlock">
                <h3>${escapeHtml(section)}</h3>
                ${list.map(renderStory).join('')}
              </div>
            `;
          })
          .join('')}
      </section>
    `;
  };

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin - Extrapolations - ${escapeHtml(dayLabel)}</title>
  <style>
    :root{--bg:#ffffff;--fg:#111;--muted:#555;--border:#e5e5e5;--link:#0b4f8a;--mono: ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;--sans: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;}
    body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 var(--sans);}
    header{border-top:6px solid #111;border-bottom:1px solid #111;padding:16px 18px;}
    main{max-width:1240px;margin:0 auto;padding:18px;}
    h1{margin:0 0 6px;font-size:20px;letter-spacing:.02em;}
    h2{margin:0;font-size:18px;}
    h3{margin:18px 0 8px;font-size:13px;letter-spacing:.06em;text-transform:uppercase;color:#222;}
    h4{margin:12px 0 6px;font-size:12px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);}
    a{color:var(--link);text-decoration:none;}
    a:hover{text-decoration:underline;}
    .muted{color:var(--muted);}
    .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
    .card{border:1px solid var(--border);background:#fff;padding:12px;margin-top:14px;}
    .badge{display:inline-block;font-family:var(--mono);font-size:12px;border:1px solid var(--border);background:#f3f3f3;padding:2px 8px;border-radius:999px;}
    .badge.hero{background:#111;color:#fff;border-color:#111;}
    .pill{display:inline-block;border:1px solid var(--border);padding:6px 10px;border-radius:999px;font-size:12px;color:inherit;background:#fbfbfb;}
    .nav{margin-top:10px;display:flex;gap:8px;flex-wrap:wrap;}
    .mono{font-family:var(--mono);font-size:12px;}
    .thesis{margin-top:8px;max-width:100ch;}
    .mini{margin:8px 0 0 18px;}
    .mini li{margin:4px 0;}
    .bullets{margin:8px 0 0 18px;}
    .bullets li{margin:4px 0;}
    details.story{border-top:1px solid var(--border);padding:10px 0;}
    details.story:first-of-type{border-top:none;}
    details.story summary{cursor:pointer;display:flex;gap:10px;align-items:baseline;flex-wrap:wrap;}
    details.story summary .title{font-weight:700;}
    details.story summary .badges{display:flex;gap:6px;flex-wrap:wrap;}
    .dek{margin-top:8px;color:#2f2f2f;}
    .monoBox{border:1px solid var(--border);background:#fafafa;padding:8px;font-family:var(--mono);font-size:12px;white-space:pre-wrap;}
    .grid3{display:grid;grid-template-columns: 1fr 1fr 1fr;gap:12px;}
    .grid2{display:grid;grid-template-columns: 1fr 1fr;gap:12px;margin-top:10px;}
    @media (max-width: 980px){.grid3{grid-template-columns:1fr;} .grid2{grid-template-columns:1fr;}}
    .sectionBlock{margin-top:12px;}
  </style>
</head>
<body>
  <header>
    <div class="row" style="justify-content:space-between">
      <div>
        <h1>Admin: Evidence + Extrapolations</h1>
        <div class="muted">Day <strong>${escapeHtml(dayLabel)}</strong> • curator <code class="mono">${escapeHtml(provider || '—')}</code> <code class="mono">${escapeHtml(model || '—')}</code> • generated <code class="mono">${escapeHtml(curationAt || '—')}</code></div>
        ${curationError ? `<div class="muted" style="margin-top:6px"><strong>Error:</strong> ${escapeHtml(curationError.slice(0, 220))}… (see JSON)</div>` : ''}
      </div>
      <div class="row">
        <a class="pill" href="${escapeHtml(frontHref)}" target="_blank" rel="noopener">Front page</a>
        <a class="pill" href="${escapeHtml(evidenceHref)}" target="_blank" rel="noopener">Evidence pack</a>
        <a class="pill" href="${escapeHtml(traceHref)}" target="_blank" rel="noopener">Event trace</a>
        <a class="pill" href="${escapeHtml(jsonHref)}" target="_blank" rel="noopener">Raw JSON</a>
      </div>
    </div>
    <div class="nav" aria-label="Years">
      ${navLinks}
      ${selectedYear == null ? `<span class="muted mono">showing all years</span>` : `<span class="muted mono">showing +${escapeHtml(String(selectedYear))}y</span>`}
    </div>
  </header>
  <main>
    <section class="card">
      <h2>Baseline evidence (today)</h2>
      <div class="muted">Top topics + signals for ${escapeHtml(dayLabel)}.</div>
      <div class="grid3" style="margin-top:10px">
        <div>
          <h3>Key topics</h3>
          ${sectionsOrder
            .map((section) => {
              const list = (topicsBySection.get(section) || []).slice(0, 3);
              if (!list.length) return `<div class="muted"><strong>${escapeHtml(section)}:</strong> —</div>`;
              return `<div style="margin-top:8px"><div class="mono"><strong>${escapeHtml(section)}</strong></div><ul class="bullets">${list
                .map((t) => `<li>${escapeHtml(String(t.label || '').slice(0, 130))}</li>`)
                .join('')}</ul></div>`;
            })
            .join('')}
        </div>
        <div>
          <h3>Top signals</h3>
          ${renderSignalMiniList(topSignals)}
        </div>
        <div>
          <h3>Indicators</h3>
          <div class="mono"><strong>Markets</strong></div>
          ${renderSignalMiniList(marketSignals)}
          <div class="mono" style="margin-top:10px"><strong>Macro</strong></div>
          ${renderSignalMiniList(econSignals)}
        </div>
      </div>
    </section>

    ${yearsToRender.map(renderYearBlock).join('')}

    <section class="card">
      <h2>Event trace (latest)</h2>
      <div class="muted">Most recent ${escapeHtml(String(Math.min(40, traceList.length)))} events (open full trace for everything).</div>
      <table style="width:100%;border-collapse:collapse;margin-top:10px">
        <thead>
          <tr>
            <th style="text-align:left;border-top:1px solid var(--border);padding:8px 6px;color:var(--muted);font-size:11px;letter-spacing:.08em;text-transform:uppercase">Time</th>
            <th style="text-align:left;border-top:1px solid var(--border);padding:8px 6px;color:var(--muted);font-size:11px;letter-spacing:.08em;text-transform:uppercase">Event</th>
            <th style="text-align:left;border-top:1px solid var(--border);padding:8px 6px;color:var(--muted);font-size:11px;letter-spacing:.08em;text-transform:uppercase">Payload</th>
          </tr>
        </thead>
        <tbody>
          ${traceList
            .slice(-40)
            .map((e) => {
              const payload = e && e.payload ? JSON.stringify(e.payload).slice(0, 320) : '';
              return `
                <tr>
                  <td style="border-top:1px solid var(--border);padding:8px 6px" class="muted mono">${escapeHtml(String(e.ts || ''))}</td>
                  <td style="border-top:1px solid var(--border);padding:8px 6px" class="mono">${escapeHtml(String(e.type || ''))}</td>
                  <td style="border-top:1px solid var(--border);padding:8px 6px" class="muted mono">${payload ? escapeHtml(payload) : '—'}</td>
                </tr>
              `;
            })
            .join('')}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>`;
}

async function requestHandler(req, res) {
  try {
    if (!req.url) {
      sendNotFound(res);
      return;
    }

    const { url, pathname, years, day } = parseRequest(req.url);

    // ── Cron auth gate ──
    if (pathname.startsWith('/api/cron')) {
      if (!checkCronAuth(req, url)) {
        sendJson(res, { ok: false, error: 'unauthorized' }, 401);
        return;
      }
    }

    // ── Admin auth gate ──
    const isProtectedRoute = pathname.startsWith('/api/admin') || pathname === '/api/day-signal';
    if (isProtectedRoute && ADMIN_SECRET) {
      const authResult = checkAdminAuth(req, url);
      if (authResult === false) {
        // No valid auth — show login page
        sendAdminLogin(res);
        return;
      }
      if (authResult === 'set-cookie') {
        // Valid secret in query param — set cookie and redirect to clean URL
        const cleanUrl = new URL(url.href);
        cleanUrl.searchParams.delete('secret');
        res.writeHead(302, {
          'Location': cleanUrl.pathname + cleanUrl.search,
          'Set-Cookie': `${ADMIN_COOKIE_NAME}=${ADMIN_SECRET}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${ADMIN_COOKIE_MAX_AGE}`
        });
        res.end();
        return;
      }
	    }

	    // ── Cron endpoints (GET) ──
	    if (pathname === '/api/cron/pipeline') {
	      if (req.method !== 'GET') return send405(res, 'GET');
	      const requestedDay = day || pipeline.getLatestDay() || formatDay();
	      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
	      await pipeline.refresh({ day: builtDay, force: false });
	      const curated = await pipeline.curateDay(builtDay, { force: false });
	      sendJson(res, { ok: true, day: builtDay, curated });
	      return;
	    }

	    if (pathname === '/api/cron/images/refresh') {
	      if (req.method !== 'GET') return send405(res, 'GET');
	      const requestedDay = day || pipeline.getLatestDay() || formatDay();
	      const yearsForward = clampYears(url.searchParams.get('years') || String(EDITION_YEARS));
	      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
	      const count = Number(url.searchParams.get('count') || 30);
	      const ideas = await refreshIdeas({ day: builtDay, pipeline, yearsForward, count, force: false });
	      const heroes = await enqueueSectionHeroJobs({ day: builtDay, pipeline, yearsForward, force: false, includeGlobalHero: true });
	      sendJson(res, { ok: true, day: builtDay, yearsForward, ideas, heroes });
	      return;
	    }

	    if (pathname === '/api/cron/images/worker') {
	      if (req.method !== 'GET') return send405(res, 'GET');
	      const requestedDay = day || pipeline.getLatestDay() || formatDay();
	      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
	      const limit = Number(url.searchParams.get('limit') || 3);
	      const maxMs = Number(url.searchParams.get('maxMs') || 220000);
	      const result = await runImageWorker({ day: builtDay, limit, maxMs });
	      sendJson(res, { day: builtDay, ...result });
	      return;
	    }

	    if (pathname === '/api/ping') {
	      sendJson(res, { ok: true });
	      return;
	    }


    if (pathname === '/api/config') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const curatorConfig = getOpusCurationConfigFromEnv();
      sendJson(res, {
        provider: {
          mode: 'anthropic',
          articleModel: process.env.ARTICLE_MODEL || 'claude-sonnet-4-5-20250929'
        },
        curator: {
          mode: String(curatorConfig.mode || 'mock').toLowerCase(),
          model: curatorConfig.model || null,
          hasApiKey: Boolean(curatorConfig.apiKey),
          keyStoriesPerEdition: Number(curatorConfig.keyStoriesPerEdition || 1)
        },
        pipeline: {
          dbFile: pipeline.dbFile || null,
          latestDay: pipeline.getLatestDay() || null,
          status: pipeline.getStatus()
        },
        server: { host: DEFAULT_HOST, port: activePort }
      });
      return;
    }

    if (pathname === '/api/pipeline/status') {
      if (req.method !== 'GET') return send405(res, 'GET');
      sendJson(res, pipeline.getStatus());
      return;
    }

    if (pathname === '/api/pipeline/sources') {
      if (req.method !== 'GET') return send405(res, 'GET');
      sendJson(res, { sources: pipeline.listSources() });
      return;
    }

    if (pathname === '/api/pipeline/standing-topics') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const section = url.searchParams.get('section') || null;
      const topics = pipeline.getStandingTopics(section);
      sendJson(res, { topics });
      return;
    }

    if (pathname === '/api/pipeline/evidence-summary') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const evidenceDay = day || pipeline.getLatestDay() || formatDay();
      const summary = pipeline.getEvidenceSummary(evidenceDay);
      sendJson(res, { day: evidenceDay, summary });
      return;
    }

    if (pathname === '/api/pipeline/topic-evidence') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const topicKey = url.searchParams.get('topic_key') || '';
      if (!topicKey) return sendJson(res, { error: 'topic_key required' }, 400);
      const daysWindow = Number(url.searchParams.get('days') || 7);
      const evidence = pipeline.getTopicEvidence(topicKey, daysWindow);
      sendJson(res, { topicKey, evidence });
      return;
    }

    // ── Future Images (admin-only) ──
    if (pathname === '/api/admin/images') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const yearsForward = clampYears(url.searchParams.get('years') || String(EDITION_YEARS));
      sendHtml(res, renderImagesAdminHtml({ day: builtDay, yearsForward }));
      return;
    }

    if (pathname === '/api/admin/images/state') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const yearsForward = clampYears(url.searchParams.get('years') || String(EDITION_YEARS));
      const state = await getImagesAdminState({ day: requestedDay, pipeline, yearsForward });
      sendJson(res, state);
      return;
    }

    if (pathname === '/api/admin/images/ideas/refresh') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const yearsForward = clampYears(url.searchParams.get('years') || String(EDITION_YEARS));
      const force = String(url.searchParams.get('force') || '').toLowerCase() === 'true';
      // Keep default small enough to reliably fit within serverless timeouts.
      const count = Number(url.searchParams.get('count') || 30);
      const result = await refreshIdeas({ day: requestedDay, pipeline, yearsForward, count, force });
      sendJson(res, result, result && result.ok ? 200 : 400);
      return;
    }

    if (pathname === '/api/admin/images/newspaper/refresh') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const yearsForward = clampYears(url.searchParams.get('years') || String(EDITION_YEARS));
      const force = String(url.searchParams.get('force') || '').toLowerCase() === 'true';
      const result = await enqueueSectionHeroJobs({ day: requestedDay, pipeline, yearsForward, force, includeGlobalHero: true });
      sendJson(res, result, result && result.ok ? 200 : 400);
      return;
    }

    if (pathname === '/api/admin/images/jobs/run') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const limit = Number(url.searchParams.get('limit') || 3);
      const maxMs = Number(url.searchParams.get('maxMs') || 220000);
      const result = await runImageWorker({ day: requestedDay, limit, maxMs });
      sendJson(res, result, result && result.ok ? 200 : 400);
      return;
    }

    if (pathname === '/api/admin/images/jobs/enqueue') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const body = await readJsonBody(req);
      const kind = String(body?.kind || '').trim();
      const yearsForward = clampYears(body?.yearsForward ?? body?.years ?? url.searchParams.get('years') ?? String(EDITION_YEARS));
      const requestedDay = normalizeDay(body?.day) || day || pipeline.getLatestDay() || formatDay();
      const force = Boolean(body?.force);
      if (kind === 'story_section_hero') {
        const storyId = String(body?.storyId || '').trim();
        const section = String(body?.section || '').trim();
        const result = await enqueueSingleStoryHeroJob({ day: requestedDay, pipeline, storyId, section, yearsForward, force });
        sendJson(res, result, result && result.ok ? 200 : 400);
        return;
      }
      if (kind === 'idea_image') {
        const ideaId = String(body?.ideaId || '').trim();
        const result = await enqueueSingleIdeaJob({ ideaId, force });
        sendJson(res, result, result && result.ok ? 200 : 400);
        return;
      }
      sendJson(res, { ok: false, error: 'unknown_kind', kind }, 400);
      return;
    }

    if (pathname === '/api/admin/prerender') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const yearsParam = url.searchParams.get('years');
      const yearsList = yearsParam
        ? [clampYears(yearsParam)]
        : Array.from({ length: 11 }, (_, i) => i);

      const storyIds = [];
      const seen = new Set();
      for (const y of yearsList) {
        const edition = pipeline.getEdition(builtDay, y);
        const articles = Array.isArray(edition?.articles) ? edition.articles : [];
        for (const article of articles) {
          const id = String(article?.id || '').trim();
          if (!id || seen.has(id)) continue;
          seen.add(id);
          storyIds.push(id);
        }
      }

      let rendered = 0;
      let missing = 0;
      for (const storyId of storyIds) {
        const story = pipeline.getStory(storyId);
        if (!story) {
          missing++;
          continue;
        }
        const seedArticle = buildSeedArticleFromStory(story);
        pipeline.storeRendered(storyId, seedArticle, { curationGeneratedAt: story?.curation?.generatedAt || null });
        cacheByKey.set(keyFor(storyId, story), seedArticle);
        rendered++;
      }

      sendJson(res, {
        ok: true,
        day: builtDay,
        years: yearsList,
        stories: storyIds.length,
        rendered,
        missing
      });
      return;
    }

    if (pathname === '/api/admin/curate') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const force = String(url.searchParams.get('force') || '').toLowerCase() === 'true';
      const result = await pipeline.curateDay(builtDay, { force });
      sendJson(res, { ok: true, day: builtDay, result });
      return;
    }

    if (pathname === '/api/admin/curator/runtime') {
      if (req.method === 'GET') {
        const config = getOpusCurationConfigFromEnv();
        const persisted = getRuntimeConfigInfo();
        sendJson(res, {
          ok: true,
          persisted,
          curator: {
            mode: config.mode,
            model: config.model,
            hasApiKey: Boolean(config.apiKey),
            keyStoriesPerEdition: config.keyStoriesPerEdition,
            systemPrompt: config.systemPrompt,
            autoCurate: String(process.env.OPUS_AUTO_CURATE || '').trim()
              ? String(process.env.OPUS_AUTO_CURATE).toLowerCase() !== 'false'
              : AUTO_CURATE_DEFAULT
          }
        });
        return;
      }
      if (req.method === 'POST') {
        const body = await readJsonBody(req);
        const mode = body?.mode != null ? String(body.mode || '').trim().toLowerCase() : null;
        const model = body?.model != null ? String(body.model || '').trim() : null;
        const apiKey = body?.apiKey != null ? String(body.apiKey || '').trim() : null;
        const systemPrompt = body?.systemPrompt != null ? String(body.systemPrompt || '').trim() : null;
        const keyStories = body?.keyStoriesPerEdition != null ? Number(body.keyStoriesPerEdition) : null;
        const autoCurate = body?.autoCurate != null ? Boolean(body.autoCurate) : null;

        if (mode !== null) process.env.OPUS_MODE = mode;
        if (model !== null) process.env.OPUS_MODEL = model;
        if (apiKey !== null) {
          if (!apiKey) {
            delete process.env.OPUS_API_KEY;
          } else {
            process.env.OPUS_API_KEY = apiKey;
          }
        }
        if (systemPrompt !== null) {
          if (!systemPrompt) {
            delete process.env.OPUS_SYSTEM_PROMPT;
          } else {
            process.env.OPUS_SYSTEM_PROMPT = systemPrompt;
          }
        }
        if (keyStories !== null && Number.isFinite(keyStories)) {
          process.env.OPUS_KEY_STORIES_PER_EDITION = String(Math.max(0, Math.min(7, Math.round(keyStories))));
        }
        if (autoCurate !== null) {
          process.env.OPUS_AUTO_CURATE = autoCurate ? 'true' : 'false';
        }

        // Persist to a local runtime config file (outside the web-served project directory).
        try {
          const patch = {};
          if (mode !== null) patch.mode = mode;
          if (model !== null) patch.model = model;
          if (apiKey !== null) patch.apiKey = apiKey || null;
          if (systemPrompt !== null) patch.systemPrompt = systemPrompt || null;
          if (keyStories !== null && Number.isFinite(keyStories)) patch.keyStoriesPerEdition = Math.max(0, Math.min(7, Math.round(keyStories)));
          updateOpusRuntimeConfig(patch);
        } catch (err) {
          // Persistence is best-effort; still return current runtime state.
          pipeline.traceEvent(formatDay(), 'curator.runtime.persist_error', { error: String(err?.message || err) });
        }

        const config = getOpusCurationConfigFromEnv();
        const persisted = getRuntimeConfigInfo();
        sendJson(res, {
          ok: true,
          persisted,
          curator: {
            mode: config.mode,
            model: config.model,
            hasApiKey: Boolean(config.apiKey),
            keyStoriesPerEdition: config.keyStoriesPerEdition,
            systemPrompt: config.systemPrompt,
            autoCurate: String(process.env.OPUS_AUTO_CURATE || '').trim()
              ? String(process.env.OPUS_AUTO_CURATE).toLowerCase() !== 'false'
              : AUTO_CURATE_DEFAULT
          }
        });
        return;
      }
      return send405(res, 'GET, POST');
    }

    if (pathname === '/api/admin/daily') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const forceRefresh = String(url.searchParams.get('forceRefresh') || '').toLowerCase() === 'true';
      const forceCuration = String(url.searchParams.get('forceCuration') || '').toLowerCase() === 'true';
      // Clear ALL caches on rebuild to ensure fresh content
      if (forceCuration || forceRefresh) {
        pipeline.db.exec('DELETE FROM render_cache;');
        pipeline.db.exec('DELETE FROM story_curations;');
        pipeline.db.exec('DELETE FROM day_curations;');
        cacheByKey.clear();
      }
      await pipeline.refresh({ day: builtDay, force: forceRefresh });
      const result = await pipeline.curateDay(builtDay, { force: forceCuration });
      sendJson(res, { ok: true, day: builtDay, refreshed: true, curated: result });
      return;
    }

    if (pathname === '/api/admin/trace') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const limit = Number(url.searchParams.get('limit') || 240);
      const events = pipeline.getDayEventTrace(builtDay, limit);
      const format = String(url.searchParams.get('format') || '').trim().toLowerCase();
      const wantsHtml = format === 'html' || (!format && clientAcceptsHtml(req));
      if (wantsHtml) {
        sendHtml(res, renderAdminTraceHtml(builtDay, events));
        return;
      }
      sendJson(res, { ok: true, day: builtDay, events });
      return;
    }

    if (pathname === '/api/admin/curation') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const yearsParam = url.searchParams.get('years');
      const yearsForward = yearsParam == null ? null : clampYears(yearsParam);
      const traceLimit = Number(url.searchParams.get('traceLimit') || 240);
      const trace = pipeline.getDayEventTrace(builtDay, traceLimit);
      const dayCuration = pipeline.getDayCuration(builtDay);
      const storyCurations = pipeline.listStoryCurations(builtDay, { yearsForward });
      const format = String(url.searchParams.get('format') || '').trim().toLowerCase();
      const wantsHtml = format === 'html' || (!format && clientAcceptsHtml(req));
      if (wantsHtml) {
        const snapshot = pipeline.ensureDaySignalSnapshot(builtDay);
        sendHtml(
          res,
          renderAdminCurationHtml({
            day: builtDay,
            yearsForward,
            snapshot,
            trace,
            dayCuration,
            storyCurations
          })
        );
        return;
      }
      sendJson(res, { ok: true, day: builtDay, trace, dayCuration, storyCurations });
      return;
    }

    if (pathname === '/api/admin/curation/preview') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const yearsForward = clampYears(url.searchParams.get('years') || '5');
      const edition = pipeline.getEdition(builtDay, yearsForward, { applyCuration: false });
      if (!edition) {
        sendJson(res, { ok: false, error: 'edition_not_found', day: builtDay, years: yearsForward }, 404);
        return;
      }
      const editionDate = String(edition.date || '');
      const candidates = pipeline.listEditionStoryCandidates(builtDay, yearsForward);
      const snapshot = pipeline.buildCurationSnapshot(builtDay);
      const config = getOpusCurationConfigFromEnv();
      const keyCount = Number(url.searchParams.get('keyCount') || config.keyStoriesPerEdition || 1);
      const prompt = buildEditionCurationPrompt({
        day: builtDay,
        yearsForward,
        editionDate,
        candidates,
        snapshot,
        keyCount
      });
      sendJson(res, {
        ok: true,
        day: builtDay,
        yearsForward,
        editionDate,
        config: {
          mode: config.mode,
          model: config.model,
          keyStoriesPerEdition: config.keyStoriesPerEdition,
          systemPrompt: config.systemPrompt
        },
        candidates: candidates.map((c) => ({
          storyId: c.storyId,
          section: c.section,
          rank: c.rank,
          angle: c.angle,
          title: c.title,
          dek: c.dek,
          topic: c.topic ? { label: c.topic.label, theme: c.topic.theme } : null
        })),
        prompt
      });
      return;
    }

    if (pathname === '/api/admin/curation/apply') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const body = await readJsonBody(req);
      const requestedDay = normalizeDay(body?.day) || day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const yearsForward = clampYears(body?.yearsForward ?? body?.years ?? url.searchParams.get('years') ?? '5');
      const prompt = String(body?.prompt || '').trim();
      const systemPrompt = body?.systemPrompt != null ? String(body.systemPrompt) : undefined;
      const keyCount = body?.keyCount;
      const result = await pipeline.curateEditionFromPrompt(builtDay, yearsForward, { prompt, keyCount, systemPrompt });
      sendJson(res, { ok: true, day: builtDay, result });
      return;
    }

    if (pathname === '/api/admin/curation/override') {
      if (req.method !== 'POST') return send405(res, 'POST');
      const body = await readJsonBody(req);
      const storyId = String(body?.storyId || '').trim();
      if (!storyId) {
        sendJson(res, { ok: false, error: 'storyId_required' }, 400);
        return;
      }
      const patch = body?.patch && typeof body.patch === 'object' ? body.patch : body;
      const result = pipeline.overrideStoryCuration(storyId, patch);
      sendJson(res, { ok: true, result });
      return;
    }

    if (pathname === '/api/admin/dashboard') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const yearsForward = clampYears(url.searchParams.get('years') || '5');
      const previewUrl = `/api/admin/curation/preview?day=${encodeURIComponent(builtDay)}&years=${encodeURIComponent(String(yearsForward))}`;
      const curationUrl = `/api/admin/curation?day=${encodeURIComponent(builtDay)}&format=json`;
      const curationHtmlUrl = `/api/admin/curation?day=${encodeURIComponent(builtDay)}&format=html`;
      const traceUrl = `/api/admin/trace?day=${encodeURIComponent(builtDay)}&format=json`;
      const traceHtmlUrl = `/api/admin/trace?day=${encodeURIComponent(builtDay)}&format=html`;
      const frontHref = `/index.html?years=${encodeURIComponent(String(yearsForward))}&day=${encodeURIComponent(builtDay)}`;
      const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin - Curation - ${escapeHtml(builtDay)}</title>
  <style>
    body{font:14px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:18px;color:#111}
    h1{margin:0 0 8px;font-size:18px}
    h2{margin:0 0 8px;font-size:14px}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
    a{color:#0b4f8a;text-decoration:none} a:hover{text-decoration:underline}
    .muted{color:#555}
    .small{font-size:12px;color:#555}
    textarea{width:100%;min-height:280px;font:12px/1.4 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
    pre{white-space:pre-wrap;background:#f7f7f7;border:1px solid #e5e5e5;padding:10px}
    button{padding:8px 12px}
    .card{border:1px solid #e5e5e5;padding:12px;margin-top:12px}
    ol{margin:8px 0 0 18px}
    li{margin:6px 0}
    code{font:12px/1.4 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
  </style>
</head>
<body>
  <h1>Admin: Daily curation (${escapeHtml(builtDay)})</h1>
  <div class="row">
    <a href="${escapeHtml(frontHref)}" target="_blank" rel="noopener">Front page</a>
    <a href="${escapeHtml(curationHtmlUrl)}" target="_blank" rel="noopener">Extrapolations (HTML)</a>
    <a href="/api/admin/images?day=${escapeHtml(encodeURIComponent(builtDay))}&years=${escapeHtml(encodeURIComponent(String(yearsForward)))}" target="_blank" rel="noopener">Images</a>
    <a href="${escapeHtml(traceHtmlUrl)}" target="_blank" rel="noopener">Event trace (HTML)</a>
    <a href="${escapeHtml(curationUrl)}" target="_blank" rel="noopener">Curation (JSON)</a>
    <a href="${escapeHtml(previewUrl)}" target="_blank" rel="noopener">Prompt preview JSON (+${escapeHtml(String(yearsForward))}y)</a>
    <a href="/api/day-signal?day=${escapeHtml(encodeURIComponent(builtDay))}&format=html" target="_blank" rel="noopener">Day signal pack</a>
    <a href="/api/admin/polymarket?day=${escapeHtml(encodeURIComponent(builtDay))}&format=html" target="_blank" rel="noopener" style="color:#6f42c1;font-weight:600">Polymarket Outcomes</a>
  </div>

  <div class="card">
    <h2>How to run everything from here</h2>
    <div class="muted">This page is the control center: configure Opus, run the daily job, inspect results, and tweak prompts.</div>
    <ol>
      <li><strong>Set Opus config:</strong> choose model + paste API key, then click <code>Save runtime config</code>. (Key is stored locally; it is never shown in the UI.)</li>
      <li><strong>Run the pipeline:</strong> click <code>Run daily</code> to refresh + curate once/day. Use <code>Force rebuild</code> only when you need to refetch sources + rerun curation.</li>
      <li><strong>Inspect evidence:</strong> open <code>Day signal pack</code> to see what today’s inputs actually were.</li>
      <li><strong>Inspect extrapolations:</strong> open <code>Extrapolations (HTML)</code> to see Opus’s per-year plans (hero/key stories, directions, traces).</li>
      <li><strong>Tweak a single year:</strong> edit the edition prompt box and click <code>Apply prompt</code> (applies to +${escapeHtml(String(yearsForward))}y for this day).</li>
      <li><strong>Debug:</strong> open <code>Event trace (HTML)</code> to see refresh/curate step timing and errors.</li>
    </ol>
    <div class="row small" style="margin-top:10px">
      <span id="persistStatus">Loading persisted config…</span>
    </div>
  </div>

  <div class="card">
    <strong>Apply a custom prompt for +${escapeHtml(String(yearsForward))}y</strong>
    <div class="muted">Edit the prompt, then click Apply. This updates story curations for the edition.</div>
    <div class="muted" style="margin-top:8px">System prompt (provider-level):</div>
    <textarea id="systemPromptBox" style="min-height:110px" placeholder="Loading system prompt..."></textarea>
    <div class="muted" style="margin-top:8px">Anthropic model:</div>
    <input id="modelBox" placeholder="e.g. claude-opus-4-6 (falls back if unavailable)" style="width:100%;padding:8px"/>
    <div class="muted" style="margin-top:8px">Anthropic API key (runtime only, not displayed):</div>
    <input id="apiKeyBox" type="password" placeholder="Paste key to activate Anthropic (stored only in this running process)" style="width:100%;padding:8px"/>
    <div class="row" style="margin-top:8px">
      <button id="saveRuntimeBtn">Save runtime config</button>
      <button id="runDailyBtn">Run daily</button>
      <button id="forceDailyBtn">Force rebuild</button>
      <span id="runtimeStatus"></span>
    </div>
    <div class="muted" style="margin-top:8px">User prompt (edition-level):</div>
    <textarea id="promptBox" placeholder="Loading prompt..."></textarea>
    <div class="row" style="margin-top:8px">
      <button id="applyBtn">Apply prompt</button>
      <span id="status"></span>
    </div>
  </div>

  <div class="card">
    <strong>Latest trace (tail)</strong>
    <pre id="traceBox">Loading trace...</pre>
  </div>

  <script>
    const day = ${JSON.stringify(builtDay)};
    const yearsForward = ${JSON.stringify(yearsForward)};
    const statusEl = document.getElementById('status');
    const runtimeStatusEl = document.getElementById('runtimeStatus');
    const persistStatusEl = document.getElementById('persistStatus');
    const systemPromptBox = document.getElementById('systemPromptBox');
    const modelBox = document.getElementById('modelBox');
    const promptBox = document.getElementById('promptBox');
    const apiKeyBox = document.getElementById('apiKeyBox');
    const traceBox = document.getElementById('traceBox');
    async function load() {
      const preview = await fetch(${JSON.stringify(previewUrl)}).then(r => r.json());
      promptBox.value = (preview && preview.prompt) ? preview.prompt : '';
      systemPromptBox.value = (preview && preview.config && preview.config.systemPrompt) ? preview.config.systemPrompt : '';
      const runtime = await fetch('/api/admin/curator/runtime').then(r => r.json());
      if (runtime && runtime.curator) {
        runtimeStatusEl.textContent = 'Curator: mode=' + runtime.curator.mode + ' model=' + runtime.curator.model + ' hasKey=' + runtime.curator.hasApiKey;
        modelBox.value = runtime.curator.model || '';
      }
      if (runtime && runtime.persisted && persistStatusEl) {
        persistStatusEl.textContent =
          'Persisted config: ' + (runtime.persisted.exists ? 'saved' : 'not saved yet') +
          ' at ' + runtime.persisted.file;
      }
      const trace = await fetch(${JSON.stringify(traceUrl + '&limit=80')} ).then(r => r.json());
      const events = (trace && trace.events) ? trace.events : [];
      traceBox.textContent = JSON.stringify(events.slice(-40), null, 2);
    }
    document.getElementById('saveRuntimeBtn').addEventListener('click', async () => {
      runtimeStatusEl.textContent = 'Saving...';
      const apiKey = apiKeyBox.value || '';
      const systemPrompt = systemPromptBox.value || '';
      const model = modelBox.value || '';
      const payload = { mode: 'anthropic', systemPrompt };
      if (model.trim()) payload.model = model.trim();
      // Do not clear the existing key if the box is empty.
      if (apiKey.trim()) payload.apiKey = apiKey.trim();
      const resp = await fetch('/api/admin/curator/runtime', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload)
      }).then(r => r.json());
      apiKeyBox.value = '';
      runtimeStatusEl.textContent = resp && resp.ok ? ('Saved. mode=' + resp.curator.mode + ' hasKey=' + resp.curator.hasApiKey) : ('Error: ' + (resp.error || 'failed'));
      await load();
    });
    document.getElementById('runDailyBtn').addEventListener('click', async () => {
      runtimeStatusEl.textContent = 'Running daily...';
      const resp = await fetch('/api/admin/daily', { method: 'POST' }).then(r => r.json());
      runtimeStatusEl.textContent = resp && resp.ok ? 'Daily run finished.' : ('Error: ' + (resp.error || 'failed'));
      await load();
    });
    document.getElementById('forceDailyBtn').addEventListener('click', async () => {
      runtimeStatusEl.textContent = 'Forcing rebuild...';
      const resp = await fetch('/api/admin/daily?forceRefresh=true&forceCuration=true', { method: 'POST' }).then(r => r.json());
      runtimeStatusEl.textContent = resp && resp.ok ? 'Forced rebuild finished.' : ('Error: ' + (resp.error || 'failed'));
      await load();
    });
    document.getElementById('applyBtn').addEventListener('click', async () => {
      statusEl.textContent = 'Applying...';
      const prompt = promptBox.value || '';
      const systemPrompt = systemPromptBox.value || '';
      const resp = await fetch('/api/admin/curation/apply', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ day, yearsForward, prompt, systemPrompt })
      }).then(r => r.json());
      statusEl.textContent = resp && resp.ok ? 'Applied.' : ('Error: ' + (resp.error || 'failed'));
      await load();
    });
    load().catch(err => { statusEl.textContent = String(err && err.message || err); });
  </script>
</body>
</html>`;
      sendHtml(res, html);
      return;
    }

    // ── Polymarket implied outcomes tab ──
    if (pathname === '/api/admin/polymarket') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const format = String(url.searchParams.get('format') || '').trim().toLowerCase();
      const wantsHtml = format === 'html' || (!format && clientAcceptsHtml(req));

      // Fetch LIVE Polymarket data to get current probabilities
      let liveMarkets = [];
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 12000);
        const polyResp = await fetch(
          'https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=200&order=volume&ascending=false',
          { headers: { 'user-agent': 'FutureTimesBot/1.0', accept: 'application/json' }, signal: controller.signal }
        ).finally(() => clearTimeout(timeout));
        if (polyResp.ok) {
          const polyData = await polyResp.json();
          liveMarkets = Array.isArray(polyData) ? polyData : Array.isArray(polyData?.markets) ? polyData.markets : [];
        }
      } catch (err) {
        // Fall back to DB data if live fetch fails
      }

      // If live fetch failed, use DB data
      if (!liveMarkets.length) {
        const rows = pipeline.db.prepare(
          `SELECT title, summary, canonical_url FROM signals WHERE signal_type='market' AND day=? ORDER BY score DESC`
        ).all(builtDay);
        liveMarkets = rows.map((r) => ({
          question: r.title,
          endDate: (r.summary || '').match(/Closes:\s*(\d{4}-\d{2}-\d{2})/)?.[1] || '',
          volumeNum: Number((r.summary || '').match(/Volume:\s*\$([\d,]+)/)?.[1]?.replace(/,/g, '') || 0),
          slug: (r.canonical_url || '').split('/market/')[1] || '',
          _dbFallback: true
        }));
      }

      // Parse and group by year
      function parseYesProbability(m) {
        let outcomes = m?.outcomes;
        let prices = m?.outcomePrices;
        if (typeof outcomes === 'string') try { outcomes = JSON.parse(outcomes); } catch { return null; }
        if (typeof prices === 'string') try { prices = JSON.parse(prices); } catch { return null; }
        if (!Array.isArray(outcomes) || !Array.isArray(prices) || outcomes.length !== prices.length) return null;
        const yesIdx = outcomes.findIndex((o) => String(o || '').toLowerCase() === 'yes');
        if (yesIdx < 0) return null;
        const p = Number(prices[yesIdx]);
        if (!Number.isFinite(p)) return null;
        return p >= 0 && p <= 1 ? p : p > 1 && p <= 100 ? p / 100 : null;
      }

      const byYear = {};
      const allMarkets = [];
      for (const m of liveMarkets) {
        const question = String(m.question || m.title || '').trim();
        if (!question) continue;
        const prob = parseYesProbability(m);
        const endDate = String(m.endDate || '').trim();
        const closesMatch = endDate.match(/(\d{4})-(\d{2})-(\d{2})/);
        const closeYear = closesMatch ? Number(closesMatch[1]) : null;
        const closeDate = closesMatch ? `${closesMatch[1]}-${closesMatch[2]}-${closesMatch[3]}` : null;
        const volume = Number(m.volumeNum ?? m.volume ?? 0) || 0;
        const slug = String(m.slug || '').trim();
        const marketUrl = slug ? `https://polymarket.com/market/${slug}` : '';

        const entry = {
          title: question,
          prob,
          probDisplay: prob !== null ? `${(prob * 100).toFixed(0)}%` : '?',
          impliedOutcome: prob !== null ? (prob >= 0.5 ? 'Likely YES' : 'Likely NO') : 'Unknown',
          closeDate,
          closeYear,
          volume,
          url: marketUrl,
          endDate
        };
        allMarkets.push(entry);

        const yearKey = closeYear || 'unknown';
        if (!byYear[yearKey]) byYear[yearKey] = [];
        byYear[yearKey].push(entry);
      }

      // Sort years, filter out trivial markets (weather, short-term sports)
      const sortedYears = Object.keys(byYear)
        .filter((k) => k !== 'unknown')
        .sort((a, b) => Number(a) - Number(b));

      if (wantsHtml) {
        const yearSections = sortedYears.map((year) => {
          const markets = byYear[year]
            .filter((m) => m.volume >= 500) // skip micro-volume markets
            .sort((a, b) => b.volume - a.volume);
          if (!markets.length) return '';

          const rows = markets.map((m) => {
            const probColor = m.prob === null ? '#999' : m.prob >= 0.7 ? '#1a7f37' : m.prob >= 0.5 ? '#9a6700' : m.prob >= 0.3 ? '#cf222e' : '#cf222e';
            const outcomeLabel = m.prob === null ? '?' : m.prob >= 0.7 ? 'Very Likely' : m.prob >= 0.5 ? 'Likely' : m.prob >= 0.3 ? 'Unlikely' : 'Very Unlikely';
            return `<tr>
              <td style="max-width:400px"><a href="${escapeHtml(m.url)}" target="_blank" rel="noopener">${escapeHtml(m.title)}</a></td>
              <td style="text-align:center;font-weight:600;color:${probColor}">${escapeHtml(m.probDisplay)}</td>
              <td style="text-align:center;color:${probColor}">${escapeHtml(outcomeLabel)}</td>
              <td style="text-align:right;color:#555">$${m.volume.toLocaleString('en-US')}</td>
              <td style="color:#555">${escapeHtml(m.closeDate || '?')}</td>
            </tr>`;
          }).join('');

          return `
            <h2 style="margin-top:24px;border-bottom:2px solid #111;padding-bottom:4px">${escapeHtml(year)} Markets (${markets.length})</h2>
            <table style="width:100%;border-collapse:collapse;font-size:13px">
              <thead>
                <tr style="background:#f7f7f7;text-align:left">
                  <th style="padding:6px 8px">Question</th>
                  <th style="padding:6px 8px;text-align:center">Prob</th>
                  <th style="padding:6px 8px;text-align:center">Implied Outcome</th>
                  <th style="padding:6px 8px;text-align:right">Volume</th>
                  <th style="padding:6px 8px">Closes</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>`;
        }).filter(Boolean).join('');

        const totalMarkets = allMarkets.length;
        const withProb = allMarkets.filter((m) => m.prob !== null).length;

        const adminHref = `/api/admin/curation?day=${encodeURIComponent(builtDay)}&format=html`;
        const polyHtml = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Polymarket Implied Outcomes - ${escapeHtml(builtDay)}</title>
  <style>
    body{font:14px/1.45 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:18px;color:#111;max-width:1100px}
    h1{margin:0 0 4px;font-size:20px}
    h2{font-size:16px;margin:0}
    a{color:#0b4f8a;text-decoration:none} a:hover{text-decoration:underline}
    table{margin-top:8px} th,td{padding:5px 8px;border-bottom:1px solid #eee}
    .muted{color:#555;font-size:13px}
    .summary{display:flex;gap:24px;margin:12px 0;padding:12px;background:#f0f4f8;border-radius:6px}
    .summary .stat{text-align:center}
    .summary .stat .num{font-size:24px;font-weight:700;color:#0b4f8a}
    .summary .stat .label{font-size:11px;color:#555;text-transform:uppercase}
  </style>
</head>
<body>
  <h1>Polymarket Implied Outcomes</h1>
  <div class="muted">Data from ${escapeHtml(builtDay)} • <a href="${escapeHtml(adminHref)}">Back to admin</a></div>

  <div class="summary">
    <div class="stat"><div class="num">${totalMarkets}</div><div class="label">Total Markets</div></div>
    <div class="stat"><div class="num">${withProb}</div><div class="label">With Probability</div></div>
    <div class="stat"><div class="num">${sortedYears.length}</div><div class="label">Years Covered</div></div>
    <div class="stat"><div class="num">${sortedYears[sortedYears.length - 1] || '?'}</div><div class="label">Furthest Out</div></div>
  </div>

  <div class="muted" style="margin-bottom:12px">
    Markets are pulled from the <a href="https://gamma-api.polymarket.com" target="_blank">Polymarket Gamma API</a> (top 200 by volume).
    The "Implied Outcome" column shows the most likely resolution based on current odds. Use these as inputs for forecasting — they represent the market's current best guess.
    Low-volume markets (&lt;$500) are hidden.
  </div>

  ${yearSections || '<p>No market signals found for this day.</p>'}
</body>
</html>`;
        sendHtml(res, polyHtml);
        return;
      }

      sendJson(res, { day: builtDay, totalMarkets: allMarkets.length, byYear, sortedYears });
      return;
    }

    if (pathname === '/api/day-signal') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const format = String(url.searchParams.get('format') || '').trim().toLowerCase();
      const wantsHtml = format === 'html' || (!format && clientAcceptsHtml(req));
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const snapshot = pipeline.ensureDaySignalSnapshot(builtDay);
      if (!snapshot) {
        sendJson(res, { error: 'signal_snapshot_not_found', day: builtDay }, 404);
        return;
      }
      if (wantsHtml) {
        sendHtml(res, renderDaySignalHtml(snapshot));
        return;
      }
      if (format === 'pretty') {
        sendJsonPretty(res, snapshot);
        return;
      }
      sendJson(res, snapshot);
      return;
    }

    if (pathname.startsWith('/api/edition')) {
      if (req.method !== 'GET') return send405(res, 'GET');
      const requestedDay = day || pipeline.getLatestDay() || formatDay();
      const builtDay = await pipeline.ensureDayBuilt(requestedDay);
      const payload = pipeline.getEdition(builtDay, years);
      if (!payload) {
        sendJson(res, { error: 'edition_not_found', day: builtDay, years }, 404);
        return;
      }
      let decorated = payload;
      try {
        decorated = await decorateEditionPayload(payload, { day: builtDay, yearsForward: years });
      } catch {
        // best-effort: never break newspaper
      }
      sendJson(res, decorated);
      return;
    }

    // ── Expand short article body to full-length via Sonnet (real-time SSE) ──
    if (pathname.match(/^\/api\/article\/.+\/expand$/)) {
      if (req.method !== 'POST') { send405(res, 'POST'); return; }
      const storyId = pathname.replace('/api/article/', '').replace('/expand', '');
      if (!storyId) { sendNotFound(res); return; }

      let story = pipeline.getStory(storyId);
      if (!story) {
        const match = String(storyId).match(/^ft-(\d{4}-\d{2}-\d{2})-y(\d+)/);
        if (match) { await pipeline.ensureDayBuilt(match[1]); story = pipeline.getStory(storyId); }
      }
      if (!story) { sendJson(res, { error: 'not_found' }, 404); return; }

      const existing = getArticleStatus(storyId, story);
      const currentBody = existing?.article?.body || '';

      const curation = story?.curation || {};
      const editionDate = story?.evidencePack?.editionDate || '';
      const baselineDay = normalizeDay(story.day) || formatDay();
      const baselineYear = baselineDay.slice(0, 4) || '2026';
      const targetYear = Number(baselineYear) + (Number.isFinite(Number(story.yearsForward)) ? Number(story.yearsForward) : 0);
      const title = curation?.curatedTitle || curation?.draftArticle?.title || story.title || '';
      const dek = curation?.curatedDek || curation?.draftArticle?.dek || story.dek || '';
      const directions = curation?.sparkDirections || '';
      const eventSeed = curation?.futureEventSeed || '';

      const expandPrompt = [
        `You are writing a full-length article for The Future Times, published on ${editionDate || targetYear}.`,
        `Write it as a REAL news report from ${targetYear}. You are a journalist in ${targetYear} reporting on events happening NOW.`,
        ``,
        `CRITICAL RULES:`,
        `- Write about what is HAPPENING in ${targetYear}, not about what happened in ${baselineYear}.`,
        `- Do NOT frame as "anniversary" or "looking back" pieces.`,
        `- Do NOT reference or link to original news stories from ${baselineYear}.`,
        `- Write PREDICTIONS and PLAUSIBLE OUTCOMES: policies, technologies, markets, specific numbers.`,
        `- Do not describe the article as a projection, simulation, or prompt output.`,
        ``,
        `You have a SHORT DRAFT of this article. EXPAND it into a full 5-7 paragraph article.`,
        `Keep the same facts, tone, and angle but add depth: more context, quotes from plausible sources, specific data, implications.`,
        `Output narrative paragraphs only (NYT-style prose, no markdown headers, no bullet lists, no Sources section).`,
        ``,
        directions ? `Curator directions: ${directions}` : '',
        eventSeed ? `Future event seed: ${eventSeed}` : '',
        ``,
        `Headline: ${title}`,
        dek ? `Dek: ${dek}` : '',
        ``,
        `SHORT DRAFT TO EXPAND:`,
        currentBody,
        ``,
        `Write the full expanded article body now.`
      ].filter(Boolean).join('\n');

      const opusCfg = readOpusRuntimeConfig() || {};
      const apiKey = process.env.ANTHROPIC_API_KEY || opusCfg.apiKey || '';
      if (!apiKey) { sendJson(res, { error: 'no_api_key' }, 500); return; }

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*'
      });

      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 120000);
        const resp = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-api-key': apiKey,
            'anthropic-version': '2023-06-01'
          },
          body: JSON.stringify({
            model: process.env.ARTICLE_MODEL || 'claude-sonnet-4-5-20250929',
            max_tokens: 4096,
            temperature: 0.6,
            stream: true,
            messages: [{ role: 'user', content: expandPrompt }]
          }),
          signal: controller.signal
        });

        if (!resp.ok) {
          const errText = await resp.text().catch(() => '');
          res.write(`data: ${JSON.stringify({ type: 'error', error: `API ${resp.status}: ${errText.slice(0, 200)}` })}\n\n`);
          res.end();
          clearTimeout(timeout);
          return;
        }

        let fullBody = '';
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let sseBuffer = '';

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          sseBuffer += decoder.decode(value, { stream: true });

          const lines = sseBuffer.split('\n');
          sseBuffer = lines.pop() || '';

          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            const d = line.slice(6).trim();
            if (d === '[DONE]') continue;
            try {
              const evt = JSON.parse(d);
              if (evt.type === 'content_block_delta' && evt.delta?.type === 'text_delta') {
                const text = evt.delta.text || '';
                fullBody += text;
                res.write(`data: ${JSON.stringify({ type: 'chunk', delta: text })}\n\n`);
              }
            } catch { /* skip */ }
          }
        }

        clearTimeout(timeout);

        if (fullBody.trim().length > 200) {
          const expandedArticle = {
            ...(existing?.article || {}),
            body: fullBody.trim(),
            expandedAt: new Date().toISOString()
          };
          pipeline.storeRendered(storyId, expandedArticle, { curationGeneratedAt: curation?.generatedAt || null });
          cacheByKey.set(keyFor(storyId, story), expandedArticle);
        }

        res.write(`data: ${JSON.stringify({ type: 'complete', body: fullBody.trim() })}\n\n`);
        res.end();
      } catch (err) {
        res.write(`data: ${JSON.stringify({ type: 'error', error: String(err?.message || err) })}\n\n`);
        res.end();
      }
      return;
    }

    if (pathname.startsWith('/api/article/')) {
      const storyId = parseStoryIdFromPath(pathname);
      if (!storyId) {
        sendNotFound(res);
        return;
      }

      let story = pipeline.getStory(storyId);
      if (!story) {
        const match = String(storyId).match(/^ft-(\d{4}-\d{2}-\d{2})-y(\d+)/);
        if (match) {
          await pipeline.ensureDayBuilt(match[1]);
          story = pipeline.getStory(storyId);
        }
      }
      if (!story) {
        sendJson(res, { status: 'not_found', storyId, error: 'Unknown story id' }, 404);
        return;
      }

      const status = getArticleStatus(storyId, story);

      if (req.method === 'GET') {
        if (status.status === 'ready') {
          let article = status.article;
          try {
            article = await decorateArticlePayload(status.article, { day: story.day, yearsForward: story.yearsForward, storyId });
          } catch {
            // ignore
          }
          sendJson(res, { status: 'ready', article });
          return;
        }
        sendJson(res, { status: status.status, startedAt: status.startedAt, storyId });
        return;
      }

      if (req.method === 'POST') {
        const started = startRenderJob(storyId, story);
        if (started.status === 'not_found') {
          sendJson(res, { status: 'not_found', storyId, error: 'Unknown story id' }, 404);
          return;
        }
        if (started.status === 'cached') {
          let article = started.article;
          try {
            article = await decorateArticlePayload(started.article, { day: story.day, yearsForward: story.yearsForward, storyId });
          } catch {
            // ignore
          }
          sendJson(res, { status: 'ready', article });
          return;
        }
        sendJson(res, { status: started.status, storyId, years: story.yearsForward, day: story.day });
        return;
      }

      send405(res, 'GET, POST');
      return;
    }

    const normalized = normalizePath(pathname);
    if (normalized === '/api') {
      sendNotFound(res);
      return;
    }

    const filePath = ensureSafeFilePath(normalized);
    if (!filePath) {
      sendNotFound(res);
      return;
    }
    serveFile(res, filePath);
  } catch (err) {
    send500(res, err);
  }
}

function socketHandler(socket) {
  socket.on('message', async (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      safeSend(socket, { type: 'render.error', error: 'Invalid JSON payload' });
      return;
    }

    if (message.type !== 'render.article') {
      safeSend(socket, { type: 'render.error', error: `Unknown message type: ${message.type}` });
      return;
    }

    const storyId = message.articleId || message.storyId;
    const requestId = message.requestId || `req-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    if (!storyId) {
      safeSend(socket, { type: 'render.error', requestId, error: 'articleId missing' });
      return;
    }

    let story = pipeline.getStory(storyId);
    if (!story) {
      const match = String(storyId).match(/^ft-(\d{4}-\d{2}-\d{2})-y(\d+)/);
      if (match) {
        await pipeline.ensureDayBuilt(match[1]);
        story = pipeline.getStory(storyId);
      }
    }
    if (!story) {
      safeSend(socket, { type: 'render.error', requestId, error: `Unknown story: ${storyId}` });
      return;
    }

    const key = keyFor(storyId, story);
    const cached =
      cacheByKey.get(key) || pipeline.getRenderedVariant(storyId, { curationGeneratedAt: story?.curation?.generatedAt || '' });
    if (cached) {
      cacheByKey.set(key, cached);
      safeSend(socket, { type: 'render.complete', requestId, articleId: storyId, article: cached });
      return;
    }

    const jobState = startRenderJob(storyId, story);
    const job = jobState.job || getActiveJob(key);
    if (!job) {
      safeSend(socket, { type: 'render.error', requestId, error: `Unable to start render: ${storyId}` });
      return;
    }

    subscribeSocketToJob(job, socket, requestId);
    safeSend(socket, { type: 'render.queued', requestId, articleId: storyId, status: job.status, startedAt: job.startedAt });
  });

  socket.on('close', () => {
    removeSocket(socket);
  });
}

function isPortAvailable(port) {
  return new Promise((resolve) => {
    const probe = net.createServer();
    const done = () => {
      try {
        probe.close();
      } catch {
        // ignore
      }
    };
    probe.once('error', () => resolve(false));
    probe.once('listening', () => {
      done();
      resolve(true);
    });
    probe.listen(port, DEFAULT_HOST);
  });
}

async function start() {
  const server = http.createServer(requestHandler);
  const wss = new WebSocketServer({ server, path: '/ws' });
  wss.on('connection', socketHandler);

  for (let i = 0; i < PORT_MAX_TRIES; i++) {
    const candidatePort = PORT_START + i * PORT_FALLBACK_STEP;
    const canUse = await isPortAvailable(candidatePort);
    if (!canUse) continue;

    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(candidatePort, DEFAULT_HOST, () => {
        activePort = candidatePort;
        resolve();
      });
    }).catch(() => {});

    if (activePort === candidatePort) {
      console.log(`Future Times running on http://${DEFAULT_HOST}:${activePort}`);
      console.log(`WebSocket endpoint: ws://${DEFAULT_HOST}:${activePort}/ws`);
      return;
    }
  }

  console.error(
    `Unable to bind port: no available ports in range ${PORT_START}..${PORT_START + PORT_MAX_TRIES * PORT_FALLBACK_STEP}`
  );
  process.exit(1);
}

// Export for Vercel serverless + local start
export { requestHandler };

// Only auto-start when run directly (not imported by Vercel)
const isDirectRun = process.argv[1] && (
  process.argv[1].endsWith('/server.js') ||
  process.argv[1].endsWith('server/server.js')
);
if (isDirectRun) {
  start().catch((err) => {
    console.error('Failed to start server:', err?.message || err);
    process.exit(1);
  });
}
