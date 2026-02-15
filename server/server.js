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
import { getRuntimeConfigInfo, updateOpusRuntimeConfig } from './pipeline/runtimeConfig.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');

const DEFAULT_HOST = process.env.HOST || '127.0.0.1';
const PORT_START = Number(process.env.PORT || 57965);
const PORT_FALLBACK_STEP = Number(process.env.PORT_FALLBACK_STEP || 53);
const PORT_MAX_TRIES = Number(process.env.PORT_MAX_TRIES || 48);

const SPARK_MODE = (process.env.SPARK_MODE || 'mock').toLowerCase();
const SPARK_WS_URL = process.env.SPARK_WS_URL || '';
const SPARK_HTTP_URL = process.env.SPARK_HTTP_URL || '';
const SPARK_AUTH_TOKEN = process.env.SPARK_AUTH_TOKEN || process.env.SPARK_TOKEN || '';
const SPARK_AUTH_HEADER = process.env.SPARK_AUTH_HEADER || 'Authorization';
const SPARK_AUTH_PREFIX = process.env.SPARK_AUTH_PREFIX || 'Bearer';
const SPARK_REQUEST_TIMEOUT_MS = Number(process.env.SPARK_REQUEST_TIMEOUT_MS || 22000);
const SPARK_FALLBACK_TO_MOCK = process.env.SPARK_FALLBACK_TO_MOCK !== 'false';

const PIPELINE_REFRESH_MS = Number(process.env.PIPELINE_REFRESH_MS || 1000 * 60 * 60);
const AUTO_CURATE_DEFAULT = process.env.OPUS_AUTO_CURATE !== 'false';
const MAX_BODY_CHUNK_BYTES = 740;
const JOB_TTL_MS = 1000 * 60 * 10;

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

function isSparkConfigured() {
  if (SPARK_MODE === 'mock') return false;
  return !!(SPARK_WS_URL || SPARK_HTTP_URL);
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
  const pack = story.evidencePack || {};
  const topic = pack.topic || {};
  const citations = Array.isArray(pack.citations) ? pack.citations : [];
  const markets = Array.isArray(pack.markets) ? pack.markets : [];
  const econ = pack.econ && typeof pack.econ === 'object' ? pack.econ : {};
  const yearsForward = Number.isFinite(Number(pack.yearsForward))
    ? Number(pack.yearsForward)
    : Number.isFinite(Number(story.yearsForward))
      ? Number(story.yearsForward)
      : 0;
  const baselineDay = normalizeDay(story.day) || formatDay();
  const baselineYear = baselineDay.slice(0, 4) || '2026';
  const editionDate = pack.editionDate || 'the target date';
  const seed = story.storyId || `${story.section}|${story.angle || ''}|${story.rank || ''}|${editionDate}`;
  const angle = String(story.angle || 'impact');
  const curation = story && story.curation && typeof story.curation === 'object' ? story.curation : null;
  const futureEventSeedRaw = String(curation?.futureEventSeed || '')
    .replace(/\s+/g, ' ')
    .trim();

  // ── SHORT-CIRCUIT: If Opus curated a full draft, use it directly ──
  const draftBody = curation?.draftArticle?.body || curation?.draftBody || '';
  if (draftBody && draftBody.length > 120) {
    return draftBody;
  }

  // ── SHORT-CIRCUIT: If Opus provided sparkDirections + futureEventSeed, build a
  //    short holding article from the real topic instead of generic templates. ──
  const sparkDirections = String(curation?.sparkDirections || '').trim();
  if (futureEventSeedRaw && sparkDirections && futureEventSeedRaw.length > 20) {
    const topicLabel = String(topic.label || story.topicLabel || '').replace(/\s+/g, ' ').trim();
    const targetYear = Number(baselineYear) + yearsForward;
    const sourceLines = citations.slice(0, 5).map((c) => {
      const url = String(c.url || '').trim();
      const title = String(c.title || '').replace(/\s+/g, ' ').trim();
      return url ? `${title} — ${url}` : title;
    }).filter(Boolean);

    const lines = [];
    lines.push(futureEventSeedRaw.endsWith('.') ? futureEventSeedRaw : `${futureEventSeedRaw}.`);
    lines.push('');
    if (sparkDirections) {
      lines.push(sparkDirections);
      lines.push('');
    }
    if (topicLabel) {
      lines.push(`This story traces back to signals first visible in ${baselineYear}, when coverage centered on: ${topicLabel}.`);
      lines.push('');
    }
    lines.push(`Full article will render when opened. Click to read the complete ${targetYear} report.`);
    if (sourceLines.length) {
      lines.push('');
      lines.push('Sources');
      lines.push('');
      lines.push(sourceLines.join('\n'));
    }
    return lines.join('\n');
  }

  const anchor = citations[0] || null;
  const anchorTitle = stripAuthorSuffix(anchor?.title || topic.label || '');
  const anchorSource = String(anchor?.source || '').replace(/\s+/g, ' ').trim();
  const anchorSummary = String(anchor?.summary || '').replace(/\s+/g, ' ').trim();

  let theme = String(topic.theme || '').trim();
  if (!theme) {
    theme = cleanTheme(topic.label || anchorTitle) || cleanTheme(anchorTitle) || 'a major shift';
  }
  theme = String(theme || '')
    .split(/[.?!:;]+/)[0]
    .replace(/[“”"]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  const copula = theme.match(/^(.+?)\s+(?:was|is|are|were|has been|have been)\s+/i);
  if (copula) {
    theme = String(copula[1] || '').trim();
  }
  if (theme.length > 92) {
    theme = theme.slice(0, 92).replace(/[,\s;:.]+$/g, '').trim();
  }
  if (!theme) theme = 'a major shift';
  const placeHint = extractPlaceHint(`${anchorTitle} ${anchorSummary}`) || extractPlaceHint(String(topic.label || '')) || '';

  const baselineYearNum = Number(baselineYear);
  const targetYear = Number.isFinite(baselineYearNum) ? String(baselineYearNum + yearsForward) : '';

  const defaultDatelineBySection = {
    'U.S.': ['Washington', 'New York', 'Chicago', 'Atlanta', 'Phoenix', 'Austin'],
    World: ['Geneva', 'London', 'Brussels', 'Beijing', 'Nairobi', 'New Delhi'],
    Business: ['New York', 'London', 'Frankfurt', 'Singapore'],
    Technology: ['San Francisco', 'Seattle', 'Austin', 'Shenzhen'],
    Arts: ['New York', 'Los Angeles', 'London', 'Paris'],
    Lifestyle: ['Los Angeles', 'Miami', 'Brooklyn', 'Austin'],
    Opinion: ['New York', 'Washington']
  };
  const fallbackPlaces = defaultDatelineBySection[story.section] || ['New York'];
  const hintMatch = fallbackPlaces.find((p) => String(p).toLowerCase() === String(placeHint || '').toLowerCase()) || '';
  const dateline = hintMatch || pickStable(fallbackPlaces, `${seed}|dateline`) || '';
  const datelinePrefix = dateline ? `${dateline} — ` : '';

  const eventNameTemplates = [
    'The Stability Framework',
    'The Implementation Compact',
    'The Audit-Ready Rulebook',
    'The Standard Timelines Program',
    'The Permanent Playbook'
  ];
  const eventName = pickStable(eventNameTemplates, `${seed}|eventName`) || eventNameTemplates[0];

  const actorByAngle = {
    policy: ['federal officials', 'lawmakers', 'a federal appeals court', 'state officials', 'agency leaders'],
    markets: ['executives', 'investors', 'auditors', 'insurers', 'dealmakers'],
    tech: ['engineers', 'vendors', 'regulators', 'standards bodies', 'security teams'],
    society: ['organizers', 'local leaders', 'voters', 'school boards', 'community groups'],
    impact: ['local officials', 'employers', 'clinics', 'schools', 'courts']
  };
  const actorPool = actorByAngle[angle] || actorByAngle.impact;
  const actor = pickStable(actorPool, `${seed}|actor`) || actorPool[0];
  const actorCap = actor ? `${actor.charAt(0).toUpperCase()}${actor.slice(1)}` : actor;

  const ledeTemplates = {
    policy: [
      ({ prefix, actorCap, eventName, theme, targetYear }) =>
        `${prefix}${actorCap} unveiled ${eventName}, a durable rewrite of how the government handles ${theme} after years of churn${targetYear ? ` leading into ${targetYear}` : ''}.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}A legal showdown ended with ${eventName}, forcing agencies to trade discretion for timelines, paperwork, and court-tested procedures tied to ${theme}.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}${eventName} passed after a series of near-misses, narrowing gray zones around ${theme} while opening new fights over enforcement and oversight.`
    ],
    markets: [
      ({ prefix, actorCap, eventName, theme }) =>
        `${prefix}${actorCap} are now budgeting for ${eventName}, treating ${theme} less like a shock and more like a permanent cost and timing constraint.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}A wave of restructurings followed ${eventName} as firms that bet early on the new reality of ${theme} began consolidating their advantage.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}${eventName} reshaped contracts tied to ${theme}, pushing uncertainty out of headlines and into compliance clauses and pricing models.`
    ],
    tech: [
      ({ prefix, eventName, theme }) =>
        `${prefix}${eventName} moved from pilots to infrastructure, turning arguments about ${theme} into a fight over error rates, redress, and accountability.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}Tools built around ${theme} hit scale under ${eventName}, shifting the debate from whether to adopt to who pays when systems fail.`,
      ({ prefix, actorCap, eventName, theme }) =>
        `${prefix}${actorCap} pushed out a standards-driven stack tied to ${theme}, and the first audits are already exposing the tradeoffs it hard-codes.`
    ],
    society: [
      ({ prefix, eventName, theme }) =>
        `${prefix}${eventName} redrew the politics around ${theme}, hardening coalitions and moving the fight from slogans to everyday compliance and trust.`,
      ({ prefix, eventName }) =>
        `${prefix}After ${eventName}, the argument became more local and more durable: school boards, employers, courts, and communities absorbing the daily consequences.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}${theme} outlived the outrage cycle. ${eventName} made it a civic routine, and routines are harder to reverse than headlines.`
    ],
    impact: [
      ({ prefix, eventName, theme }) =>
        `${prefix}${eventName} is changing daily life around ${theme} in ways that are quieter than they were in ${baselineYear}, but more permanent.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}The institutions closest to the public reorganized under ${eventName}, turning volatility into routines tied to ${theme}.`,
      ({ prefix, eventName, theme }) =>
        `${prefix}What began as a dispute over ${theme} has become a services problem under ${eventName}: staffing, triage, and the slow work of making rules usable.`
    ]
  };

  const ledeTemplate = pickStable(ledeTemplates[angle] || ledeTemplates.impact, `${seed}|lede`) || ledeTemplates.impact[0];
  let lede = typeof ledeTemplate === 'function'
    ? ledeTemplate({ prefix: datelinePrefix, actorCap, eventName, theme, targetYear })
    : `${datelinePrefix}${String(ledeTemplate || '').trim()}`;

  if (futureEventSeedRaw) {
    const clipped = futureEventSeedRaw.length > 260 ? futureEventSeedRaw.slice(0, 260).replace(/[,\s;:.]+$/g, '').trim() : futureEventSeedRaw;
    const punct = /[.?!]$/.test(clipped) ? '' : '.';
    lede = `${datelinePrefix}${clipped}${punct}`.trim();
  }

  const kernel = citations
    .slice(0, 2)
    .map((c) => ({
      id: c.id || '',
      title: stripAuthorSuffix(String(c.title || '')).replace(/\s+/g, ' ').trim(),
      summary: String(c.summary || '').replace(/\s+/g, ' ').trim()
    }))
    .filter((c) => c.title || c.summary);

  const firstSentence = (text, maxLen = 220) => {
    const raw = String(text || '').replace(/\s+/g, ' ').trim();
    if (!raw) return '';
    const head = raw.split(/[.?!]\s+/)[0].trim();
    if (!head) return '';
    if (head.length <= maxLen) return head.replace(/[.?!]\s*$/g, '').trim();
    return head.slice(0, maxLen).replace(/[,\s;:.]+$/g, '').trim();
  };
  const kernelLine = kernel[0]?.summary || kernel[0]?.title || '';
  const kernelLine2 = kernel[1]?.summary || kernel[1]?.title || '';
  const kernelSentence = firstSentence(kernelLine, 220);
  const kernelSentence2 = firstSentence(kernelLine2, 220);

  const evidenceMosaic = (() => {
    if (!citations.length) return '';
    const items = citations
      .slice(0, 5)
      .map((c) => {
        const source = String(c.source || '').replace(/\s+/g, ' ').trim();
        const title = stripAuthorSuffix(String(c.title || '')).replace(/[“”"]/g, '').replace(/\s+/g, ' ').trim();
        const raw = c.summary ? String(c.summary) : title;
        const snippet = firstSentence(raw, 170).replace(/[“”"]/g, '').trim();
        if (!snippet) return null;
        return { source, title, snippet };
      })
      .filter(Boolean);
    if (items.length < 3) return '';

    const s1 = items[0];
    const s2 = items[1];
    const s3 = items[2];
    const s4 = items[3] || null;

    const bits = [];
    bits.push(
      `The ${baselineYear} inputs were scattered across outlets and institutions. One thread, ${s1.source ? `tracked by ${s1.source},` : 'tracked closely,'} was straightforward: ${s1.snippet}.`
    );
    bits.push(
      `Another signal pointed to the same constraint from a different angle: ${s2.snippet}${s2.source ? ` (${s2.source}).` : '.'}`
    );
    bits.push(
      `A third, more technical line suggested the change would be hard to reverse once systems were built: ${s3.snippet}${s3.source ? ` (${s3.source}).` : '.'}`
    );
    if (s4) {
      bits.push(
        `And by the end of ${baselineYear}, the pattern was broadening beyond a single headline: ${s4.snippet}${s4.source ? ` (${s4.source}).` : '.'}`
      );
    }
    bits.push(
      `None of it guaranteed the outcome now unfolding. But together, the signals narrowed the political options and made a stability-first framework operationally attractive to people tasked with running the system.`
    );

    return bits.join(' ');
  })();

  const nutGrafByAngle = {
    policy: [
      () =>
        `Under ${eventName}, agencies must follow standardized timelines, publish metrics, and provide an appeals track. Officials say it reduces chaos; critics say it hardens enforcement and hides the sharpest edges behind paperwork.`,
      () =>
        `${eventName} turns the debate into implementation: budgets, staffing, procurement, and the lawsuits that follow.`,
      () =>
        `The new rules trade discretion for documentation, with courts and auditors now effectively setting the boundaries of what counts as compliant.`
    ],
    markets: [
      () =>
        `The change is showing up in contracts: more reporting, higher insurance costs, and a new layer of compliance vendors selling certainty as a service.`,
      () =>
        `For companies, the question is no longer whether this is temporary. It is how much to spend to make it routine and defensible.`,
      () =>
        `Uncertainty has not disappeared. It has moved: from surprise disruptions into line items, audit trails, and clauses that decide who absorbs the risk.`
    ],
    tech: [
      () =>
        `The framework mandates logging, audits, and redress, pushing technical teams to treat failure modes as governance problems rather than edge cases.`,
      () =>
        `As the tools scale, error rates matter more than demos, and appeals matter more than marketing.`,
      () =>
        `Standards, procurement, and oversight now shape the technology as much as innovation does.`
    ],
    society: [
      () =>
        `The conflict has moved into local institutions: schools, employers, courts, and city agencies making daily calls that used to be political abstractions.`,
      () =>
        `Coalitions have adapted to the new incentives. The loudest fights remain symbolic, but the decisive ones are administrative.`,
      () =>
        `Once routines form, politics follows. That is why the next backlash is likely to be about process, not principle.`
    ],
    impact: [
      () =>
        `The biggest changes are practical: staffing, triage, and the slow work of making rules usable at the front desk.`,
      () =>
        `The shift is quieter than a headline, but harder to undo because it lives in systems that keep running even when the news cycle moves on.`,
      () =>
        `In the short term, the new routines create backlogs and edge cases. In the long term, they create a new definition of “normal” that is hard to unwind without a shock.`
    ]
  };
  const nutPool = nutGrafByAngle[angle] || nutGrafByAngle.impact;
  const nutGrafTemplate = pickStable(nutPool, `${seed}|nut`) || nutPool[0];
  const nutGraf = typeof nutGrafTemplate === 'function' ? nutGrafTemplate() : String(nutGrafTemplate || '');

  const anchorShort = String(anchorTitle || '').split(/[.?!:;]+/)[0].trim();
  const anchorShortClean = String(anchorShort || '')
    .replace(/[“”"]/g, '')
    .replace(/\s+/g, ' ')
    .trim();
  const anniversaryLine = yearsForward > 0 && anchorShortClean
    ? `It arrives near the ${yearsForward === 1 ? 'first' : ordinal(yearsForward)} anniversary of ${anchorSource ? `${anchorSource}'s` : `${baselineYear}`} headline "${anchorShortClean}".`
    : '';
  const baselineTemplates = [
    ({ baselineYear, kernelSentence, kernelSentence2, theme, eventName }) => {
      const signalLine = kernelSentence
        ? `In ${baselineYear}, early warning signs were visible: ${kernelSentence}${kernelSentence2 ? `; ${kernelSentence2}.` : '.'}`
        : `In ${baselineYear}, the first clear signals appeared across news coverage, research, and market indicators.`;
      const ann = anniversaryLine ? `${anniversaryLine} ` : '';
      return `${ann}${signalLine} Over time, those threads hardened into constraints that made ${eventName} plausible.`;
    },
    ({ baselineYear, anchorShortClean, theme }) => {
      if (!anchorShortClean) {
        return `Back in ${baselineYear}, the story was volatility. In the years since, the story became governance: who builds systems around ${theme}, and who pays for their failures.`;
      }
      const src = anchorSource ? `${anchorSource}` : `${baselineYear} coverage`;
      const ann = anniversaryLine ? `${anniversaryLine} ` : '';
      return `${ann}Back in ${baselineYear}, ${src} captured the volatility in headlines like "${anchorShortClean}." The future version is less dramatic, but more durable: implementation, audits, and the slow grind of institutions adapting.`;
    }
  ];
  const baselineTemplate = pickStable(baselineTemplates, `${seed}|baseline`) || baselineTemplates[0];
  const baseline = baselineTemplate({ baselineYear, kernelSentence, kernelSentence2, anchorShortClean, theme, eventName });

  const timelineByAngle = {
    policy: [
      () =>
        `The framework did not appear overnight. After ${baselineYear}, agencies tried a patchwork of guidance and exceptions, only to see it challenged case by case. Over time, the same pressure points kept resurfacing: inconsistent timelines, unclear standards for evidence, and decisions that could not be defended when reviewed.`,
      () =>
        `Officials involved in the rollout say ${baselineYear} forced an uncomfortable realization: the old system relied on discretion that could not scale. The intervening years became a slow negotiation between courts, agencies, and lawmakers over what would be allowed to persist.`
    ],
    markets: [
      () =>
        `The change has been gradual. First came the volatility of ${baselineYear}, then a wave of contingency planning, then a growing ecosystem of vendors promising audit-ready workflows. By the time the framework was formalized, many companies had already built their own version of it.`,
      () =>
        `Executives say the years between ${baselineYear}'s disruptions and today were defined by recalibration. Hiring slowed in exposed areas, contracts became heavier, and compliance moved from a specialist concern to an operating constraint that shaped strategy.`
    ],
    tech: [
      () =>
        `The arc has been familiar: pilots, then procurement, then audit. Early systems optimized for speed and coverage, but ${baselineYear}'s backlash shifted priorities toward governance. The result is tooling that is less flashy and more disciplined, designed to produce explanations as reliably as outcomes.`,
      () =>
        `In the years after ${baselineYear}, agencies and vendors learned that accuracy was not enough. They needed traceability, monitoring, and repair mechanisms. The framework is the institutionalization of those lessons.`
    ],
    society: [
      () =>
        `Over time, the politics followed the process. Coalitions that once fought over slogans began fighting over levers: budgets, staffing, and the rules for appeal. ${baselineYear}'s volatility did not disappear; it became the rationale for standardization.`,
      () =>
        `Community groups say the years since ${baselineYear} were marked by adaptation. People learned the workarounds, then watched them get closed. The framework replaces some arbitrary choices with documented ones, a trade critics say can still produce unfairness.`
    ],
    impact: [
      () =>
        `For institutions on the ground, the story has been a shift from emergency response to standing operations. Temporary measures turned into permanent desks. Ad hoc decisions turned into checklists. And checklists, once adopted, tend to outlast the people who adopted them.`,
      () =>
        `Local leaders describe the years after ${baselineYear} as an era of improvisation followed by fatigue. The framework is a response to that fatigue, an attempt to create a routine that does not collapse under attention.`
    ]
  };
  const timelinePool = timelineByAngle[angle] || timelineByAngle.impact;
  const timelineTemplate = pickStable(timelinePool, `${seed}|timeline`) || timelinePool[0];
  const timeline = typeof timelineTemplate === 'function' ? timelineTemplate() : String(timelineTemplate || '');

  const secondParaByAngle = {
    policy: [
      () =>
        `The immediate impact is procedural: new filing windows, standardized checklists, and a published cadence for audits. Agency leaders say the goal is to cut down on emergency exceptions that collapsed under litigation in ${baselineYear}.`,
      () =>
        `In the near term, the changes are being measured in backlogs and court dockets, not in speeches. A senior official described the framework as "an attempt to make the process boring again," meaning predictable enough to withstand the next headline cycle.`
    ],
    markets: [
      () =>
        `For businesses, the shift is showing up as a calendar and a cost. Legal teams are reworking templates, insurers are updating underwriting questions, and compliance vendors are marketing automation as the only way to keep up.`,
      () =>
        `The framework has quickly become a negotiating term. In deals, executives say, the question is less whether a risk exists than which party is willing to document it, audit it, and absorb the penalty when it goes wrong.`
    ],
    tech: [
      () =>
        `The shift is technical as much as political. Logging requirements, auditability standards, and appeals workflows are forcing systems that once prioritized speed to prioritize explainability.`,
      () =>
        `Engineers describe the new standard as a move from "pilot logic" to "production accountability." That means fewer one-off exceptions, and more visible failure modes that can be challenged, measured, and corrected.`
    ],
    society: [
      () =>
        `The fight has become more local. Instead of arguing only about goals, communities are now arguing about process: who gets flagged, who gets a hearing, and how long it takes to get an answer.`,
      () =>
        `Supporters say the framework reduces chaos. Critics argue it codifies it, turning what was once discretionary into a routine that is harder to contest once it is operational.`
    ],
    impact: [
      () =>
        `For the institutions closest to the public, the change is operational. The work is less about emergency response and more about triage, staffing, and rules that can survive the next lawsuit.`,
      () =>
        `The ripple effects are uneven. Well-staffed jurisdictions have begun adapting quickly. Others are already reporting new delays as the paper trail grows and the margin for improvisation shrinks.`
    ]
  };
  const secondParaTemplate = pickStable(secondParaByAngle[angle] || secondParaByAngle.impact, `${seed}|p2`) || (secondParaByAngle.impact[0]);
  const secondPara = typeof secondParaTemplate === 'function' ? secondParaTemplate() : String(secondParaTemplate || '');

  const mechanicsByAngle = {
    policy: [
      () =>
        `${eventName} creates a two-track system: a fast lane for routine cases and a slower, heavily documented lane for exceptions. The intent is to reduce discretionary swings without eliminating discretion entirely, a compromise shaped by ${baselineYear}'s litigation.`,
      () =>
        `Under the framework, agencies must publish performance metrics, maintain an appeals log, and demonstrate that automated decisions can be reviewed by a human with authority to reverse them. Courts, in effect, become a secondary design partner.`
    ],
    markets: [
      () =>
        `The mechanics are simple but costly. Every step in the process now produces documentation that can be audited later, which means more tooling, more outside counsel, and more time spent proving compliance instead of pursuing growth.`,
      () =>
        `Executives describe the new environment as "audit-first." It rewards companies that can standardize workflows and penalizes those that rely on informal judgment or bespoke arrangements that do not survive scrutiny.`
    ],
    tech: [
      () =>
        `Technically, the framework is a stack: identity, verification, logging, and redress. The hard part is not building the components; it is integrating them across agencies and vendors without creating new failure points or privacy exposure.`,
      () =>
        `The new standard elevates the unglamorous work: data quality, monitoring, and incident response. The most expensive errors are no longer bugs, but decisions that cannot be explained or appealed.`
    ],
    society: [
      () =>
        `On the ground, the framework turns political conflict into an administrative one. That change matters because administrative fights are won by capacity: time, documentation, and the ability to keep showing up.`,
      () =>
        `The rules also change who takes the blame. When decisions are routed through a standardized process, leaders can point to compliance. Critics say that is precisely the problem: it narrows accountability to whether a box was checked.`
    ],
    impact: [
      () =>
        `At the operational level, the framework functions like a compliance operating system. It standardizes intake, defines what counts as evidence, and requires a trail of decisions that can be audited later. The result is less improvisation, but also less flexibility when reality refuses to fit the templates.`,
      () =>
        `The systems being built now are not temporary. They are designed to persist across administrations and news cycles, which is why the debates have moved from whether to act to what gets encoded into the process. Once encoded, the policy becomes harder to see and harder to change.`
    ]
  };
  const mechanicsTemplate = pickStable(mechanicsByAngle[angle] || mechanicsByAngle.impact, `${seed}|mech`) || mechanicsByAngle.impact[0];
  const mechanics = typeof mechanicsTemplate === 'function' ? mechanicsTemplate() : String(mechanicsTemplate || '');

  const implementationByAngle = {
    policy: [
      () =>
        `The rollout is designed to be auditable from day one. Agencies are required to publish guidance, train staff on the new timelines, and keep an appeals ledger that can be sampled by inspectors and courts. Officials say that is the lesson of ${baselineYear}: the policy is whatever survives review.`,
      () =>
        `Implementation will be the real test. The framework depends on staffing and training, and it assumes the data entering the system is clean enough to be reviewed. In places where the inputs are messy, the process will slow, and delays will effectively become part of the policy.`
    ],
    markets: [
      () =>
        `Companies are adjusting with the tools they know: budgets, vendors, and contracts. A growing share of compliance work is being automated, executives say, not because automation is perfect but because manual review does not scale when documentation requirements multiply.`,
      () =>
        `The new environment also changes hiring and investment timelines. Projects that cannot be documented cleanly are being delayed, while firms that can standardize workflows are accelerating, turning compliance into a competitive advantage rather than a tax.`
    ],
    tech: [
      () =>
        `Technologists say the hard part is stitching together accountability. Logs must be complete, decisions must be reproducible, and appeals must be tracked end-to-end. That requires integration across vendors and agencies that historically did not share standards, let alone data.`,
      () =>
        `Audits are already shaping design choices. Teams are prioritizing explainability, monitoring, and rollback paths, even when that makes systems slower. The new metric is not raw throughput; it is whether a mistake can be found, explained, and corrected in time to matter.`
    ],
    society: [
      () =>
        `Local institutions are adapting in ways that rarely make national headlines. Schools, employers, clinics, and courts are building standing playbooks, and those playbooks often decide outcomes long before a case reaches a decision-maker with discretion.`,
      () =>
        `The framework also changes civic incentives. When the process becomes standardized, participation shifts toward the people who can navigate it repeatedly. Critics argue that makes power quieter, not smaller.`
    ],
    impact: [
      () =>
        `For frontline workers, the new framework means more steps, more logging, and fewer informal shortcuts. That can reduce chaotic swings, but it also increases the time it takes to handle hard cases, and hard cases are where the system is judged. Managers say the quickest way to break the new process is to starve it of staff.`,
      () =>
        `Leaders describe the goal as "durable administration": rules that can be executed with ordinary staffing. But ordinary staffing is in short supply, and the short supply will determine whether the framework feels stabilizing or simply slow. The early weeks are expected to be the roughest as new training and new forms collide with old backlogs.`
    ]
  };
  const implementationTemplate = pickStable(implementationByAngle[angle] || implementationByAngle.impact, `${seed}|impl`) || implementationByAngle.impact[0];
  const implementation = typeof implementationTemplate === 'function' ? implementationTemplate() : String(implementationTemplate || '');

  const stakeholdersByAngle = {
    policy: [
      () =>
        `The coalition behind the framework is uneasy. Some supporters want stricter enforcement; others want stricter guardrails. What unites them is a preference for predictability: a process that produces the same result for the same inputs, and an appeals track that is legible enough to defend.`,
      () =>
        `In hearings and closed-door meetings, the argument has shifted from goals to mechanics. Agencies want flexibility. Courts want consistency. Advocates want timelines that are enforceable. The framework is a compromise that satisfies no one completely, which is partly why it might last.`
    ],
    markets: [
      () =>
        `Industry groups have largely embraced the idea of standardization, even as they complain about costs. Insurers and auditors prefer it because it makes risk measurable. Smaller firms fear it because measurable risk can quickly become a barrier to entry.`,
      () =>
        `The strongest backers are often the most operationally mature organizations. They can absorb documentation requirements, train staff, and build systems. Their critics argue that this is precisely the problem: standardization can entrench incumbency.`
    ],
    tech: [
      () =>
        `Vendors and regulators are now locked into a feedback loop. Audits shape design. Design shapes what audits can measure. Privacy advocates warn that the loop can normalize surveillance, while engineers argue that without instrumentation there is no accountability.`,
      () =>
        `The most contentious debates are not about whether to build the tools, but about governance: who gets access, who can challenge a decision, and what happens when an error is discovered at scale.`
    ],
    society: [
      () =>
        `Community response has been split. Some groups see the framework as a chance to make invisible harm visible by forcing documentation. Others see it as a way to proceduralize harm and make it harder to contest in public.`,
      () =>
        `The people most affected are often the least represented in the rooms where the process is designed. That mismatch, critics say, is how a system can be stable and still be unjust.`
    ],
    impact: [
      () =>
        `On the ground, the framework has created new roles and new frictions. Institutions are hiring coordinators, building intake desks, and learning to speak in the language of documentation. The risk, critics say, is that the language becomes the reality.`,
      () =>
        `Supporters argue the process is at least visible. Critics argue visibility is not enough if people cannot navigate the process without time, transportation, and help.`
    ]
  };
  const stakeholdersPool = stakeholdersByAngle[angle] || stakeholdersByAngle.impact;
  const stakeholdersTemplate = pickStable(stakeholdersPool, `${seed}|stake`) || stakeholdersPool[0];
  const stakeholders = typeof stakeholdersTemplate === 'function' ? stakeholdersTemplate() : String(stakeholdersTemplate || '');

  const detailsByAngle = {
    policy: [
      () =>
        `A central dispute is how much discretion remains once the rules are written down. The framework preserves an exceptions lane, but it requires those exceptions to be justified and logged, which officials say is necessary for legitimacy. Critics say the exceptions lane will become a privilege lane, exercised more easily by people with counsel and time.`,
      () =>
        `The most technical fights are about definitions. What counts as sufficient evidence? Which timelines are enforceable? What documentation must be provided to make an appeal meaningful? These questions rarely trend on social media, but they are where systems become humane or punitive.`
    ],
    markets: [
      () =>
        `The compliance layer is not neutral. It changes incentives, pulling resources toward documentation, risk management, and systems that can survive third-party review. Some executives welcome that shift, arguing it prevents the worst outcomes; others warn it drains capital from innovation and favors the already-large.`,
      () =>
        `The framework also reshapes competition among vendors. Tools that can prove what happened and when are winning budgets. Tools that only promise better outcomes without accountability are facing sharper scrutiny from auditors and boards.`
    ],
    tech: [
      () =>
        `The governance questions are now engineering questions. Which models can be explained? How do you log decisions without leaking sensitive data? How do you handle redress when downstream systems have already acted on an upstream error? The answers determine whether accountability is real or performative.`,
      () =>
        `Teams are also wrestling with measurement. A low error rate can hide concentrated harm if the errors fall on the same communities. That is why advocates are pushing for transparency not only on accuracy, but on appeals and reversals.`
    ],
    society: [
      () =>
        `Process can be a form of power. A system that is legible to insiders but opaque to everyone else can still claim neutrality while producing predictable winners and losers. That is the core fear among critics who see the framework as a way to stabilize outcomes without debating them.`,
      () =>
        `Supporters argue that documentation is a step toward accountability, not away from it. Their case is that what can be measured can be challenged, and what can be challenged can be improved. The question is whether the people harmed can access the challenge.`
    ],
    impact: [
      () =>
        `The day-to-day hinge is the appeals queue. If appeals are fast and meaningful, officials argue, mistakes become correctable. If appeals are slow, advocates argue, the system becomes a machine for delay, and delay becomes an outcome in itself.`,
      () =>
        `The other hinge is staffing. Frameworks assume people to run them. When staffing is thin, the rules can become an excuse to do less, slower, and with less transparency than the rhetoric suggests.`
    ]
  };
  const detailsPool = detailsByAngle[angle] || detailsByAngle.impact;
  const detailsTemplate = pickStable(detailsPool, `${seed}|details`) || detailsPool[0];
  const details = typeof detailsTemplate === 'function' ? detailsTemplate() : String(detailsTemplate || '');

  const sectionContextBySection = {
    'U.S.': [
      ({ theme }) =>
        `Politically, the shift has been to treat ${theme} as an administrative problem rather than a campaign slogan. That does not make it less contested. It changes who has leverage: the people who write procurement language, allocate staff, and decide how strictly a rule is interpreted when the cameras are gone.`
    ],
    World: [
      ({ theme }) =>
        `In the world view, the stakes are coordination and legitimacy. Systems that depend on shared definitions and shared timelines tend to break at borders, where laws and capacities diverge. The framework’s promise is order; its risk is bottlenecks that push decisions into the shadows.`
    ],
    Business: [
      ({ theme }) =>
        `For business, the biggest change is predictability. A predictable constraint can be priced, hedged, and engineered around. An unpredictable constraint becomes a shock. The framework aims to turn the shock into a cost, and costs shape strategy.`
    ],
    Technology: [
      ({ theme }) =>
        `For technology, the moment is a reminder that governance is a product requirement. Audit logs, appeals, and rollback paths are no longer "nice to have." They are the features that decide whether a system is allowed to operate at scale.`
    ],
    Arts: [
      ({ theme }) =>
        `For arts and culture, the question is not only funding. It is process: which institutions have the capacity to comply, document, and appeal. In practice, administrative friction can become a quiet form of censorship, even when no one calls it that.`
    ],
    Lifestyle: [
      ({ theme }) =>
        `For households, the change is felt as friction. A system that is stable can still be exhausting if it demands time, forms, and repeated proof. The burden falls hardest where time is scarce and help is expensive.`
    ],
    Opinion: [
      ({ theme }) =>
        `The deeper question is whether stability is being used as a substitute for justice. A process can be coherent, measurable, and durable, and still be wrong. The danger is that durability can be mistaken for legitimacy.`
    ]
  };
  const sectionContextPool = sectionContextBySection[story.section] || sectionContextBySection['U.S.'];
  const sectionContextTemplate = pickStable(sectionContextPool, `${seed}|sect`) || sectionContextPool[0];
  const sectionContext = typeof sectionContextTemplate === 'function' ? sectionContextTemplate({ theme }) : String(sectionContextTemplate || '');

  const vignetteBySection = {
    'U.S.': [
      ({ dateline }) =>
        `In ${dateline || 'one city'}, administrators say the new routines have changed the tempo of the work. Meetings are longer, checklists are stricter, and the definition of "done" has shifted from resolving a case to leaving a trail that can be defended later.`
    ],
    World: [
      ({ dateline }) =>
        `In ${dateline || 'one regional hub'}, officials say the framework is forcing coordination across agencies that previously worked in parallel. The coordination reduces surprises, they say, but it can also widen bottlenecks when any one link in the chain is understaffed.`
    ],
    Business: [
      ({ dateline }) =>
        `In ${dateline || 'boardrooms'}, executives describe a new kind of caution. Deals are being structured around compliance milestones, and "audit readiness" has become as common a phrase as revenue forecasts.`
    ],
    Technology: [
      ({ dateline }) =>
        `In ${dateline || 'engineering teams'}, the emphasis is on systems that fail loudly and recover quickly. The new expectation is that every critical decision can be traced, replayed, and appealed, even when that adds cost and complexity.`
    ],
    Arts: [
      ({ dateline }) =>
        `In ${dateline || 'cultural institutions'}, leaders say the uncertainty of ${baselineYear} reshaped funding and programming. The new framework is not directly about art, they say, but its paperwork and timelines determine which projects are viable.`
    ],
    Lifestyle: [
      ({ dateline }) =>
        `In ${dateline || 'everyday life'}, the changes are subtle: new forms, new waits, and new rules that show up as friction. For some households, the process is manageable. For others, it is another barrier layered onto an already thin schedule.`
    ],
    Opinion: [
      ({ dateline }) =>
        `The argument is not only about policy outcomes. It is about the system that produces them. A framework that is durable can be either a safeguard or a trap, depending on who can navigate it and who cannot.`
    ]
  };
  const vignettePool = vignetteBySection[story.section] || vignetteBySection['U.S.'];
  const vignetteTemplate = pickStable(vignettePool, `${seed}|vignette`) || vignettePool[0];
  const vignette = typeof vignetteTemplate === 'function' ? vignetteTemplate({ dateline }) : String(vignetteTemplate || '');

  const metricsByAngle = {
    policy: [
      () =>
        `Officials say success will be measured in the boring metrics that used to be afterthoughts: time-to-decision, appeal resolution times, and the share of cases reversed on review. Critics argue those dashboards can hide as much as they reveal if people fall out of the process before they ever show up in the numbers.`
    ],
    markets: [
      () =>
        `In the private sector, the early scorekeeping is already underway. Firms are tracking compliance spend, audit outcomes, and cycle time the way they once tracked marketing efficiency. That may reward discipline, executives say, but it also risks turning the process into a pay-to-play advantage.`
    ],
    tech: [
      () =>
        `The new standard also changes what gets optimized. Teams are measuring false positives, appeal rates, and time-to-correction, not just throughput. In audits, engineers say, the most damaging failure is a decision that cannot be reproduced.`
    ],
    society: [
      () =>
        `The metrics will be political as much as technical. Participation rates, complaint volumes, and the geography of delays will be read as a referendum on legitimacy. Community leaders warn that slow harm often looks like silence in official reports.`
    ],
    impact: [
      () =>
        `The first signals of success or failure will be practical. How long do people wait? How often do cases get kicked back for missing documentation? How many staff hours are consumed by review and appeals? Those questions will determine whether the framework is experienced as stability or as gridlock.`
    ]
  };
  const metricsPool = metricsByAngle[angle] || metricsByAngle.impact;
  const metricsTemplate = pickStable(metricsPool, `${seed}|metrics`) || metricsPool[0];
  const metricsPara = typeof metricsTemplate === 'function' ? metricsTemplate() : String(metricsTemplate || '');

  const failureModesByAngle = {
    policy: [
      () =>
        `The failure modes are familiar to anyone who has watched a process become a system. Edge cases pile up. Definitions drift. Timelines slip. And once a backlog exists, officials start making implicit policy decisions about which delays matter and which ones can be tolerated.`
    ],
    markets: [
      () =>
        `For companies, the failure mode is not only cost. It is rigidity. When compliance becomes the dominant constraint, organizations can become optimized for audit rather than outcome, and that can slow innovation even in areas where the risk is manageable.`
    ],
    tech: [
      () =>
        `For engineers, the hardest failures are the quiet ones: bad data that looks plausible, appeals that are logged but not acted on, and feedback loops that reinforce errors. The framework can expose those problems, but it can also generate so much instrumentation that accountability becomes noise.`
    ],
    society: [
      () =>
        `For communities, the failure mode is exclusion by process. A system can have an appeals track on paper and still be inaccessible in practice. The question is whether support exists for the people who need the process most: translation, assistance, transportation, time.`
    ],
    impact: [
      () =>
        `For frontline institutions, the failure mode is a widening gap between what the rules require and what capacity allows. That gap produces improvisation, and improvisation produces inconsistency. The framework is meant to reduce inconsistency, but without staffing it can simply relocate it.`
    ]
  };
  const failurePool = failureModesByAngle[angle] || failureModesByAngle.impact;
  const failureTemplate = pickStable(failurePool, `${seed}|fail`) || failurePool[0];
  const failureModes = typeof failureTemplate === 'function' ? failureTemplate() : String(failureTemplate || '');

  const adaptationByAngle = {
    policy: [
      () =>
        `Already, agencies are adapting by building "safe defaults": standardized decisions that minimize litigation risk. Advocates are adapting by targeting the bottlenecks, pushing for resources where the delays are longest. And lawmakers are adapting by treating oversight as a budget line, not a press release.`
    ],
    markets: [
      () =>
        `Businesses are adapting quickly. Some are investing in tooling and training. Others are narrowing scope, avoiding the hardest-to-document activities. A few are turning compliance into a product, selling infrastructure to competitors who can’t afford to build it.`
    ],
    tech: [
      () =>
        `Teams are adapting by designing for reversibility. They are building audit trails that can be queried, interfaces that explain decisions, and incident playbooks that treat errors as inevitable. The question is whether those safeguards remain intact once systems are under political pressure to "move faster."`
    ],
    society: [
      () =>
        `Communities are adapting, too. Mutual-aid groups are building navigation playbooks. Local officials are hiring coordinators. And critics are shifting focus from symbolic fights to procedural ones, because procedure is what survives when attention fades.`
    ],
    impact: [
      () =>
        `The adaptation is happening in small moves: extra staff in intake, new checklists, partnerships with local nonprofits, and quiet changes to how people are prioritized. Those moves can reduce harm. They can also create new inequities, depending on who has access to help.`
    ]
  };
  const adaptationPool = adaptationByAngle[angle] || adaptationByAngle.impact;
  const adaptationTemplate = pickStable(adaptationPool, `${seed}|adapt`) || adaptationPool[0];
  const adaptation = typeof adaptationTemplate === 'function' ? adaptationTemplate() : String(adaptationTemplate || '');

  const voicesByAngle = {
    policy: [
      () =>
        `"If it isn’t documented, it doesn’t exist in court," said one agency lawyer involved in drafting the rollout. "The whole point is to make the next challenge predictable."`,
      () =>
        `One advocate described the new system as "a rights fight that moved to the timeline." The appeals track exists, the advocate said, but only if people can survive the delays.`
    ],
    markets: [
      () =>
        `"The winners are the firms that can turn uncertainty into a checklist," said a compliance executive at a multinational. "The losers are the ones who treated this as temporary."`,
      () =>
        `A dealmaker involved in recent negotiations put it more bluntly: "Everyone agrees on the risk. The argument is who pays to prove they managed it."`
    ],
    tech: [
      () =>
        `"We used to measure success as throughput," said an engineer working on verification tooling. "Now we measure it as reversibility: how fast we can correct ourselves when we are wrong."`,
      () =>
        `A regulator involved in early audits said the hardest problems were not malicious abuse but edge cases: "Systems fail at the margins, and that’s where the harm concentrates."`
    ],
    society: [
      () =>
        `"People think the fight is over when a rule is published," said a local organizer. "It starts when the rule shows up at the counter and someone has to decide what it means."`,
      () =>
        `A community leader described the new routines as stabilizing for some and punishing for others: "Predictable doesn’t automatically mean fair."`
    ],
    impact: [
      () =>
        `"The paperwork used to be a byproduct," said a county administrator. "Now the paperwork is the product. Everything else has to fit around it."`,
      () =>
        `A clinic director who deals with the downstream effects said the system has become slower but more legible: "We can explain the steps. We can’t always change the outcome."`
    ]
  };
  const voiceTemplate = pickStable(voicesByAngle[angle] || voicesByAngle.impact, `${seed}|voice`) || voicesByAngle.impact[0];
  const voicePara = typeof voiceTemplate === 'function' ? voiceTemplate() : String(voiceTemplate || '');

  const indicatorsPara = (() => {
    const econKeys = Object.keys(econ || {});
    const hasEcon = econKeys.length > 0;
    const hasMarkets = markets.length > 0;
    if (!hasEcon && !hasMarkets) return '';

    const marketLine = hasMarkets
      ? `Prediction markets in ${baselineYear} repeatedly priced elevated uncertainty around timing and enforcement. The probabilities were inputs, not outcomes, but they influenced planning by keeping risk on the dashboard long after the headlines moved on.`
      : '';
    const econLine = hasEcon
      ? `Macro indicators in ${baselineYear} suggested limited slack for large-scale churn. That backdrop helped push decision-makers toward systems that can be scaled without improvisation, even when scaling creates friction.`
      : '';

    return [marketLine, econLine].filter(Boolean).join(' ');
  })();

  const pushbackByAngle = {
    policy: [
      ({ theme }) =>
        `Opponents say the framework’s real effect is to move the cost of compliance onto individuals and small institutions. They argue that even a well-designed appeals process can become a mirage if timelines stretch and assistance is scarce.`,
      ({ theme }) =>
        `Supporters counter that ${baselineYear} proved the old approach was not only volatile but brittle. In their view, a brittle system is cruel in a different way: it breaks in public, and people get caught in the breakage.`
    ],
    markets: [
      ({ theme }) =>
        `Critics argue the new compliance layer is an economic tax that favors incumbents: large firms can absorb audits and vendors, while smaller players face higher fixed costs and fewer degrees of freedom.`,
      ({ theme }) =>
        `Defenders say the costs are the price of predictability. "You can’t scale this on exception-handling," one executive said. "Either you standardize, or you keep paying for surprises."`
    ],
    tech: [
      ({ theme }) =>
        `Privacy advocates say the framework’s logging and verification requirements risk creating a permanent surveillance substrate. Proponents respond that without instrumentation there is no auditability, and without auditability there is no accountability.`,
      ({ theme }) =>
        `Engineers warn that "paper accountability" can coexist with real-world harm. A system can be auditable and still embed bias or fail at the margins where people have the least power to contest decisions.`
    ],
    society: [
      ({ theme }) =>
        `For critics, the risk is normalization. Once the routines are in place, the political energy required to change them rises dramatically, and harm becomes easier to ignore because it is procedural rather than spectacular.`,
      ({ theme }) =>
        `Supporters argue the opposite: routines create visibility. When everyone is forced into the same process, patterns and bottlenecks become legible, and so do the places where reform actually matters.`
    ],
    impact: [
      ({ theme }) =>
        `In practice, the consequences depend on capacity. Where staffing is strong, the process can be navigated. Where staffing is weak, the process becomes the problem, and delays become a policy choice in disguise.`,
      ({ theme }) =>
        `Even many supporters concede the framework will create new pain in the short run. The bet is that the pain becomes correctable: documented, appealed, measured, and eventually redesigned.`
    ]
  };
  const pushbackTemplate = pickStable(pushbackByAngle[angle] || pushbackByAngle.impact, `${seed}|push`) || pushbackByAngle.impact[0];
  const pushback = typeof pushbackTemplate === 'function' ? pushbackTemplate({ theme }) : String(pushbackTemplate || '');

  const nextBattlesByAngle = {
    policy: [
      () =>
        `The next fights are already forming around implementation: budgets, staffing, and the first wave of legal challenges. Courts will test whether the new timelines are real constraints or aspirational ones. Legislatures will test whether the oversight mechanisms remain funded once the spotlight fades.`
    ],
    markets: [
      () =>
        `Next, executives say, come the second-order effects: which sectors can absorb compliance overhead, which shift to vendors, and which exit. Disputes that once played out in public are likely to show up as contract conflicts, audit findings, and insurance terms.`
    ],
    tech: [
      () =>
        `Technologists expect the next round of conflict to be about standards. What gets audited, how often, and by whom will determine which approaches win. Security incidents or high-profile failures could trigger an abrupt tightening of requirements, reshaping the stack again.`
    ],
    society: [
      () =>
        `The next battles are likely to be local and procedural. Coalition energy will move toward school boards, city agencies, and courts where the daily decisions are made. And as the process becomes routine, the politics around it may become less dramatic and more durable.`
    ],
    impact: [
      () =>
        `The near-term test is whether capacity matches ambition. If staffing and training keep up, the framework can feel stabilizing. If they do not, the system can devolve into delay, with the most vulnerable paying the highest price for "order."`
    ]
  };
  const nextBattlesPool = nextBattlesByAngle[angle] || nextBattlesByAngle.impact;
  const nextBattlesTemplate = pickStable(nextBattlesPool, `${seed}|next`) || nextBattlesPool[0];
  const nextBattles = typeof nextBattlesTemplate === 'function' ? nextBattlesTemplate() : String(nextBattlesTemplate || '');

  const counterVoiceByAngle = {
    policy: [
      () =>
        `"A standardized process is not the same as a fair process," said an attorney who represents clients caught in administrative delays. "If you can’t afford to wait, you can’t afford your rights."`
    ],
    markets: [
      () =>
        `"We’re building a compliance economy," said a small-business owner who has been navigating the new requirements. "The forms don’t care how many people you have on staff."`
    ],
    tech: [
      () =>
        `"Auditability can turn into surveillance if you’re not careful," said a privacy researcher. "The question is whether the logs protect people or just make them legible."`
    ],
    society: [
      () =>
        `"Routines are power," said a community leader. "Once the routine is set, the people who can’t keep up disappear from the story."`
    ],
    impact: [
      () =>
        `"Stability is good," said a frontline supervisor. "But stability without capacity is just a longer line."`
    ]
  };
  const counterPool = counterVoiceByAngle[angle] || counterVoiceByAngle.impact;
  const counterTemplate = pickStable(counterPool, `${seed}|counter`) || counterPool[0];
  const counterVoice = typeof counterTemplate === 'function' ? counterTemplate() : String(counterTemplate || '');

  const outlookTemplates = [
    ({ theme }) =>
      `What happens next will depend less on new announcements and more on implementation capacity: staffing, training, oversight, and whether the system’s mistakes are visible enough to correct.`,
    ({ theme }) =>
      `The equilibrium is fragile. A major court ruling, a macro shock, or a security incident could destabilize the new regime and force a faster renegotiation of the rules around ${theme}.`,
    ({ theme }) =>
      `For now, the direction is clear: more documentation, more measurement, and more disputes over who gets to define "working" in a system built around ${theme}.`
  ];
  const outlookTemplate = pickStable(outlookTemplates, `${seed}|outlook`) || outlookTemplates[0];
  const outlook = typeof outlookTemplate === 'function' ? outlookTemplate({ theme }) : String(outlookTemplate || '');

  const closingTemplates = [
    ({ eventName }) =>
      `If ${baselineYear} was defined by volatility, this year is defined by process. ${eventName} may not end the fight, but it sets the terrain: a fight over capacity, documentation, and the right to appeal.`,
    ({ eventName }) =>
      `In the end, the framework’s success will be judged in mundane places: offices, call centers, dashboards, and the gap between what the rules promise and what people experience.`,
    ({ eventName }) =>
      `For now, the new playbook is in place. The next story will be whether it produces stability, or simply makes instability harder to see.`
  ];
  const closingTemplate = pickStable(closingTemplates, `${seed}|close`) || closingTemplates[0];
  const closing = typeof closingTemplate === 'function' ? closingTemplate({ eventName }) : String(closingTemplate || '');

  const conflictTemplates = {
    policy: [
      ({ theme }) =>
        `Supporters argue that the new regime reduces chaos. Critics say it hardens enforcement and moves the real fight into paperwork, where the burden falls on the people least able to navigate it.`,
      ({ theme }) =>
        `The next battles are already taking shape: which data sets count, which timelines are enforceable, and how courts handle inevitable errors once the system is scaled.`,
      ({ theme }) =>
        `Even advocates who agree on goals disagree on levers. Funding, reporting requirements, and vendor contracts are now where the argument over ${theme} will be decided.`
    ],
    markets: [
      ({ theme }) =>
        `The economic impact is showing up in budgets, not slogans: compliance overhead, delayed projects, and a faster push toward substitution where the rules are costly to navigate.`,
      ({ theme }) =>
        `The winners are the firms that can turn uncertainty into a checklist. The losers are the ones that treated ${theme} as a temporary disruption instead of a structural constraint.`,
      ({ theme }) =>
        `The volatility has not disappeared. It has moved. It is now priced into contracts, audits, and insurance terms rather than surprise disruptions.`
    ],
    tech: [
      ({ theme }) =>
        `As deployment scales, the arguments are getting more technical: false positives, appeals, security, and the quiet ways that a system can be “accurate” and still be unjust.`,
      ({ theme }) =>
        `Engineers call it instrumentation. Critics call it surveillance. Either way, the technical layer around ${theme} is now deciding outcomes in places where policy used to decide them.`,
      ({ theme }) =>
        `The fight is no longer about whether the tools exist. It is about governance: who audits them, who can challenge them, and what happens when they fail at scale.`
    ],
    society: [
      ({ theme }) =>
        `For communities, the shift is less about a single decision and more about accumulation: small frictions that change participation, trust, and who feels protected by institutions.`,
      ({ theme }) =>
        `Coalitions have adjusted to the new incentives. The loudest arguments remain on television, but the decisive work is happening in local offices, hearings, and compliance desks.`,
      ({ theme }) =>
        `Routines change politics. And once ${theme} becomes routine, it becomes harder to unwind without a crisis big enough to force a new bargain.`
    ],
    impact: [
      ({ theme }) =>
        `In practice, institutions are improvising: standing services where there used to be emergency response, and permanent triage where there used to be ad hoc judgment.`,
      ({ theme }) =>
        `The costs are uneven. Some places have the staff and budgets to absorb the new system. Others are forced into delays, backlogs, and blunt decisions.`,
      ({ theme }) =>
        `The long-run effect is not a single outcome. It is a shift in capacity: what can be handled quickly, what becomes delayed, and what quietly gets dropped.`
    ]
  };
  const conflictTemplate = pickStable(conflictTemplates[angle] || conflictTemplates.impact, `${seed}|conflict`) || conflictTemplates.impact[0];
  const conflict = typeof conflictTemplate === 'function' ? conflictTemplate({ theme }) : String(conflictTemplate || '');

  const legacyOutlookTemplates = [
    ({ theme }) =>
      `What happens next will depend less on new announcements and more on implementation capacity: staffing, training, oversight, and whether the system’s mistakes are visible enough to correct.`,
    ({ theme }) =>
      `The equilibrium is fragile. A major court ruling, a macro shock, or a security incident could destabilize the new regime and force a faster renegotiation of the rules around ${theme}.`,
    ({ theme }) =>
      `For now, the direction is clear: more documentation, more measurement, and more disputes over who gets to define “working” in a system built around ${theme}.`
  ];
  const legacyOutlook = pickStable(legacyOutlookTemplates, `${seed}|outlook`)({ theme });

  const sourceLines = [];
  const usedUrls = new Set();
  const addSource = (title, url) => {
    const cleanUrl = String(url || '').trim();
    if (!cleanUrl) return;
    if (usedUrls.has(cleanUrl)) return;
    usedUrls.add(cleanUrl);
    const cleanTitle = stripAuthorSuffix(String(title || '')).replace(/\s+/g, ' ').trim();
    sourceLines.push(`- ${cleanTitle || cleanUrl} — ${cleanUrl}`.trim());
  };

  for (const c of citations.slice(0, 6)) {
    addSource(c.title, c.url);
  }
  for (const m of markets.slice(0, 2)) {
    const label = String(m.label || '').replace(/\?+$/g, '').trim();
    addSource(label ? `Polymarket market: ${label}` : 'Polymarket market', m.url);
  }
  for (const key of Object.keys(econ || {}).slice(0, 2)) {
    const row = econ[key] || {};
    addSource(key, row.url);
  }

  const lines = [];
  lines.push(lede);
  lines.push('');
  if (secondPara) {
    lines.push(secondPara);
    lines.push('');
  }
  lines.push(nutGraf);
  lines.push('');
  if (baseline) {
    lines.push(baseline);
    lines.push('');
  }
  lines.push(timeline);
  lines.push('');
  if (evidenceMosaic) {
    lines.push(evidenceMosaic);
    lines.push('');
  }
  lines.push(mechanics);
  lines.push('');
  lines.push(implementation);
  lines.push('');
  lines.push(stakeholders);
  lines.push('');
  lines.push(details);
  lines.push('');
  lines.push(sectionContext);
  lines.push('');
  lines.push(vignette);
  lines.push('');
  lines.push(metricsPara);
  lines.push('');
  lines.push(failureModes);
  lines.push('');
  lines.push(adaptation);
  lines.push('');
  lines.push(voicePara);
  lines.push('');
  if (indicatorsPara) {
    lines.push(indicatorsPara);
    lines.push('');
  }
  lines.push(pushback);
  lines.push('');
  lines.push(nextBattles);
  lines.push('');
  lines.push(counterVoice);
  lines.push('');
  lines.push(outlook);
  lines.push('');
  lines.push(closing);
  lines.push('');
  if (sourceLines.length) {
    lines.push('Sources');
    lines.push('');
    lines.push(sourceLines.join('\n'));
  }

  return lines.join('\n');
}

function buildSeedArticleFromStory(story) {
  const pack = story.evidencePack || {};
  const citations = Array.isArray(pack.citations) ? pack.citations : [];
  const signals = Array.isArray(pack.signals) ? pack.signals : [];
  const markets = Array.isArray(pack.markets) ? pack.markets : [];
  const curation = story && story.curation && typeof story.curation === 'object' ? story.curation : null;

  const editionDate = pack.editionDate || '';
  // Prefer Opus-curated title/dek over heuristic seeds
  const title = curation?.curatedTitle || curation?.draftArticle?.title || story.headlineSeed || 'Future Times story';
  const dek = curation?.curatedDek || curation?.draftArticle?.dek || story.dekSeed || '';
  const meta = editionDate ? `${story.section} • ${editionDate}` : String(story.section || '').trim();
  const body = buildForecastBody(story);

  return {
    id: story.storyId,
    section: story.section,
    title,
    dek,
    meta,
    image: 'assets/img/humanoids-labor-market.svg',
    body,
    signals,
    markets,
    prompt: `Editorial photo illustration prompt: ${title}. Documentary realism. Dated ${editionDate}.`,
    citations,
    stats: { econ: pack.econ || {}, markets: pack.markets || [] },
    editionDate,
    generatedFrom: `signals-pipeline / ${story.day}`,
    generatedAt: new Date().toISOString(),
    curationGeneratedAt: story?.curation?.generatedAt || null,
    yearsForward: story.yearsForward
  };
}

async function runMockRenderer(job, seedArticle) {
  const rendered = { ...seedArticle };
  const stages = [
    { label: 'Evidence pack loaded', percent: 12 },
    { label: 'Angle and structure', percent: 36 },
    { label: 'Draft composed', percent: 68 },
    { label: 'Citation pass', percent: 88 },
    { label: 'Formatting complete', percent: 100 }
  ];

  for (const stage of stages) {
    await sleep(randomDelay(140, 240));
    broadcastToJobSubscribers(job, { type: 'render.progress', phase: stage.label, percent: stage.percent });
  }

  const chunks = splitToChunks(rendered.body.trim(), MAX_BODY_CHUNK_BYTES);
  for (const chunk of chunks) {
    await sleep(randomDelay(120, 220));
    broadcastToJobSubscribers(job, { type: 'render.chunk', delta: `${chunk}\n\n` });
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
    const errorText = typeof event.error === 'string' ? event.error : event.message || event.err || 'Spark render failed';
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

function buildSparkRequest(seedArticle, story) {
  const editionDate = story.evidencePack?.editionDate || seedArticle.editionDate;
  const baselineDay = normalizeDay(story.day) || formatDay();
  const baselineYear = baselineDay.slice(0, 4) || '2026';
  const yearsForward = story.yearsForward;
  const targetYear = Number(baselineYear) + (Number.isFinite(Number(yearsForward)) ? Number(yearsForward) : 0);
  const timeAnchor = editionDate ? `${editionDate} (${targetYear || 'future'})` : `${targetYear || 'future'}`;
  let instructions = [
    `You are writing an article for The Future Times as if it is being published on ${timeAnchor}.`,
    `Write it as a real news report in the target year: do not describe it as a projection, simulation, or prompt output.`,
    `Invent an original future event that follows plausibly from the stored evidence pack. Do NOT restate the baseline headline as the future story.`,
    `Treat the evidence pack citations as historical context from ${baselineDay} (past) if helpful.`,
    `Prediction markets are idea inputs: use them to infer the most likely outcome, and report that outcome as the one that happened. Do not pose the story as a question.`,
    `Avoid the phrase “baseline year.” Use explicit years (for example: “in ${baselineYear}”).`,
    `Output narrative paragraphs (NYT-style) and end with a short “Sources” list (4-8 links). Avoid section headings.`
  ].join('\n');

  const curation = story && story.curation && typeof story.curation === 'object' ? story.curation : null;
  if (curation) {
    const topicTitle = String(curation.topicTitle || curation.topicSeed || '').trim();
    const directions = String(curation.sparkDirections || '').trim();
    const eventSeed = String(curation.futureEventSeed || '').trim();
    const outline = Array.isArray(curation.outline) ? curation.outline.filter(Boolean).slice(0, 10) : [];
    if (topicTitle || directions || eventSeed || outline.length) {
      const curatorBlock = [
        '',
        'Curator plan (internal guidance; do not mention explicitly):',
        topicTitle ? `- Topic: ${topicTitle}` : null,
        directions ? `- Directions: ${directions}` : null,
        eventSeed ? `- Future event seed: ${eventSeed}` : null,
        outline.length ? `- Outline: ${outline.map((x) => String(x).trim()).filter(Boolean).join(' | ')}` : null
      ]
        .filter(Boolean)
        .join('\n');
      instructions = `${instructions}\n${curatorBlock}`.trim();
    }
  }

  return {
    type: 'render.article',
    model: process.env.SPARK_MODEL || 'codex-spark',
    yearsForward,
    baselineDay,
    editionDate,
    section: story.section,
    headlineSeed: story.headlineSeed,
    dekSeed: story.dekSeed,
    evidencePack: story.evidencePack || {},
    instructions,
    curation: curation || undefined,
    constraints: {
      grounding: 'baseline_citations',
      // Baseline facts must be grounded in the evidence pack citations.
      baselineFactsMustBeCited: true,
      // Forecast narrative is allowed (invented future events), but must be consistent with the baseline signals.
      allowForecastNarrative: true,
      // Do not restate the baseline headline as the future story; write an original future-news article.
      avoidCopyingBaselineHeadlines: true,
      // Headlines should not be posed as questions.
      noQuestionHeadlines: true,
      // Prefer a readable article: narrative paragraphs and a short "Sources" list at the end.
      requireCitationsInBody: false,
      includeCondensedSourcesSection: true,
      avoidSectionHeadings: true,
      avoidMetaForecastLanguage: true,
      presentTenseInTargetYear: true,
      marketsAsInputsWriteLikelyOutcome: true
    },
    article: seedArticle
  };
}

async function runCodexSocketRenderer(job, story, seedArticle) {
  const requestBody = buildSparkRequest(seedArticle, story);
  const headers = {};
  if (SPARK_AUTH_TOKEN) {
    headers[SPARK_AUTH_HEADER] = `${SPARK_AUTH_PREFIX} ${SPARK_AUTH_TOKEN}`;
  }

  return new Promise((resolve, reject) => {
    let resolved = false;
    let settled = false;
    const timeout = setTimeout(() => {
      if (settled) return;
      settled = true;
      socket.terminate();
      reject(new Error('Spark provider timeout'));
    }, SPARK_REQUEST_TIMEOUT_MS);

    const socket = new WebSocket(SPARK_WS_URL, { headers });

    socket.on('open', () => {
      broadcastToJobSubscribers(job, { type: 'render.progress', phase: 'Connected to Codex Spark', percent: 10 });
      socket.send(JSON.stringify(requestBody));
    });

    socket.on('message', (raw) => {
      if (settled) return;
      const frames = parseProviderFrame(raw.toString());
      for (const frame of frames) {
        const normalized = normalizeProviderEvent(frame);
        if (!normalized.type && !normalized.delta && !normalized.article) continue;

        if (normalized.type === 'render.progress') {
          broadcastToJobSubscribers(job, { type: 'render.progress', phase: normalized.phase || 'Rendering', percent: normalized.percent });
          continue;
        }

        if (normalized.type === 'render.chunk' && normalized.delta) {
          broadcastToJobSubscribers(job, { type: 'render.chunk', delta: normalized.delta });
          seedArticle.body = `${seedArticle.body || ''}${String(normalized.delta)}`;
          continue;
        }

        if (normalized.type === 'render.article' && normalized.article) {
          resolved = true;
          const a = normalized.article;
          seedArticle.title = a.title || seedArticle.title;
          seedArticle.dek = a.dek || seedArticle.dek;
          seedArticle.body = a.body || seedArticle.body || '';
          seedArticle.signals = a.signals || seedArticle.signals;
          seedArticle.markets = a.markets || seedArticle.markets;
          seedArticle.prompt = a.prompt || seedArticle.prompt;
          seedArticle.citations = a.citations || seedArticle.citations;
        }

        if (normalized.type === 'render.complete' || normalized.type === 'render.article') {
          settled = true;
          clearTimeout(timeout);
          socket.close();
          job.complete = true;
          job.status = 'complete';
          job.result = seedArticle;
          cacheByKey.set(job.key, seedArticle);
          pipeline.storeRendered(job.storyId, seedArticle, { curationGeneratedAt: job.curationGeneratedAt });
          broadcastToJobSubscribers(job, { type: 'render.complete', article: seedArticle });
          finalizeJobCleanup(job);
          resolve(seedArticle);
          return;
        }

        if (normalized.type === 'render.error') {
          settled = true;
          clearTimeout(timeout);
          socket.close();
          reject(new Error(normalized.error || 'Spark render failed'));
          return;
        }
      }
    });

    socket.on('error', (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      reject(err);
    });

    socket.on('close', () => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      if (resolved) {
        job.complete = true;
        job.status = 'complete';
        job.result = seedArticle;
        cacheByKey.set(job.key, seedArticle);
        pipeline.storeRendered(job.storyId, seedArticle, { curationGeneratedAt: job.curationGeneratedAt });
        broadcastToJobSubscribers(job, { type: 'render.complete', article: seedArticle });
        finalizeJobCleanup(job);
        resolve(seedArticle);
        return;
      }
      reject(new Error('Spark provider closed connection'));
    });
  });
}

async function runSparkRender(job, story, seedArticle) {
  if ((SPARK_WS_URL || SPARK_HTTP_URL) && SPARK_MODE !== 'mock') {
    try {
      if (SPARK_WS_URL) {
        await runCodexSocketRenderer(job, story, seedArticle);
        return;
      }
      throw new Error('HTTP mode for Spark is not implemented yet');
    } catch (error) {
      if (!SPARK_FALLBACK_TO_MOCK) throw error;
      broadcastToJobSubscribers(job, { type: 'render.progress', phase: `Spark unavailable: ${error.message || 'fallback to mock'}`, percent: 16 });
      await runMockRenderer(job, seedArticle);
      return;
    }
  }

  await runMockRenderer(job, seedArticle);
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
  await runSparkRender(job, story, seedArticle);
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

  const preferredSections = ['U.S.', 'World', 'Business', 'Technology', 'Arts', 'Lifestyle', 'Opinion'];
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
  </style>
</head>
<body>
  <header>
    <div class="row" style="justify-content:space-between">
      <div>
        <h1>Future Times Signal Pack</h1>
        <div class="muted">Day <strong>${escapeHtml(day)}</strong> - Generated <span class="small">${escapeHtml(generatedAt)}</span></div>
      </div>
      <div class="row">
        <a class="badge" href="${escapeHtml(backHref)}">Back to front page</a>
        <a class="badge" href="${escapeHtml(jsonHref)}">JSON</a>
        <a class="badge" href="${escapeHtml(prettyHref)}">Pretty JSON</a>
      </div>
    </div>
    <div class="kpi muted small" style="margin-top:10px">
      Counts: <strong>${escapeHtml(counts.rawItems ?? rawItems.length)}</strong> raw -
      <strong>${escapeHtml(counts.signals ?? signals.length)}</strong> signals -
      <strong>${escapeHtml(counts.topics ?? topics.length)}</strong> topics -
      <strong>${escapeHtml(counts.editions ?? editions.length)}</strong> editions
      ${typeCountLine ? ` - ${escapeHtml(typeCountLine)}` : ''}
    </div>
    <div class="divider"></div>
    <div class="muted small" style="margin-top:12px;max-width:980px">
      This is the daily evidence pack: all ingested items normalized into signals, clustered into section topics, and used to build editions (+0..+10 years).
      Prediction markets and macro data are treated as indicators/inputs; they do not become story headlines.
    </div>
    <nav class="toc" aria-label="Jump to section">
      <a href="#at-a-glance">At a glance</a>
      ${sections.map((s) => `<a href="#${escapeHtml(sectionId(s))}">${escapeHtml(s)}</a>`).join('')}
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
          <h3>Indicators + health</h3>
          <div class="muted small">Market and macro inputs (baseline), plus source health.</div>
          <div class="row" style="margin-top:8px">
            <span class="badge">sources ok ${escapeHtml(sourcesOk)}</span>
            <span class="badge">sources error ${escapeHtml(sourcesWithError)}</span>
          </div>
          <h4 class="tight">Econ snapshot</h4>
          ${econSignals.length ? `<ul class="bullets">${econSignals
            .map((s) => `<li><strong>${escapeHtml(s.title || '')}</strong> <span class="muted">${escapeHtml(String(s.summary || '').slice(0, 90))}</span></li>`)
            .join('')}</ul>` : '<div class="muted small">No econ signals.</div>'}
          <h4 class="tight">Prediction markets (inputs)</h4>
          ${marketSignals.length ? `<ul class="bullets">${marketSignals
            .map((s) => `<li>${escapeHtml(String(s.title || '').replace(/\?+$/g, ''))} <span class="muted">(${escapeHtml(String(s.summary || '').slice(0, 50))})</span></li>`)
            .join('')}</ul>` : '<div class="muted small">No market signals.</div>'}
        </div>
      </div>
    </section>

    ${sections
      .map((section) => {
        const sectionTopics = topicsBySection.get(section) || [];
        const sectionSignals = signalsBySection.get(section) || [];
        return `
          <section id="${escapeHtml(sectionId(section))}">
            <h2>${escapeHtml(section)}</h2>
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
      const sparkHref = `/api/admin/spark-request?id=${encodeURIComponent(String(s.storyId || ''))}`;

      const badges = [
        key ? `<span class="badge">key</span>` : '',
        isHero ? `<span class="badge hero">hero</span>` : '',
        topicTitle ? `<span class="badge">${escapeHtml(topicTitle)}</span>` : ''
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
            <a class="pill" href="${escapeHtml(sparkHref)}" target="_blank" rel="noopener">Spark request (JSON)</a>
            <span class="muted mono">storyId ${escapeHtml(String(s.storyId || ''))}</span>
          </div>
          <div class="grid2">
            <div>
              <h4>Future event seed</h4>
              <div class="monoBox">${eventSeed ? escapeHtml(eventSeed) : '<span class="muted">—</span>'}</div>
              <h4>Codex Spark directions</h4>
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

    if (pathname === '/api/ping') {
      sendJson(res, { ok: true });
      return;
    }

    if (pathname === '/api/config') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const curatorConfig = getOpusCurationConfigFromEnv();
      sendJson(res, {
        provider: {
          mode: SPARK_MODE,
          configured: isSparkConfigured(),
          wsUrl: SPARK_WS_URL || null,
          httpUrl: SPARK_HTTP_URL || null,
          fallbackToMock: SPARK_FALLBACK_TO_MOCK
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

    if (pathname === '/api/admin/spark-request') {
      if (req.method !== 'GET') return send405(res, 'GET');
      const storyId = String(url.searchParams.get('id') || url.searchParams.get('storyId') || '').trim();
      if (!storyId) {
        sendJson(res, { ok: false, error: 'missing_story_id' }, 400);
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
        sendJson(res, { ok: false, error: 'story_not_found', storyId }, 404);
        return;
      }

      const seedArticle = buildSeedArticleFromStory(story);
      const requestBody = buildSparkRequest(seedArticle, story);
      sendJson(res, { ok: true, storyId, request: requestBody });
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
    <a href="${escapeHtml(traceHtmlUrl)}" target="_blank" rel="noopener">Event trace (HTML)</a>
    <a href="${escapeHtml(curationUrl)}" target="_blank" rel="noopener">Curation (JSON)</a>
    <a href="${escapeHtml(previewUrl)}" target="_blank" rel="noopener">Prompt preview JSON (+${escapeHtml(String(yearsForward))}y)</a>
    <a href="/api/day-signal?day=${escapeHtml(encodeURIComponent(builtDay))}&format=html" target="_blank" rel="noopener">Day signal pack</a>
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
    <div class="muted">Edit the prompt, then click Apply. This updates story curations and future Spark renders.</div>
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
      sendJson(res, payload);
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
          sendJson(res, { status: 'ready', article: status.article });
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
          sendJson(res, { status: 'ready', article: started.article });
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
      console.log(`Future Times Spark service running on http://${DEFAULT_HOST}:${activePort}`);
      console.log(`WebSocket endpoint: ws://${DEFAULT_HOST}:${activePort}/ws`);
      return;
    }
  }

  console.error(
    `Unable to bind port: no available ports in range ${PORT_START}..${PORT_START + PORT_MAX_TRIES * PORT_FALLBACK_STEP}`
  );
  process.exit(1);
}

start().catch((err) => {
  console.error('Failed to start server:', err?.message || err);
  process.exit(1);
});
