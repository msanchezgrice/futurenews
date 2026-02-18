import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { openDatabase, migrate } from './db.js';

// ── Image generation for key stories ──
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const IMG_DIR = path.resolve(__dirname, '..', '..', 'assets', 'img', 'generated');

async function generateStoryImage(prompt, storyId) {
  const apiKey = process.env.OPENAI_API_KEY || '';
  if (!apiKey) return null;
  try {
    if (!fs.existsSync(IMG_DIR)) fs.mkdirSync(IMG_DIR, { recursive: true });
    const safeId = String(storyId || 'story').replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 80);
    const filename = `${safeId}.png`;
    const filepath = path.join(IMG_DIR, filename);

    // Skip if already generated
    if (fs.existsSync(filepath)) return `assets/img/generated/${filename}`;

    const resp = await fetch('https://api.openai.com/v1/images/generations', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'dall-e-3',
        prompt: `Newspaper editorial photograph style, photojournalistic. ${prompt}. No text overlays. Modern, high quality.`,
        n: 1,
        size: '1792x1024',
        quality: 'standard'
      }),
      signal: AbortSignal.timeout(60000)
    });

    if (!resp.ok) {
      console.error(`Image gen failed (${resp.status}): ${await resp.text().catch(() => '')}`);
      return null;
    }

    const data = await resp.json();
    const imageUrl = data?.data?.[0]?.url;
    if (!imageUrl) return null;

    // Download the image
    const imgResp = await fetch(imageUrl, { signal: AbortSignal.timeout(30000) });
    if (!imgResp.ok) return null;
    const buffer = Buffer.from(await imgResp.arrayBuffer());
    fs.writeFileSync(filepath, buffer);

    return `assets/img/generated/${filename}`;
  } catch (err) {
    console.error(`Image gen error for ${storyId}: ${err.message}`);
    return null;
  }
}
import { parseFeed } from './rss.js';
import { fetchPolymarketRawItems } from './polymarket.js';
import { fetchFredSeriesRawItem } from './fred.js';
import {
  ANGLES,
  SECTION_ORDER,
  canonicalizeUrl,
  formatDay,
  formatEditionDate,
  generateFutureDescriptor,
  isoNow,
  jaccard,
  normalizeDay,
  pickDeterministic,
  sha256Hex,
  slugify,
  stableHash,
  tokenize
} from './utils.js';
import { buildEditionCurationPrompt, generateEditionCurationPlan, getOpusCurationConfigFromEnv, generateMissingArticleBodies } from './curation.js';

const DEFAULT_DB_FILE = path.resolve(process.cwd(), 'data', 'future-times.sqlite');
const DEFAULT_SOURCES_FILE = path.resolve(process.cwd(), 'config', 'sources.json');
const RENDER_CACHE_VERSION = '19';

function renderVariantFromCurationGeneratedAt(value) {
  const raw = String(value || '').trim();
  if (!raw) return 'c0';
  return `c${sha256Hex(raw).slice(0, 10)}`;
}

const SIGNAL_TYPE_BY_SOURCE = (source) => {
  const url = String(source?.url || '').toLowerCase();
  if (String(source?.type || '').toLowerCase() === 'api_json' && String(source?.source_id || '').includes('polymarket')) {
    return 'market';
  }
  if (String(source?.type || '').toLowerCase() === 'csv' && String(source?.source_id || '').includes('fred')) {
    return 'econ';
  }
  if (url.includes('arxiv.org') || String(source?.name || '').toLowerCase().includes('arxiv')) {
    return 'research';
  }
  return 'news';
};

const HORIZON_BY_SIGNAL_TYPE = {
  news: 'near',
  econ: 'near',
  market: 'mid',
  research: 'mid',
  policy: 'mid'
};

const KEYWORDS_BY_SECTION = {
  'U.S.': ['congress', 'senate', 'house', 'supreme', 'court', 'election', 'federal', 'state', 'governor', 'immigration'],
  World: ['china', 'russia', 'europe', 'eu', 'ukraine', 'gaza', 'israel', 'iran', 'trade', 'nato', 'war', 'global'],
  Business: ['market', 'stocks', 'earnings', 'inflation', 'jobs', 'economy', 'recession', 'rates', 'bond', 'bank', 'ipo'],
  Technology: ['chip', 'semiconductor', 'software', 'cloud', 'security', 'cyber', 'gpu', 'open-source', 'quantum', 'blockchain', 'crypto'],
  AI: [
    'ai', 'artificial intelligence', 'machine learning', 'deep learning', 'neural network', 'llm', 'large language model',
    'gpt', 'claude', 'gemini', 'openai', 'anthropic', 'deepmind', 'transformer', 'diffusion',
    'robot', 'robotics', 'humanoid', 'autonomous', 'self-driving', 'autopilot',
    'agent', 'agents', 'agentic', 'multi-agent', 'reasoning', 'inference',
    'chatbot', 'copilot', 'foundation model', 'fine-tuning', 'rlhf', 'alignment',
    'computer vision', 'nlp', 'natural language', 'text-to-image', 'text-to-video',
    'agi', 'superintelligence', 'ai safety', 'ai regulation', 'ai governance',
    'benchmark', 'training', 'compute', 'scaling', 'emergent', 'multimodal'
  ],
  Arts: ['film', 'music', 'book', 'museum', 'artist', 'gallery', 'festival', 'theatre', 'theater', 'culture'],
  Lifestyle: ['health', 'travel', 'food', 'wellness', 'housing', 'fitness', 'work', 'school', 'family', 'fashion'],
  Opinion: ['opinion', 'editorial', 'column', 'debate', 'analysis', 'rights', 'privacy', 'democracy']
};

// AI sub-categories for finer-grained classification within the AI section
const AI_CATEGORIES = {
  'Foundation Models': ['llm', 'large language model', 'gpt', 'claude', 'gemini', 'foundation model', 'transformer', 'scaling', 'training', 'benchmark', 'multimodal', 'diffusion'],
  'Robotics & Embodied AI': ['robot', 'robotics', 'humanoid', 'autonomous', 'self-driving', 'autopilot', 'embodied', 'manipulation', 'locomotion', 'drone'],
  'AI Agents': ['agent', 'agents', 'agentic', 'multi-agent', 'tool use', 'function calling', 'reasoning', 'planning', 'orchestration', 'workflow'],
  'AI Safety & Governance': ['ai safety', 'alignment', 'ai regulation', 'ai governance', 'agi', 'superintelligence', 'existential risk', 'bias', 'fairness', 'interpretability', 'explainability'],
  'Applied AI': ['computer vision', 'nlp', 'natural language', 'text-to-image', 'text-to-video', 'speech', 'medical ai', 'drug discovery', 'protein', 'weather', 'climate ai'],
  'AI Industry': ['openai', 'anthropic', 'deepmind', 'google ai', 'meta ai', 'microsoft ai', 'nvidia', 'compute', 'data center', 'gpu', 'tpu', 'chip', 'funding', 'valuation', 'acquisition']
};

function classifyAICategory(text) {
  const lower = String(text || '').toLowerCase();
  let best = { category: 'General AI', score: 0 };
  for (const [category, keywords] of Object.entries(AI_CATEGORIES)) {
    let score = 0;
    for (const kw of keywords) {
      if (lower.includes(kw)) score += 1;
    }
    if (score > best.score) best = { category, score };
  }
  return best.category;
}

function safeJson(value, fallback) {
  try {
    return JSON.stringify(value ?? fallback);
  } catch {
    return JSON.stringify(fallback);
  }
}

function safeParseJson(text, fallback) {
  try {
    return JSON.parse(String(text || ''));
  } catch {
    return fallback;
  }
}

function normalizeSection(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw.toLowerCase() === 'us') return 'U.S.';
  if (raw.toLowerCase() === 'u.s.') return 'U.S.';
  for (const section of SECTION_ORDER) {
    if (section.toLowerCase() === raw.toLowerCase()) return section;
  }
  return raw;
}

function classifySection(text, defaultSection = 'World') {
  const lower = String(text || '').toLowerCase();
  let best = { section: defaultSection, score: 0 };
  for (const section of SECTION_ORDER) {
    const keywords = KEYWORDS_BY_SECTION[section] || [];
    let score = 0;
    for (const kw of keywords) {
      if (lower.includes(kw)) score += 1;
    }
    if (score > best.score) best = { section, score };
  }
  return best.section || defaultSection;
}

function extractKeywords(text, max = 12) {
  const words = tokenize(text);
  const counts = new Map();
  for (const w of words) {
    counts.set(w, (counts.get(w) || 0) + 1);
  }
  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, max)
    .map(([w]) => w);
}

function loadEntityDicts(rootDir) {
  const file = path.resolve(rootDir, 'server', 'pipeline', 'entity-dicts.json');
  try {
    const raw = fs.readFileSync(file, 'utf8');
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch {
    return null;
  }
}

function extractEntities(text, dicts) {
  const raw = String(text || '');
  if (!raw) return [];
  const lower = raw.toLowerCase();
  const found = [];
  const addMatches = (arr) => {
    for (const item of arr || []) {
      const needle = String(item || '').toLowerCase();
      if (!needle) continue;
      if (lower.includes(needle)) {
        found.push(String(item));
      }
    }
  };
  addMatches(dicts?.companies);
  addMatches(dicts?.places);
  addMatches(dicts?.institutions);

  // Lightweight proper-noun heuristic as a backstop (limits noise by requiring 2+ words).
  const proper = raw.match(/\b[A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+)+\b/g) || [];
  for (const p of proper.slice(0, 6)) {
    if (!found.includes(p)) found.push(p);
  }
  return found.slice(0, 10);
}

function scoreSignal(signalType, publishedAtIso, title) {
  const base = signalType === 'market' ? 1.15 : signalType === 'research' ? 1.1 : signalType === 'econ' ? 0.95 : 1.0;
  const now = Date.now();
  const ts = publishedAtIso ? Date.parse(publishedAtIso) : NaN;
  const ageHours = Number.isFinite(ts) ? Math.max(0, (now - ts) / (1000 * 60 * 60)) : 24;
  const recencyBoost = Math.max(0.2, Math.min(1.0, 1.0 / (1.0 + ageHours / 18)));
  const lengthBoost = Math.min(1.0, 0.6 + Math.min(0.4, String(title || '').length / 120));
  return base * recencyBoost * lengthBoost;
}

function buildTopicBrief(signals) {
  const lines = [];
  for (const signal of signals.slice(0, 4)) {
    const summary = String(signal.summary || '')
      .replace(/<[^>]*>/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    if (!summary) continue;
    lines.push(`- ${summary.slice(0, 220)}`);
  }
  return lines.length ? lines.join('\n') : 'A cluster of signals suggests a developing storyline worth tracking.';
}

function chooseHorizonMix(yearsForward) {
  const y = Number(yearsForward) || 0;
  if (y <= 2) return { near: 0.6, mid: 0.3, long: 0.1 };
  if (y <= 5) return { near: 0.3, mid: 0.5, long: 0.2 };
  return { near: 0.1, mid: 0.4, long: 0.5 };
}

function sectionSlug(section) {
  return slugify(section === 'U.S.' ? 'us' : section, 20);
}

function buildStoryId(day, yearsForward, section, topicSlug, angle) {
  return `ft-${day}-y${yearsForward}-${sectionSlug(section)}-${topicSlug}-${angle}`;
}

function stripProperNouns(text) {
  const raw = String(text || '');
  if (!raw) return '';
  // Remove sequences of Title Case words (names/places/brands) but keep acronyms (AI, U.S., EU).
  return raw
    .replace(/\b[A-Z][a-z][A-Za-z'-]+(?:['’][A-Za-z]+)?(?:\s+[A-Z][a-z][A-Za-z'-]+(?:['’][A-Za-z]+)?){0,3}\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function deriveThemePhrase(topicLabel, topicBrief) {
  const label = String(topicLabel || '').trim();
  const brief = String(topicBrief || '')
    .split('\n')
    .map((line) => String(line || '').replace(/^\s*-\s+/, '').trim())
    .filter(Boolean)
    .join(' ');
  const withoutNames = `${stripProperNouns(label)} ${stripProperNouns(brief)}`.replace(/\s+/g, ' ').trim();
  const tokens = tokenize(withoutNames || label);
  const tokenSet = new Set(tokens);
  const hasAny = (list) => list.some((w) => tokenSet.has(w));

  if (hasAny(['protest', 'protests', 'strike', 'strikes', 'walkout', 'walkouts', 'demonstration', 'demonstrations'])) {
    return 'Protests and Strikes';
  }
  if (hasAny(['war', 'conflict', 'invasion', 'ceasefire', 'sanctions', 'missile', 'nato'])) {
    return 'Geopolitical Conflict';
  }
  if (hasAny(['immigration', 'border', 'asylum', 'deportation'])) return 'Immigration Enforcement';
  if (hasAny(['election', 'ballot', 'campaign', 'voter', 'primary'])) return 'Election Politics';
  if (hasAny(['inflation', 'cpi', 'prices', 'pricing'])) return 'Inflation and Prices';
  if (hasAny(['unemployment', 'jobs', 'wages', 'labor', 'pay'])) return 'The Labor Market';
  if (hasAny(['rates', 'yield', 'bond', 'bonds', 'fed', 'fedfunds'])) return 'Interest Rates';
  if (hasAny(['llm', 'language', 'gpt', 'claude', 'gemini', 'transformer', 'chatbot'])) return 'Foundation Models';
  if (hasAny(['agent', 'agents', 'agentic', 'orchestration', 'reasoning'])) return 'AI Agents';
  if (hasAny(['robot', 'robots', 'robotics', 'humanoid', 'autonomous', 'self-driving'])) return 'Robotics and Autonomy';
  if (hasAny(['alignment', 'safety', 'regulation', 'governance', 'superintelligence', 'agi'])) return 'AI Safety and Governance';
  if (hasAny(['ai', 'model', 'models', 'automation', 'machine', 'neural'])) return 'AI and Automation';
  if (hasAny(['chip', 'chips', 'semiconductor', 'semiconductors', 'gpu', 'gpus'])) return 'The Chip Supply Chain';
  if (hasAny(['climate', 'wildfire', 'wildfires', 'hurricane', 'hurricanes', 'heat', 'flood', 'floods'])) return 'Climate Adaptation';
  if (hasAny(['housing', 'rent', 'mortgage', 'mortgages'])) return 'Housing Affordability';
  if (hasAny(['health', 'vaccine', 'vaccines', 'hospital', 'hospitals', 'medicine'])) return 'Public Health';
  if (hasAny(['film', 'music', 'book', 'books', 'museum', 'museums', 'artist', 'artists', 'theater', 'theatre'])) return 'Arts and Culture';
  if (hasAny(['privacy', 'surveillance', 'tracking'])) return 'Privacy and Surveillance';
  if (hasAny(['court', 'courts', 'supreme', 'lawsuit', 'lawsuits', 'appeals', 'judge', 'judges'])) return 'Courts and Regulation';
  if (hasAny(['assault', 'shooting', 'shootings', 'charges', 'charged', 'trial', 'sentence', 'sentenced', 'strangulation', 'murder'])) return 'High-Profile Criminal Cases';

  if (tokens.length >= 3) {
    const phrase = `${tokens[0]} ${tokens[1]} ${tokens[2]}`;
    return phrase.replace(/\b[a-z]/g, (c) => c.toUpperCase());
  }
  if (tokens.length >= 2) {
    const phrase = `${tokens[0]} ${tokens[1]}`;
    return phrase.replace(/\b[a-z]/g, (c) => c.toUpperCase());
  }
  if (tokens.length === 1) {
    return tokens[0].replace(/\b[a-z]/g, (c) => c.toUpperCase());
  }
  return 'A Major Shift';
}

function cleanTopicForHeadline(topicLabel) {
  let base = String(topicLabel || '').replace(/\s+/g, ' ').trim();
  // Remove trailing author attributions like "| Simon Jenkins" or "| Editorial"
  base = base.replace(/\s*\|\s*[^|]{2,120}$/g, '').trim();
  // Remove trailing question marks / questions
  if (base.includes('?')) {
    base = base.split('?')[0].trim();
  }
  const questionLead = base.match(/[.?!]\s*(Why|How|What|When|Where|Who)\b/i);
  if (questionLead && typeof questionLead.index === 'number') {
    base = base.slice(0, questionLead.index).trim();
  }
  base = base.replace(/\?+$/g, '').replace(/[.:]\s*$/g, '').trim();
  // Coerce "Will X ..." framings to declarative
  const willWin = base.match(/^Will\s+(.+?)\s+win\s+(.+)$/i);
  if (willWin) base = `${willWin[1]} Wins ${willWin[2]}`;
  else {
    const willBe = base.match(/^Will\s+(.+?)\s+be\s+(.+)$/i);
    if (willBe) base = `${willBe[1]} Is ${willBe[2]}`;
    else if (/^Will\s+/i.test(base)) base = base.replace(/^Will\s+/i, '').trim();
  }
  // Remove "review" suffixes for arts topics
  base = base.replace(/\s+review\b.*$/i, '').trim();
  // Cap length
  if (base.length > 100) base = base.slice(0, 100).replace(/[,\s;:.]+$/g, '').trim();
  return base || 'Signal shift';
}

function buildHeadlineSeed(topicLabel, topicBrief, yearsForward, seed) {
  let base = cleanTopicForHeadline(topicLabel);
  if (!base || base.length < 4) base = 'Signal shift';

  // For yearsForward=0, just use the topic label cleaned up
  if (yearsForward === 0) {
    return base;
  }

  // For future editions, create a future-oriented headline from the original topic.
  // Use the actual topic label to derive a meaningful headline (NOT random token extraction).
  const baselineYear = 2026;
  const targetYear = baselineYear + (Number(yearsForward) || 0);

  // Extract a short DOMAIN PHRASE (2-6 words) from the headline to use in future templates.
  // The goal: a raw headline like "'Nice shoes, mate': we road test Lego Crocs" should become
  // something like "Fashion & Consumer Products", not the verbatim headline.
  let subject = base;
  // Strip quotes, attributions, and parentheticals
  subject = subject.replace(/[''""]/g, '').replace(/\(.*?\)/g, '').trim();
  // Remove common lead-in patterns
  subject = subject.replace(/^(The|A|An|Why|How|What|Who|When|Where)\s+/i, '').trim();
  // Take the first meaningful clause only (before comma, colon, dash, semicolon)
  const clauseBreak = subject.search(/[,;:–—|]/);
  if (clauseBreak > 5 && clauseBreak < 50) {
    subject = subject.slice(0, clauseBreak).trim();
  }
  // If still too long, extract 2-4 key noun phrases
  if (subject.length > 45) {
    // Try to extract proper nouns and key terms
    const words = subject.split(/\s+/).filter((w) => w.length > 2);
    const importantWords = words.filter((w) => /^[A-Z]/.test(w) || /^(?:AI|US|UK|EU|GDP|NASA|UN)\b/i.test(w));
    if (importantWords.length >= 2) {
      subject = importantWords.slice(0, 4).join(' ');
    } else {
      subject = words.slice(0, 5).join(' ');
    }
  }
  if (subject.length < 4) subject = 'Signal shift';

  // Minimal fallback headline — Opus should override this with a real extrapolation
  return `${subject} in ${targetYear}`;
}

function buildDekSeed(topicLabel, topicBrief, yearsForward, editionDate, baselineDay) {
  const cleanBrief = String(topicBrief || '')
    .split('\n')
    .map((line) => String(line || '').replace(/^\s*-\s+/, '').trim())
    .filter(Boolean)
    .join(' ')
    .replace(/<[^>]*>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  const label = cleanTopicForHeadline(topicLabel);

  if (yearsForward === 0) {
    // For the baseline edition, use a clean summary of the topic
    return cleanBrief.slice(0, 280) || label;
  }

  const baselineYear = String(baselineDay || '').slice(0, 4) || '2026';
  const targetYear = Number(baselineYear) + (Number(yearsForward) || 0);
  const dateLabel = String(editionDate || '').trim() || String(targetYear);

  // Minimal fallback dek — Opus should override this with a real extrapolation
  const shortLabel = label.length > 80 ? label.slice(0, 80).replace(/\s+\S*$/, '').trim() : label;
  return `A ${targetYear} report on ${shortLabel.toLowerCase()}.`;
}

function buildEvidencePack({ topic, evidenceSignals, econSignals, marketSignals, editionDate, yearsForward }) {
  const citations = [];
  for (let i = 0; i < evidenceSignals.length; i++) {
    const s = evidenceSignals[i];
    const id = `c${i + 1}`;
    citations.push({
      id,
      title: s.title,
      url: s.canonical_url || '',
      source: (safeParseJson(s.citations_json, [])[0] || {}).source || (safeParseJson(s.citations_json, [])[0] || {}).source_name || '',
      publishedAt: s.published_at || null,
      summary: String(s.summary || '')
        .replace(/<[^>]*>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 280)
    });
  }

  const econSnapshot = {};
  for (const s of econSignals) {
    const payload = safeParseJson(s.citations_json, []);
    econSnapshot[s.title] = {
      summary: s.summary || '',
      url: s.canonical_url || '',
      citation: payload[0] || null
    };
  }

  const markets = marketSignals.slice(0, 4).map((s) => ({
    label: String(s.title || '').replace(/\?+$/g, ''),
    prob: String(s.summary || '').slice(0, 60),
    url: s.canonical_url || ''
  }));

  const sidebarSignals = evidenceSignals.slice(0, 6).map((s) => ({
    label: s.title,
    value: (safeParseJson(s.citations_json, [])[0] || {}).source || ''
  }));

  return {
    grounding: 'hard_citations',
    section: topic.section,
    editionDate,
    yearsForward,
    topic: {
      topicId: topic.topic_id,
      label: topic.label,
      theme: deriveThemePhrase(topic.label, topic.brief),
      brief: topic.brief,
      horizon: topic.horizon_bucket
    },
    citations,
    markets,
    econ: econSnapshot,
    signals: sidebarSignals
  };
}

export class FutureTimesPipeline {
  constructor(options = {}) {
    this.rootDir = options.rootDir || process.cwd();
    this.dbFile = options.dbFile || DEFAULT_DB_FILE;
    this.sourcesFile = options.sourcesFile || DEFAULT_SOURCES_FILE;
    this.db = null;
    this.entityDicts = null;
    this.refreshInFlightByDay = new Map();
    this.lastRefresh = null;
    this.curationInFlight = null;
    this.lastCuration = null;
  }

  init() {
    if (this.db) return;
    this.db = openDatabase(this.dbFile);
    migrate(this.db);
    this.entityDicts = loadEntityDicts(this.rootDir);
    this.loadSourcesIntoDb();
    this.loadStandingTopicsIntoDb();
  }

  loadSourcesIntoDb() {
    const raw = fs.readFileSync(this.sourcesFile, 'utf8');
    const config = safeParseJson(raw, { sources: [] });
    const sources = Array.isArray(config.sources) ? config.sources : [];

    const stmt = this.db.prepare(`
      INSERT INTO sources(source_id, name, type, section, url, enabled, fetch_interval_minutes, meta_json)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(source_id) DO UPDATE SET
        name=excluded.name,
        type=excluded.type,
        section=excluded.section,
        url=excluded.url,
        enabled=excluded.enabled,
        fetch_interval_minutes=excluded.fetch_interval_minutes,
        meta_json=excluded.meta_json;
    `);

    for (const s of sources) {
      stmt.run(
        String(s.source_id),
        String(s.name || s.source_id),
        String(s.type || 'rss'),
        s.section === null ? null : String(s.section || ''),
        String(s.url || ''),
        s.enabled === false ? 0 : 1,
        Number(s.fetch_interval_minutes || 60),
        safeJson(s, {})
      );
    }
  }

  loadStandingTopicsIntoDb() {
    const topicsFile = path.resolve(this.rootDir, 'config', 'standing-topics.json');
    let config;
    try {
      const raw = fs.readFileSync(topicsFile, 'utf8');
      config = JSON.parse(raw);
    } catch {
      return; // No standing topics file yet — that's fine
    }
    const topics = Array.isArray(config?.topics) ? config.topics : [];
    if (!topics.length) return;

    const stmt = this.db.prepare(`
      INSERT INTO standing_topics(topic_key, section, category, subcategory, label, description, extrapolation_axes, keywords, milestones, enabled, created_at, updated_at)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(topic_key) DO UPDATE SET
        section=excluded.section,
        category=excluded.category,
        subcategory=excluded.subcategory,
        label=excluded.label,
        description=excluded.description,
        extrapolation_axes=excluded.extrapolation_axes,
        keywords=excluded.keywords,
        milestones=excluded.milestones,
        enabled=excluded.enabled,
        updated_at=excluded.updated_at;
    `);

    const now = isoNow();
    for (const t of topics) {
      stmt.run(
        String(t.topic_key),
        String(t.section || 'AI'),
        t.category || null,
        t.subcategory || null,
        String(t.label || t.topic_key),
        t.description || null,
        safeJson(t.extrapolation_axes, []),
        safeJson(t.keywords, []),
        safeJson(t.milestones, []),
        t.enabled === false ? 0 : 1,
        now,
        now
      );
    }
  }

  // ── Standing Topics: get all enabled ──
  getStandingTopics(section = null) {
    if (section) {
      return this.db.prepare('SELECT * FROM standing_topics WHERE enabled=1 AND section=? ORDER BY category, label').all(section);
    }
    return this.db.prepare('SELECT * FROM standing_topics WHERE enabled=1 ORDER BY section, category, label').all();
  }

  // ── Evidence Mapping: link signals to standing topics ──
  matchSignalsToStandingTopics(day) {
    const standingTopics = this.getStandingTopics();
    if (!standingTopics.length) return;

    const signals = this.db.prepare(`
      SELECT signal_id, section, title, summary, keywords_json
      FROM signals WHERE day=?
    `).all(day);

    const insertEvidence = this.db.prepare(`
      INSERT OR IGNORE INTO topic_evidence(standing_topic_key, signal_id, day, relevance_score, matched_keywords, ai_category, created_at)
      VALUES(?, ?, ?, ?, ?, ?, ?);
    `);

    const now = isoNow();

    for (const signal of signals) {
      const combined = `${signal.title} ${signal.summary || ''}`.toLowerCase();
      const signalKeywords = safeParseJson(signal.keywords_json, []);

      for (const st of standingTopics) {
        const stKeywords = safeParseJson(st.keywords, []);
        const matched = [];
        let score = 0;

        for (const kw of stKeywords) {
          const kwLower = String(kw).toLowerCase();
          if (combined.includes(kwLower)) {
            matched.push(kw);
            // Multi-word keyword matches are worth more
            score += kwLower.includes(' ') ? 2 : 1;
          }
        }

        // Also check keyword overlap
        for (const sk of signalKeywords) {
          const skLower = String(sk).toLowerCase();
          for (const stk of stKeywords) {
            if (String(stk).toLowerCase() === skLower && !matched.includes(stk)) {
              matched.push(stk);
              score += 0.5;
            }
          }
        }

        // Only create evidence link if there's meaningful overlap
        if (score >= 2 || matched.length >= 2) {
          const relevance = Math.min(1.0, score / 8);
          const aiCat = st.section === 'AI' ? classifyAICategory(combined) : null;
          insertEvidence.run(
            st.topic_key,
            signal.signal_id,
            day,
            relevance,
            safeJson(matched, []),
            aiCat,
            now
          );
        }
      }
    }
  }

  // ── Get evidence for a standing topic over a rolling window ──
  getTopicEvidence(topicKey, days = 7) {
    return this.db.prepare(`
      SELECT te.*, s.title AS signal_title, s.summary AS signal_summary, s.canonical_url, s.citations_json, s.published_at
      FROM topic_evidence te
      JOIN signals s ON s.signal_id = te.signal_id
      WHERE te.standing_topic_key = ?
      ORDER BY te.day DESC, te.relevance_score DESC
      LIMIT ?;
    `).all(topicKey, days * 20);
  }

  // ── Get evidence counts per standing topic for a day ──
  getEvidenceSummary(day) {
    return this.db.prepare(`
      SELECT te.standing_topic_key, st.label, st.category, st.section,
             COUNT(*) AS evidence_count,
             AVG(te.relevance_score) AS avg_relevance,
             MAX(te.relevance_score) AS max_relevance
      FROM topic_evidence te
      JOIN standing_topics st ON st.topic_key = te.standing_topic_key
      WHERE te.day = ?
      GROUP BY te.standing_topic_key
      ORDER BY evidence_count DESC;
    `).all(day);
  }

  getLatestDay() {
    const row = this.db.prepare('SELECT day FROM editions ORDER BY day DESC LIMIT 1').get();
    return row?.day || '';
  }

  getEdition(day, yearsForward, options = {}) {
    const row = this.db.prepare('SELECT payload_json FROM editions WHERE day=? AND years_forward=?').get(day, yearsForward);
    if (!row) return null;
    const parsed = safeParseJson(row.payload_json, null);
    if (!parsed) return null;
    if (options && options.applyCuration === false) return parsed;
    return this.applyStoryCurationsToEditionPayload(parsed);
  }

  getStoryCuration(storyId) {
    const row = this.db
      .prepare('SELECT story_id, generated_at, model, key_story, plan_json, article_json FROM story_curations WHERE story_id=? LIMIT 1')
      .get(storyId);
    if (!row) return null;
    const plan = safeParseJson(row.plan_json, null);
    return {
      storyId: row.story_id,
      generatedAt: row.generated_at || null,
      model: row.model || null,
      key: Boolean(row.key_story),
      plan,
      article: row.article_json ? safeParseJson(row.article_json, null) : null
    };
  }

  getStoryCurationsByIds(storyIds) {
    const ids = Array.isArray(storyIds) ? storyIds.map((v) => String(v || '').trim()).filter(Boolean) : [];
    if (!ids.length) return [];
    const uniq = Array.from(new Set(ids));
    const placeholders = uniq.map(() => '?').join(', ');
    const sql = `
      SELECT story_id, generated_at, model, key_story, plan_json
      FROM story_curations
      WHERE story_id IN (${placeholders});
    `;
    return this.db.prepare(sql).all(...uniq);
  }

  applyStoryCurationsToEditionPayload(payload) {
    const base = payload && typeof payload === 'object' ? payload : null;
    if (!base) return payload;
    const articles = Array.isArray(base.articles) ? base.articles : [];
    if (!articles.length) return base;

    const ids = Array.from(new Set(articles.map((a) => String(a?.id || '').trim()).filter(Boolean)));
    if (!ids.length) return base;

    const rows = this.getStoryCurationsByIds(ids);
    if (!rows.length) return base;

    const map = new Map();
    let heroOverride = '';
    let maxStoryCurationAt = '';
    for (const row of rows) {
      const plan = safeParseJson(row.plan_json, null);
      if (plan && plan.hero) heroOverride = String(row.story_id || '');
      const gen = String(row.generated_at || '').trim();
      if (gen && (!maxStoryCurationAt || gen > maxStoryCurationAt)) {
        maxStoryCurationAt = gen;
      }
      map.set(String(row.story_id || ''), {
        storyId: String(row.story_id || ''),
        generatedAt: row.generated_at || null,
        model: row.model || null,
        key: Boolean(row.key_story),
        plan
      });
    }

    const patchedArticles = articles.map((a) => {
      const baseArticle = a && typeof a === 'object' ? { ...a } : a;
      const id = String(baseArticle?.id || '').trim();
      const c = id ? map.get(id) : null;
      if (!c || !c.plan) {
        // Preserve the full edition. Curations are applied opportunistically; uncurated
        // stories remain present with default confidence and no curation metadata.
        if (baseArticle && typeof baseArticle === 'object') {
          const conf = Number(baseArticle.confidence);
          baseArticle.confidence = Number.isFinite(conf) ? conf : 0;
          if (!('curation' in baseArticle)) baseArticle.curation = null;
        }
        return baseArticle;
      }
      const curatedTitle = String(c.plan.curatedTitle || c.plan.title || '').trim();
      const curatedDek = String(c.plan.curatedDek || c.plan.dek || '').trim();
      const draftBody = c.plan.draftArticle?.body || '';
      const conf = Number(c.plan.confidence);
      const confClamped = Number.isFinite(conf) ? Math.max(0, Math.min(100, Math.round(conf))) : 0;
      return {
        ...baseArticle,
        title: curatedTitle || baseArticle.title,
        dek: curatedDek || baseArticle.dek,
        body: (draftBody && draftBody.length > 100) ? draftBody : (baseArticle.body || ''),
        confidence: confClamped,
        curation: {
          key: Boolean(c.key) || Boolean(c.plan.key),
          hero: Boolean(c.plan.hero),
          model: c.model,
          generatedAt: c.generatedAt,
          curatedTitle: curatedTitle || '',
          curatedDek: curatedDek || '',
          topicTitle: c.plan.topicTitle || c.plan.topicSeed || '',
          sparkDirections: c.plan.sparkDirections || '',
          futureEventSeed: c.plan.futureEventSeed || '',
          confidence: confClamped,
          outline: Array.isArray(c.plan.outline) ? c.plan.outline.slice(0, 10) : [],
          draftArticle: c.plan.draftArticle || null
        }
      };
    });

    // Preserve the full edition so each section keeps coverage even when curation
    // confidence lands at zero for some stories.
    const finalArticles = patchedArticles;

    const resolvedHeroId = heroOverride || base.heroId || base.heroStoryId || (finalArticles[0] ? finalArticles[0].id : null);
    const dayCuration = this.getDayCuration(base.day);
    const curationGeneratedAt = dayCuration?.generatedAt || maxStoryCurationAt || null;
    return {
      ...base,
      heroId: resolvedHeroId,
      heroStoryId: resolvedHeroId,
      curationGeneratedAt,
      curation: dayCuration
        ? {
            generatedAt: dayCuration.generatedAt,
            provider: dayCuration.provider,
            model: dayCuration.model,
            error: dayCuration.error
          }
        : curationGeneratedAt
          ? { generatedAt: curationGeneratedAt, provider: null, model: null, error: null }
          : null,
      articles: finalArticles
    };
  }

  listSources() {
    const rows = this.db.prepare('SELECT source_id, name, type, section, url, enabled, fetch_interval_minutes, last_fetched_at, last_error, last_status, last_item_count FROM sources ORDER BY source_id').all();
    return rows || [];
  }

  getStatus() {
    const latest = this.getLatestDay();
    const day = latest || formatDay();
    const counts = {
      day,
      lastRefresh: this.lastRefresh,
      rawItems: this.db.prepare('SELECT COUNT(1) AS n FROM raw_items WHERE day=?').get(day)?.n || 0,
      signals: this.db.prepare('SELECT COUNT(1) AS n FROM signals WHERE day=?').get(day)?.n || 0,
      topics: this.db.prepare('SELECT COUNT(1) AS n FROM topics WHERE day=?').get(day)?.n || 0,
      editions: this.db.prepare('SELECT COUNT(1) AS n FROM editions WHERE day=?').get(day)?.n || 0,
      curations: this.db.prepare('SELECT COUNT(1) AS n FROM story_curations WHERE day=?').get(day)?.n || 0
    };
    return counts;
  }

  getDaySignalSnapshot(day) {
    const normalized = normalizeDay(day) || formatDay();
    const row = this.db.prepare('SELECT snapshot_json FROM day_signal_snapshots WHERE day=? LIMIT 1').get(normalized);
    if (!row) return null;
    return safeParseJson(row.snapshot_json, null);
  }

  ensureDaySignalSnapshot(day) {
    const normalized = normalizeDay(day) || formatDay();
    const existing = this.getDaySignalSnapshot(normalized);
    if (existing) return existing;
    this.storeDaySignalSnapshot(normalized);
    return this.getDaySignalSnapshot(normalized);
  }

  buildDaySignalSnapshot(day) {
    const normalized = normalizeDay(day) || formatDay();
    const sources = this.listSources();

    const rawItems = this.db.prepare(`
      SELECT raw_id, source_id, published_at, canonical_url, title, summary, section_hint
      FROM raw_items
      WHERE day=?
      ORDER BY COALESCE(published_at, fetched_at) DESC, raw_id DESC;
    `).all(normalized);

    const signals = this.db.prepare(`
      SELECT signal_id, raw_id, section, signal_type, title, published_at, canonical_url, summary, horizon_bucket, score, citations_json
      FROM signals
      WHERE day=?
      ORDER BY score DESC, signal_id DESC;
    `).all(normalized);

    const topics = this.db.prepare(`
      SELECT topic_id, section, label, brief, horizon_bucket, topic_slug, evidence_links_json, score
      FROM topics
      WHERE day=?
      ORDER BY score DESC, topic_id DESC;
    `).all(normalized);

    const editions = this.db.prepare(`
      SELECT years_forward, version, generated_at
      FROM editions
      WHERE day=?
      ORDER BY years_forward ASC;
    `).all(normalized);

    return {
      schema: 1,
      day: normalized,
      generatedAt: isoNow(),
      counts: {
        rawItems: rawItems.length,
        signals: signals.length,
        topics: topics.length,
        editions: editions.length
      },
      sources,
      rawItems: rawItems.map((r) => ({
        rawId: r.raw_id,
        sourceId: r.source_id,
        sectionHint: r.section_hint || null,
        publishedAt: r.published_at || null,
        url: r.canonical_url || '',
        title: r.title,
        summary: String(r.summary || '').slice(0, 320)
      })),
      signals: signals.map((s) => ({
        signalId: s.signal_id,
        rawId: s.raw_id || null,
        section: s.section,
        type: s.signal_type,
        horizon: s.horizon_bucket || null,
        score: s.score ?? null,
        publishedAt: s.published_at || null,
        url: s.canonical_url || '',
        title: s.title,
        summary: String(s.summary || '').slice(0, 420),
        citations: safeParseJson(s.citations_json, [])
      })),
      topics: topics.map((t) => ({
        topicId: t.topic_id,
        section: t.section,
        slug: t.topic_slug,
        label: t.label,
        brief: t.brief,
        horizon: t.horizon_bucket,
        score: t.score ?? null,
        evidenceLinks: safeParseJson(t.evidence_links_json, [])
      })),
      editions
    };
  }

  storeDaySignalSnapshot(day) {
    const normalized = normalizeDay(day) || formatDay();
    const snapshot = this.buildDaySignalSnapshot(normalized);
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO day_signal_snapshots(day, generated_at, snapshot_json)
      VALUES(?, ?, ?);
    `);
    stmt.run(normalized, isoNow(), safeJson(snapshot, {}));
  }

  traceEvent(day, eventType, payload = null) {
    const normalized = normalizeDay(day) || formatDay();
    try {
      const stmt = this.db.prepare(`
        INSERT INTO day_event_traces(day, ts, event_type, payload_json)
        VALUES(?, ?, ?, ?);
      `);
      stmt.run(normalized, isoNow(), String(eventType || 'event'), payload ? safeJson(payload, {}) : null);
    } catch {
      // Tracing should never break the pipeline.
    }
  }

  getDayEventTrace(day, limit = 240) {
    const normalized = normalizeDay(day) || formatDay();
    const n = Math.max(10, Math.min(2000, Number(limit) || 240));
    const rows = this.db
      .prepare(
        `
        SELECT event_id, day, ts, event_type, payload_json
        FROM day_event_traces
        WHERE day=?
        ORDER BY event_id DESC
        LIMIT ?;
      `
      )
      .all(normalized, n);
    const list = (rows || [])
      .map((r) => ({
        id: r.event_id,
        day: r.day,
        ts: r.ts,
        type: r.event_type,
        payload: safeParseJson(r.payload_json, null)
      }))
      .reverse();
    return list;
  }

  getDayCuration(day) {
    const normalized = normalizeDay(day) || formatDay();
    const row = this.db
      .prepare('SELECT day, generated_at, provider, model, prompt_json, payload_json, error FROM day_curations WHERE day=? LIMIT 1')
      .get(normalized);
    if (!row) return null;
    return {
      day: row.day,
      generatedAt: row.generated_at,
      provider: row.provider,
      model: row.model,
      prompt: row.prompt_json ? safeParseJson(row.prompt_json, null) : null,
      payload: row.payload_json ? safeParseJson(row.payload_json, null) : null,
      error: row.error || null
    };
  }

  storeDayCuration(day, { provider, model, prompt, payload, error }) {
    const normalized = normalizeDay(day) || formatDay();
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO day_curations(day, generated_at, provider, model, prompt_json, payload_json, error)
      VALUES(?, ?, ?, ?, ?, ?, ?);
    `);
    stmt.run(
      normalized,
      isoNow(),
      String(provider || 'unknown'),
      String(model || 'unknown'),
      prompt ? safeJson(prompt, {}) : null,
      safeJson(payload || {}, {}),
      error ? String(error) : null
    );
  }

  getStory(storyId) {
    const row = this.db.prepare(`
      SELECT s.story_id, s.section, s.rank, s.angle, s.headline_seed, s.dek_seed, s.evidence_pack_json,
             e.day as day, e.years_forward as years_forward, e.payload_json as payload_json,
             c.generated_at as curation_generated_at, c.model as curation_model, c.key_story as curation_key_story,
             c.plan_json as curation_plan_json
      FROM edition_stories s
      JOIN editions e ON e.edition_id = s.edition_id
      LEFT JOIN story_curations c ON c.story_id = s.story_id
      WHERE s.story_id=?
      LIMIT 1;
    `).get(storyId);
    if (!row) return null;
    const curationPlan = row.curation_plan_json ? safeParseJson(row.curation_plan_json, null) : null;
    const curatedTitle = curationPlan ? String(curationPlan.curatedTitle || curationPlan.title || '').trim() : '';
    const curatedDek = curationPlan ? String(curationPlan.curatedDek || curationPlan.dek || '').trim() : '';
    return {
      storyId: row.story_id,
      day: row.day,
      yearsForward: row.years_forward,
      section: row.section,
      rank: row.rank,
      angle: row.angle,
      headlineSeed: curatedTitle || row.headline_seed,
      dekSeed: curatedDek || row.dek_seed,
      evidencePack: safeParseJson(row.evidence_pack_json, {}),
      curation: curationPlan
        ? {
            ...curationPlan,
            generatedAt: row.curation_generated_at || curationPlan.generatedAt || null,
            model: row.curation_model || curationPlan.model || null,
            key: Boolean(row.curation_key_story) || Boolean(curationPlan.key),
            hero: Boolean(curationPlan.hero)
          }
        : null
    };
  }

  getRendered(storyId) {
    return this.getRenderedVariant(storyId, {});
  }

  getRenderedVariant(storyId, options = {}) {
    const id = String(storyId || '').trim();
    if (!id) return null;
    const override = String(options.curationGeneratedAt || options.curationAt || '').trim();
    let variant = override ? renderVariantFromCurationGeneratedAt(override) : '';
    if (!variant) {
      const row = this.db.prepare('SELECT generated_at FROM story_curations WHERE story_id=? LIMIT 1').get(id);
      variant = renderVariantFromCurationGeneratedAt(row?.generated_at || '');
    }
    const cacheKey = `${id}|${variant}|r${RENDER_CACHE_VERSION}`;
    const row = this.db.prepare('SELECT article_json FROM render_cache WHERE cache_key=? LIMIT 1').get(cacheKey);
    if (row) return safeParseJson(row.article_json, null);

    // Back-compat for older caches that did not include a curation variant.
    if (variant === 'c0') {
      const legacyKey = `${id}|r${RENDER_CACHE_VERSION}`;
      const legacy = this.db.prepare('SELECT article_json FROM render_cache WHERE cache_key=? LIMIT 1').get(legacyKey);
      if (legacy) {
        const parsed = safeParseJson(legacy.article_json, null);
        if (parsed) {
          // Store under the new key for future lookups.
          try {
            this.storeRendered(id, parsed, { curationGeneratedAt: '' });
          } catch {
            // ignore
          }
        }
        return parsed;
      }
    }
    return null;
  }

  buildRenderCacheKey(storyId, options = {}) {
    const id = String(storyId || '').trim();
    const override = String(options.curationGeneratedAt || options.curationAt || '').trim();
    if (override) {
      return `${id}|${renderVariantFromCurationGeneratedAt(override)}|r${RENDER_CACHE_VERSION}`;
    }
    const row = this.db.prepare('SELECT generated_at FROM story_curations WHERE story_id=? LIMIT 1').get(id);
    return `${id}|${renderVariantFromCurationGeneratedAt(row?.generated_at || '')}|r${RENDER_CACHE_VERSION}`;
  }

  storeRendered(storyId, article, options = {}) {
    const cacheKey = this.buildRenderCacheKey(storyId, {
      curationGeneratedAt: options.curationGeneratedAt || article?.curationGeneratedAt || article?.curation?.generatedAt || null
    });
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO render_cache(cache_key, story_id, generated_at, article_json)
      VALUES(?, ?, ?, ?);
    `);
    stmt.run(cacheKey, storyId, isoNow(), safeJson(article, {}));
  }

  async ensureDayBuilt(day) {
    const normalized = normalizeDay(day) || formatDay();
    const existing = this.db.prepare('SELECT COUNT(1) AS n FROM editions WHERE day=?').get(normalized)?.n || 0;
    if (existing > 0) {
      // Serve existing editions immediately; refresh in the background if this process has not yet run.
      if (!this.lastRefresh) {
        void this.refresh({ day: normalized, force: false }).catch(() => {});
      }
      return normalized;
    }
    await this.refresh({ day: normalized, force: false });
    return normalized;
  }

  async refresh(options = {}) {
    const day = normalizeDay(options.day) || formatDay();
    const force = options.force === true;
    const inflightKey = day;
    if (this.refreshInFlightByDay.has(inflightKey)) {
      return this.refreshInFlightByDay.get(inflightKey);
    }

    const promise = (async () => {
      const startedAtMs = Date.now();
      this.traceEvent(day, 'refresh.start', { day, force });
      const fetchedAt = isoNow();
      const sources = this.db
        .prepare('SELECT source_id, name, type, section, url, enabled, fetch_interval_minutes, last_fetched_at FROM sources WHERE enabled=1')
        .all();
      const insertRaw = this.db.prepare(`
        INSERT OR IGNORE INTO raw_items(source_id, day, fetched_at, published_at, canonical_url, title, summary, payload_json, fingerprint, section_hint)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
      `);
      const updateSourceOk = this.db.prepare(`
        UPDATE sources SET last_fetched_at=?, last_error=NULL, last_status=?, last_item_count=? WHERE source_id=?;
      `);
      const updateSourceErr = this.db.prepare(`
        UPDATE sources SET last_fetched_at=?, last_error=?, last_status=? WHERE source_id=?;
      `);

      const nowMs = Date.now();
      const timeoutMs = Math.max(2500, Math.min(12000, Number(process.env.PIPELINE_FETCH_TIMEOUT_MS || 6500)));
      const concurrency = Math.max(2, Math.min(10, Number(process.env.PIPELINE_FETCH_CONCURRENCY || 6)));

      const shouldFetchSource = (source) => {
        if (force) return true;
        const intervalMin = Number(source.fetch_interval_minutes || 60);
        if (!Number.isFinite(intervalMin) || intervalMin <= 0) return true;
        const lastIso = String(source.last_fetched_at || '').trim();
        if (!lastIso) return true;
        const lastMs = Date.parse(lastIso);
        if (!Number.isFinite(lastMs)) return true;
        return nowMs - lastMs >= intervalMin * 60 * 1000;
      };

      const processSource = async (source) => {
        const sourceId = source.source_id;
        const url = String(source.url || '');
        const sectionHint = source.section ? normalizeSection(source.section) : null;

        if (!shouldFetchSource(source)) {
          return;
        }

        try {
          let items = [];
          if (String(source.type).toLowerCase() === 'rss') {
            const resp = await fetchWithTimeout(url, {
              timeoutMs,
              headers: { 'user-agent': 'FutureTimesBot/1.0', accept: 'application/xml,text/xml,*/*' }
            });
            const status = resp.status;
            if (!resp.ok) throw new Error(`RSS ${status}`);
            const xml = await resp.text();
            items = parseFeed(xml).slice(0, 40);
            for (const item of items) {
              const canonicalUrl = canonicalizeUrl(item.link);
              const pubDay = item.publishedAt ? formatDay(item.publishedAt) : day;
              const fingerprint = sha256Hex(`${String(item.title).toLowerCase()}|${canonicalUrl}|${pubDay}`);
              insertRaw.run(
                sourceId,
                day,
                fetchedAt,
                item.publishedAt || null,
                canonicalUrl,
                item.title.slice(0, 240),
                (item.summary || '').slice(0, 1200),
                null,
                fingerprint,
                sectionHint
              );
            }
            updateSourceOk.run(fetchedAt, status, items.length, sourceId);
            return;
          }

          if (String(source.type).toLowerCase() === 'api_json' && String(sourceId).includes('polymarket')) {
            items = await fetchPolymarketRawItems(url, fetchedAt);
            for (const item of items.slice(0, 220)) {
              const canonicalUrl = canonicalizeUrl(item.link);
              const pubDay = item.publishedAt ? formatDay(item.publishedAt) : day;
              const fingerprint = sha256Hex(`${String(item.title).toLowerCase()}|${canonicalUrl}|${pubDay}`);
              insertRaw.run(
                sourceId,
                day,
                fetchedAt,
                item.publishedAt || null,
                canonicalUrl,
                item.title.slice(0, 240),
                (item.summary || '').slice(0, 1200),
                safeJson(item.payloadJson || null, null),
                fingerprint,
                sectionHint
              );
            }
            updateSourceOk.run(fetchedAt, 200, items.length, sourceId);
            return;
          }

          if (String(source.type).toLowerCase() === 'csv' && String(sourceId).includes('fred')) {
            const seriesId = new URL(url).searchParams.get('id') || '';
            const item = await fetchFredSeriesRawItem(url, fetchedAt, seriesId);
            const canonicalUrl = canonicalizeUrl(item.link);
            const pubDay = day;
            const fingerprint = sha256Hex(`${String(item.title).toLowerCase()}|${canonicalUrl}|${pubDay}`);
            insertRaw.run(
              sourceId,
              day,
              fetchedAt,
              item.publishedAt || null,
              canonicalUrl,
              item.title.slice(0, 240),
              (item.summary || '').slice(0, 1200),
              safeJson(item.payloadJson || null, null),
              fingerprint,
              sectionHint || 'Business'
            );
            updateSourceOk.run(fetchedAt, 200, 1, sourceId);
            return;
          }

          // Unknown source type; skip.
          updateSourceOk.run(fetchedAt, 200, 0, sourceId);
        } catch (err) {
          updateSourceErr.run(fetchedAt, String(err?.message || 'fetch failed'), 0, sourceId);
        }
      };

      let cursor = 0;
      const workerCount = Math.min(concurrency, sources.length || 1);
      const workers = Array.from({ length: workerCount }, async () => {
        while (true) {
          const idx = cursor++;
          if (idx >= sources.length) break;
          await processSource(sources[idx]);
        }
      });
      await Promise.all(workers);

      this.processSignalsForDay(day);
      this.matchSignalsToStandingTopics(day);
      this.buildTopicsForDay(day);
      this.buildEditionsForDay(day);
      this.storeDaySignalSnapshot(day);
      this.lastRefresh = isoNow();

      const status = this.getStatus();
      this.traceEvent(day, 'refresh.end', {
        day,
        force,
        elapsedMs: Date.now() - startedAtMs,
        counts: {
          rawItems: status.rawItems,
          signals: status.signals,
          topics: status.topics,
          editions: status.editions
        }
      });
    })().finally(() => {
      this.refreshInFlightByDay.delete(inflightKey);
    });

    this.refreshInFlightByDay.set(inflightKey, promise);
    return promise;
  }

  processSignalsForDay(day) {
    const rows = this.db.prepare(`
      SELECT r.raw_id, r.source_id, r.published_at, r.canonical_url, r.title, r.summary, r.payload_json, r.section_hint,
             s.type AS source_type, s.name AS source_name, s.url AS source_url
      FROM raw_items r
      JOIN sources s ON s.source_id = r.source_id
      WHERE r.day=?
        AND NOT EXISTS (SELECT 1 FROM signals x WHERE x.raw_id = r.raw_id)
      ORDER BY r.raw_id ASC;
    `).all(day);

    const insert = this.db.prepare(`
      INSERT INTO signals(raw_id, day, section, signal_type, title, published_at, summary, canonical_url, entities_json, keywords_json, horizon_bucket, score, citations_json)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    `);

    for (const row of rows) {
      const source = {
        source_id: row.source_id,
        name: row.source_name,
        type: row.source_type,
        url: row.source_url
      };
      const signalType = SIGNAL_TYPE_BY_SOURCE(source);
      const title = String(row.title || '').trim();
      const summary = String(row.summary || '').trim();
      const combined = `${title}\n${summary}`;
      const section = normalizeSection(row.section_hint) || classifySection(combined, 'World');
      const keywords = extractKeywords(combined, 14);
      const entities = extractEntities(combined, this.entityDicts);
      const horizon = HORIZON_BY_SIGNAL_TYPE[signalType] || 'near';
      const score = scoreSignal(signalType, row.published_at, title);
      const canonicalUrl = canonicalizeUrl(row.canonical_url || '');
      const citations = [
        {
          url: canonicalUrl,
          title,
          source: row.source_name || row.source_id,
          publishedAt: row.published_at || null
        }
      ];

      insert.run(
        row.raw_id,
        day,
        section,
        signalType,
        title.slice(0, 240),
        row.published_at || null,
        summary.slice(0, 1200),
        canonicalUrl,
        safeJson(entities, []),
        safeJson(keywords, []),
        horizon,
        score,
        safeJson(citations, [])
      );
    }

    // Derived econ: yield curve spread (DGS10 - DGS2) if present.
    const econSignals = this.db.prepare(`
      SELECT title, summary, canonical_url, citations_json
      FROM signals
      WHERE day=? AND signal_type='econ'
      ORDER BY signal_id DESC;
    `).all(day);

    const findVal = (id) => {
      const match = econSignals.find((s) => String(s.title || '').includes(id));
      if (!match) return null;
      const m = String(match.summary || '').match(/:\s*([0-9]+(?:\.[0-9]+)?)/);
      if (!m) return null;
      return Number(m[1]);
    };

    const dgs10 = findVal('DGS10');
    const dgs2 = findVal('DGS2');
    if (Number.isFinite(dgs10) && Number.isFinite(dgs2)) {
      const spread = dgs10 - dgs2;
      const exists = this.db.prepare(`SELECT COUNT(1) AS n FROM signals WHERE day=? AND title='Economic indicator YC_SPREAD_10Y2Y'`).get(day)?.n || 0;
      if (!exists) {
        const citations = [
          { url: 'https://fred.stlouisfed.org/series/DGS10', title: 'FRED DGS10', source: 'FRED', publishedAt: null },
          { url: 'https://fred.stlouisfed.org/series/DGS2', title: 'FRED DGS2', source: 'FRED', publishedAt: null }
        ];
        this.db.prepare(`
          INSERT INTO signals(raw_id, day, section, signal_type, title, summary, canonical_url, entities_json, keywords_json, horizon_bucket, score, citations_json)
          VALUES(NULL, ?, 'Business', 'econ', 'Economic indicator YC_SPREAD_10Y2Y', ?, '', '[]', '[]', 'near', 0.92, ?);
        `).run(day, `10y-2y spread: ${spread.toFixed(2)} (derived)`, safeJson(citations, []));
      }
    }
  }

  buildTopicsForDay(day) {
    const insert = this.db.prepare(`
      INSERT OR REPLACE INTO topics(day, section, label, brief, horizon_bucket, topic_slug, evidence_signal_ids_json, evidence_links_json, score)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
    `);

    // Clear existing topics for a rebuild (keeps day deterministic on refresh).
    this.db.prepare('DELETE FROM topics WHERE day=?').run(day);

    for (const section of SECTION_ORDER) {
      const signals = this.db.prepare(`
        SELECT signal_id, title, summary, canonical_url, entities_json, keywords_json, horizon_bucket, score, citations_json, published_at
        FROM signals
        WHERE day=? AND section=? AND signal_type NOT IN ('market', 'econ')
        ORDER BY score DESC;
      `).all(day, section);

      if (!signals.length) continue;

      const topics = [];

      for (const signal of signals.slice(0, 60)) {
        const tokens = tokenize(signal.title);
        const entities = safeParseJson(signal.entities_json, []);
        let bestIdx = -1;
        let bestSim = 0;
        let bestSharedEntity = false;
        for (let i = 0; i < topics.length; i++) {
          const candidate = topics[i];
          const sim = jaccard(tokens, candidate.tokens);
          const sharedEntity = entities.some((e) => candidate.entities.has(e));
          if (sim > bestSim) {
            bestSim = sim;
            bestIdx = i;
            bestSharedEntity = sharedEntity;
          }
        }

        const shouldMerge = bestIdx >= 0 && (bestSim >= 0.36 || (bestSharedEntity && bestSim >= 0.18));
        if (shouldMerge) {
          const t = topics[bestIdx];
          t.signals.push(signal);
          for (const e of entities) t.entities.add(e);
          for (const w of tokens) t.tokens.add(w);
          t.score = Math.max(t.score, signal.score || 0);
          continue;
        }

        const t = {
          label: signal.title,
          section,
          signals: [signal],
          tokens: new Set(tokens),
          entities: new Set(entities),
          score: signal.score || 0
        };
        topics.push(t);
        if (topics.length >= 24) break;
      }

      const usedSlugs = new Set();
      for (const t of topics) {
        const evidenceSignals = t.signals.slice(0, 6);
        const label = String(t.label || '').slice(0, 220);
        const brief = buildTopicBrief(evidenceSignals);
        const horizon = pickDeterministic(evidenceSignals.map((s) => s.horizon_bucket), `${day}-${label}`) || 'near';
        let slug = slugify(label, 58);
        if (usedSlugs.has(slug)) {
          slug = `${slug}-${(stableHash(label) % 97) + 2}`;
        }
        usedSlugs.add(slug);

        const evidenceIds = evidenceSignals.map((s) => s.signal_id);
        const evidenceLinks = evidenceSignals.map((s) => ({
          title: s.title,
          url: s.canonical_url || ''
        }));

        insert.run(
          day,
          section,
          label,
          brief,
          horizon,
          slug,
          safeJson(evidenceIds, []),
          safeJson(evidenceLinks, []),
          t.score
        );
      }
    }
  }

  buildEditionsForDay(day) {
    const topics = this.db.prepare(`
      SELECT topic_id, day, section, label, brief, horizon_bucket, topic_slug, evidence_signal_ids_json, score
      FROM topics
      WHERE day=?
      ORDER BY score DESC;
    `).all(day);

    const signalsById = new Map();
    const allSignals = this.db.prepare(`
      SELECT signal_id, section, signal_type, title, summary, canonical_url, citations_json, published_at
      FROM signals
      WHERE day=?
    `).all(day);
    for (const s of allSignals) signalsById.set(s.signal_id, s);
    const econSignals = allSignals.filter((s) => s.signal_type === 'econ');
    const marketSignals = allSignals.filter((s) => s.signal_type === 'market');

    const insertEdition = this.db.prepare(`
      INSERT INTO editions(day, years_forward, generated_at, payload_json, version)
      VALUES(?, ?, ?, ?, ?)
      ON CONFLICT(day, years_forward) DO UPDATE SET
        generated_at=excluded.generated_at,
        payload_json=excluded.payload_json,
        version=excluded.version;
    `);

    const insertStory = this.db.prepare(`
      INSERT OR REPLACE INTO edition_stories(story_id, edition_id, section, rank, topic_id, angle, headline_seed, dek_seed, evidence_pack_json)
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);
    `);

    // Only build +5y edition
    for (const yearsForward of [5]) {
      const mix = chooseHorizonMix(yearsForward);
      const editionDate = formatEditionDate(day, yearsForward);
      const version = sha256Hex(`${day}|${yearsForward}|v1`).slice(0, 12);
      const chosen = [];
      const usedTopicIds = new Set();
      const usedLabelTokens = [];

      const pickTopicForSection = (section, needed) => {
        const sectionTopics = topics
          .filter((t) => t.section === section)
          .sort((a, b) => (b.score || 0) - (a.score || 0));

        const usedThemes = new Set();

        // Pick from a top pool then shuffle deterministically by year to avoid identical editions.
        const TOP_POOL = Math.max(needed * 3, 18);
        const topPool = sectionTopics.slice(0, TOP_POOL);
        const byBucket = {
          near: topPool.filter((t) => t.horizon_bucket === 'near'),
          mid: topPool.filter((t) => t.horizon_bucket === 'mid'),
          long: topPool.filter((t) => t.horizon_bucket === 'long')
        };

        const want = {
          near: Math.round(needed * mix.near),
          mid: Math.round(needed * mix.mid),
          long: Math.max(0, needed - Math.round(needed * mix.near) - Math.round(needed * mix.mid))
        };

        const selected = [];
        const tryFill = (bucketName, count) => {
          const pool = byBucket[bucketName] || [];
          const seed = `${day}|${yearsForward}|${section}|${bucketName}`;
          const sorted = [...pool].sort((a, b) => stableHash(`${seed}|${a.topic_slug}`) - stableHash(`${seed}|${b.topic_slug}`));

          for (const t of sorted) {
            if (selected.length >= count) break;
            if (usedTopicIds.has(t.topic_id)) continue;
            const theme = deriveThemePhrase(t.label, t.brief);
            if (theme && usedThemes.has(theme)) continue;
            const tokens = tokenize(t.label);
            const tooSimilar = usedLabelTokens.some((prev) => jaccard(tokens, prev) >= 0.55);
            if (tooSimilar) continue;
            selected.push(t);
            usedTopicIds.add(t.topic_id);
            usedLabelTokens.push(tokens);
            if (theme) usedThemes.add(theme);
          }
        };

        tryFill('near', want.near);
        tryFill('mid', want.mid);
        tryFill('long', want.long);

        // Fill remaining from any bucket.
        if (selected.length < needed) {
          const remaining = needed - selected.length;
          const seed = `${day}|${yearsForward}|${section}|any`;
          const pool = topPool.length ? topPool : sectionTopics;
          const sorted = [...pool].sort((a, b) => stableHash(`${seed}|${a.topic_slug}`) - stableHash(`${seed}|${b.topic_slug}`));
          for (const t of sorted) {
            if (selected.length >= needed) break;
            if (usedTopicIds.has(t.topic_id)) continue;
            const theme = deriveThemePhrase(t.label, t.brief);
            if (theme && usedThemes.has(theme)) continue;
            const tokens = tokenize(t.label);
            const tooSimilar = usedLabelTokens.some((prev) => jaccard(tokens, prev) >= 0.55);
            if (tooSimilar) continue;
            selected.push(t);
            usedTopicIds.add(t.topic_id);
            usedLabelTokens.push(tokens);
            if (theme) usedThemes.add(theme);
          }

          // As a last resort: allow less strict similarity if still short.
          if (selected.length < needed) {
            for (const t of sorted) {
              if (selected.length >= needed) break;
              if (usedTopicIds.has(t.topic_id)) continue;
              selected.push(t);
              usedTopicIds.add(t.topic_id);
            }
          }
          if (selected.length < needed) {
            // Fill with random picks (deterministic) from all topics, preserving uniqueness.
            const backup = topics.filter((t) => !usedTopicIds.has(t.topic_id));
            for (let i = 0; i < remaining && backup.length; i++) {
              const pick = pickDeterministic(backup, `${seed}|backup|${i}`);
              if (!pick) break;
              selected.push(pick);
              usedTopicIds.add(pick.topic_id);
            }
          }
        }

        return selected.slice(0, needed);
      };

      const storiesForEdition = [];
      for (const section of SECTION_ORDER) {
        // ── HYBRID PATH: AI section uses standing topics + evidence ──
        if (section === 'AI') {
          const aiStandingTopics = this.getStandingTopics('AI');
          const aiStories = this._buildAISectionStories({
            day, yearsForward, editionDate, aiStandingTopics,
            signalsById, econSignals, marketSignals
          });
          storiesForEdition.push(...aiStories);
          continue;
        }

        // ── EMERGENT PATH: all other sections use clustered topics ──
        const sectionPicks = pickTopicForSection(section, 5);
        for (let i = 0; i < sectionPicks.length; i++) {
          const topic = sectionPicks[i];
          const evidenceIds = safeParseJson(topic.evidence_signal_ids_json, []);
          const evidenceSignals = evidenceIds.map((id) => signalsById.get(id)).filter(Boolean);
          const relatedMarket = marketSignals
            .map((s) => ({ s, sim: jaccard(tokenize(topic.label), tokenize(`${s.title} ${s.summary}`)) }))
            .sort((a, b) => b.sim - a.sim)
            .filter((row) => row.sim >= 0.22)
            .slice(0, 4)
            .map((row) => row.s);

          const evidencePack = buildEvidencePack({
            topic,
            evidenceSignals,
            econSignals,
            marketSignals: relatedMarket,
            editionDate,
            yearsForward
          });

          const angle = ANGLES[i % ANGLES.length];
          const storyId = buildStoryId(day, yearsForward, section, topic.topic_slug, angle);
          const headlineSeed = buildHeadlineSeed(topic.label, topic.brief, yearsForward, storyId);
          const dekSeed = buildDekSeed(topic.label, topic.brief, yearsForward, editionDate, day);
          const meta = `${section} • ${editionDate}`;

          storiesForEdition.push({
            storyId,
            section,
            rank: i + 1,
            angle,
            topicId: topic.topic_id,
            topicLabel: topic.label,
            title: headlineSeed,
            dek: dekSeed,
            meta,
            image: '',
            prompt: `Editorial photo illustration of: ${topic.label}. Newspaper photography style. Dated ${editionDate}.`,
            evidencePack
          });
        }
      }

      const hero = storiesForEdition.find((s) => s.section === 'U.S.') || storiesForEdition[0];
      const payload = {
        schema: 2,
        day,
        offsetYears: yearsForward,
        date: editionDate,
        generatedFrom: `signals-pipeline / ${day}`,
        version,
        heroId: hero ? hero.storyId : null,
        heroStoryId: hero ? hero.storyId : null,
        // Maintain existing client shape (articles[]).
        articles: storiesForEdition.map((s) => ({
          id: s.storyId,
          section: s.section,
          title: s.title,
          dek: s.dek,
          image: s.image,
          meta: s.meta,
          prompt: s.prompt,
          topicLabel: s.topicLabel
        })),
        marketsSummary: marketSignals.slice(0, 4).map((s) => ({ label: s.title, prob: s.summary || '' })),
        econSummary: econSignals.slice(0, 6).map((s) => ({ label: s.title, value: s.summary || '' }))
      };

      insertEdition.run(day, yearsForward, isoNow(), safeJson(payload, {}), version);
      const editionIdRow = this.db.prepare('SELECT edition_id FROM editions WHERE day=? AND years_forward=?').get(day, yearsForward);
      const editionId = editionIdRow?.edition_id;
      if (!editionId) continue;

      // Replace existing stories for this edition.
      this.db.prepare('DELETE FROM edition_stories WHERE edition_id=?').run(editionId);
      for (const s of storiesForEdition) {
        insertStory.run(
          s.storyId,
          editionId,
          s.section,
          s.rank,
          s.topicId,
          s.angle,
          s.title,
          s.dek,
          safeJson(s.evidencePack, {})
        );
      }
    }
  }

  // ── Build AI section stories from standing topics + evidence ──
  _buildAISectionStories({ day, yearsForward, editionDate, aiStandingTopics, signalsById, econSignals, marketSignals }) {
    const section = 'AI';
    const stories = [];
    const baselineYear = 2026;
    const targetYear = baselineYear + (Number(yearsForward) || 0);

    // Sort standing topics by amount of fresh evidence (most active first)
    const topicsWithEvidence = aiStandingTopics.map((st) => {
      const evidence = this.db.prepare(`
        SELECT te.signal_id, te.relevance_score, te.matched_keywords, te.ai_category,
               s.title, s.summary, s.canonical_url, s.citations_json, s.published_at
        FROM topic_evidence te
        JOIN signals s ON s.signal_id = te.signal_id
        WHERE te.standing_topic_key = ? AND te.day = ?
        ORDER BY te.relevance_score DESC
        LIMIT 8;
      `).all(st.topic_key, day);

      return { ...st, evidence, evidenceCount: evidence.length };
    });

    // Sort: topics with most evidence first, but always include all standing topics
    topicsWithEvidence.sort((a, b) => b.evidenceCount - a.evidenceCount);

    // Take top 6 standing topics (one per category ideally)
    const usedCategories = new Set();
    const selected = [];
    // First pass: pick one from each category that has evidence
    for (const st of topicsWithEvidence) {
      if (selected.length >= 6) break;
      if (st.category && usedCategories.has(st.category)) continue;
      selected.push(st);
      if (st.category) usedCategories.add(st.category);
    }
    // Second pass: fill remaining slots
    for (const st of topicsWithEvidence) {
      if (selected.length >= 6) break;
      if (selected.some((s) => s.topic_key === st.topic_key)) continue;
      selected.push(st);
    }

    for (let i = 0; i < selected.length; i++) {
      const st = selected[i];
      const axes = safeParseJson(st.extrapolation_axes, []);
      const milestones = safeParseJson(st.milestones, []);
      const stKeywords = safeParseJson(st.keywords, []);

      // Build evidence signals for the evidence pack
      const evidenceSignals = st.evidence.map((e) => ({
        signal_id: e.signal_id,
        title: e.title,
        summary: e.summary,
        canonical_url: e.canonical_url,
        citations_json: e.citations_json,
        published_at: e.published_at
      }));

      // Find relevant milestone for this year offset
      const relevantMilestone = milestones.find((m) => Math.abs((m.year || 0) - targetYear) <= 1);

      // Build a rich brief from standing topic description + evidence
      const evidenceBrief = st.evidence.slice(0, 3).map((e) => {
        const summary = String(e.summary || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
        return `- ${summary.slice(0, 200)}`;
      }).join('\n');

      const topicBrief = `${st.description || ''}\n\nLatest evidence:\n${evidenceBrief || '(No fresh signals today)'}`;

      // Build headline that uses standing topic label + extrapolation
      const topicSlug = slugify(st.topic_key, 58);
      const angle = ANGLES[i % ANGLES.length];
      const storyId = buildStoryId(day, yearsForward, section, topicSlug, angle);

      let headline;
      if (yearsForward === 0) {
        // Current day: use latest evidence as headline driver
        const topSignal = st.evidence[0];
        headline = topSignal ? cleanTopicForHeadline(topSignal.title) : st.label;
      } else {
        // Future: extrapolate from standing topic
        headline = this._buildAIFutureHeadline(st, targetYear, relevantMilestone, storyId);
      }

      let dek;
      if (yearsForward === 0) {
        dek = st.evidence.length
          ? `${st.label}: ${st.evidence.length} signals tracked today. ${st.description?.slice(0, 120) || ''}`
          : st.description || st.label;
      } else {
        dek = this._buildAIFutureDek(st, targetYear, baselineYear, relevantMilestone, axes, editionDate);
      }

      const meta = `AI${st.category ? ' / ' + st.category : ''} • ${editionDate}`;

      // Build evidence pack with standing-topic enrichments
      const citations = evidenceSignals.slice(0, 6).map((s, idx) => ({
        id: `c${idx + 1}`,
        title: s.title,
        url: s.canonical_url || '',
        source: (safeParseJson(s.citations_json, [])[0] || {}).source || '',
        publishedAt: s.published_at || null,
        summary: String(s.summary || '').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 280)
      }));

      const evidencePack = {
        grounding: 'standing_topic',
        section,
        editionDate,
        yearsForward,
        standingTopic: {
          key: st.topic_key,
          label: st.label,
          category: st.category,
          description: st.description,
          extrapolationAxes: axes,
          milestones: milestones.filter((m) => m.year >= targetYear - 1 && m.year <= targetYear + 2),
          keywords: stKeywords.slice(0, 12)
        },
        topic: {
          topicId: st.topic_key,
          label: st.label,
          theme: st.category || 'AI and Automation',
          brief: topicBrief,
          horizon: yearsForward <= 2 ? 'near' : yearsForward <= 5 ? 'mid' : 'long'
        },
        citations,
        markets: [],
        econ: {},
        signals: evidenceSignals.slice(0, 6).map((s) => ({
          label: s.title,
          value: (safeParseJson(s.citations_json, [])[0] || {}).source || ''
        })),
        aiCategory: st.category,
        evidenceCount: st.evidenceCount
      };

      stories.push({
        storyId,
        section,
        rank: i + 1,
        angle,
        topicId: st.topic_key,
        topicLabel: st.label,
        title: headline,
        dek,
        meta,
        image: '',
        prompt: `Futuristic editorial illustration: ${st.label} in ${targetYear}. ${st.category || 'AI'} theme. Newspaper photography style.`,
        evidencePack
      });
    }

    return stories;
  }

  _buildAIFutureHeadline(standingTopic, targetYear, milestone, seed) {
    const label = standingTopic.label || 'AI';
    const category = standingTopic.category || '';

    if (milestone?.event) {
      // Use the milestone as the headline basis
      const event = String(milestone.event).slice(0, 80);
      return `${targetYear}: ${event}`;
    }

    const templates = [
      `${label} Reaches New Milestone in ${targetYear}`,
      `${targetYear}: ${label} Enters a New Phase`,
      `${category || label} Industry Crosses Critical Threshold in ${targetYear}`,
      `The ${targetYear} ${label} Landscape: What's Changed`,
      `${label} Deployment Accelerates as ${targetYear} Reshapes the Market`,
      `New ${label} Capabilities Arrive Ahead of Schedule in ${targetYear}`
    ];
    return templates[stableHash(seed) % templates.length];
  }

  _buildAIFutureDek(standingTopic, targetYear, baselineYear, milestone, axes, editionDate) {
    const label = standingTopic.label || 'AI';

    if (milestone?.event) {
      return `By ${editionDate}, ${String(milestone.event).toLowerCase()}. The pace of change in ${label.toLowerCase()} continues to surprise even optimistic forecasters.`;
    }

    if (axes.length) {
      const axisNames = axes.slice(0, 2).map((a) => a.axis || a.description || '').filter(Boolean).join(' and ');
      return `Advances in ${axisNames} are reshaping ${label.toLowerCase()} in ${targetYear}, with concrete implications for industries, workers, and policymakers worldwide.`;
    }

    return `The ${label.toLowerCase()} landscape of ${targetYear} has evolved rapidly, with new capabilities, market dynamics, and regulatory frameworks redefining the field.`;
  }

  buildCurationSnapshot(day) {
    const normalized = normalizeDay(day) || formatDay();
    const snap = this.ensureDaySignalSnapshot(normalized);
    const topics = Array.isArray(snap?.topics) ? snap.topics : [];
    const signals = Array.isArray(snap?.signals) ? snap.signals : [];

    const topicsBySection = {};
    for (const s of SECTION_ORDER) topicsBySection[s] = [];
    for (const t of topics) {
      const section = String(t.section || 'World');
      if (!topicsBySection[section]) topicsBySection[section] = [];
      topicsBySection[section].push({
        label: t.label,
        brief: t.brief,
        score: t.score ?? null,
        horizon: t.horizon ?? null
      });
    }
    for (const [section, list] of Object.entries(topicsBySection)) {
      topicsBySection[section] = (list || [])
        .sort((a, b) => (Number(b.score) || 0) - (Number(a.score) || 0))
        .slice(0, 8);
    }

    const pickSignals = (filterFn, limit) =>
      signals
        .filter(filterFn)
        .sort((a, b) => (Number(b.score) || 0) - (Number(a.score) || 0))
        .slice(0, limit)
        .map((s) => ({
          title: s.title,
          source: (Array.isArray(s.citations) && s.citations[0] ? s.citations[0].source || '' : '') || '',
          type: s.type,
          score: s.score ?? null
        }));

    return {
      day: normalized,
      generatedAt: isoNow(),
      topicsBySection,
      topSignals: pickSignals((s) => s.type !== 'market' && s.type !== 'econ', 14),
      marketSignals: pickSignals((s) => s.type === 'market', 10),
      econSignals: pickSignals((s) => s.type === 'econ', 10)
    };
  }

  listEditionStoryCandidates(day, yearsForward) {
    const normalized = normalizeDay(day) || formatDay();
    const rows = this.db
      .prepare(
        `
        SELECT s.story_id, s.section, s.rank, s.angle, s.headline_seed, s.dek_seed, s.evidence_pack_json
        FROM edition_stories s
        JOIN editions e ON e.edition_id = s.edition_id
        WHERE e.day=? AND e.years_forward=?
        ORDER BY s.rank ASC, s.section ASC, s.story_id ASC;
      `
      )
      .all(normalized, yearsForward);

    return (rows || []).map((r) => {
      const pack = safeParseJson(r.evidence_pack_json, {});
      return {
        storyId: r.story_id,
        section: r.section,
        rank: r.rank,
        angle: r.angle,
        title: r.headline_seed,
        dek: r.dek_seed,
        topicLabel: pack?.topic?.label || '',
        topic: pack?.topic || null,
        evidencePack: pack
      };
    });
  }

  async curateDay(day, options = {}) {
    const normalized = normalizeDay(day) || formatDay();
    const force = options.force === true;

    if (this.curationInFlight) {
      return this.curationInFlight;
    }

    this.curationInFlight = (async () => {
      const startedAtMs = Date.now();
      this.traceEvent(normalized, 'curate.start', { day: normalized, force });
      await this.ensureDayBuilt(normalized);

      const existingDay = this.db.prepare('SELECT generated_at, error FROM day_curations WHERE day=? LIMIT 1').get(normalized);
      if (!force && existingDay && !existingDay.error) {
        this.traceEvent(normalized, 'curate.skip', { day: normalized, reason: 'already_curated', generatedAt: existingDay.generated_at });
        this.lastCuration = isoNow();
        return { ok: true, day: normalized, skipped: true, reason: 'already_curated', generatedAt: existingDay.generated_at };
      }

      const existing = this.db.prepare('SELECT COUNT(1) AS n FROM story_curations WHERE day=?').get(normalized)?.n || 0;
      if (!force && existing >= 30) {
        this.traceEvent(normalized, 'curate.skip', { day: normalized, reason: 'story_curations_present', existing });
        this.lastCuration = isoNow();
        return { ok: true, day: normalized, skipped: true, reason: 'story_curations_present', existing };
      }

      const config = getOpusCurationConfigFromEnv();
      const snapshot = this.buildCurationSnapshot(normalized);

      const upsert = this.db.prepare(`
        INSERT OR REPLACE INTO story_curations(
          story_id, day, years_forward, section, rank, generated_at, model, key_story, plan_json, article_json
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
      `);

      let curatedStories = 0;
      let keyStories = 0;
      let editionCount = 0;
      const errors = [];
      const editionPrompts = {};
      const editionPlans = {};

      // Only curate +5y edition
      for (const yearsForward of [5]) {
        const edition = this.getEdition(normalized, yearsForward, { applyCuration: false });
        if (!edition) continue;
        const editionDate = String(edition.date || formatEditionDate(normalized, yearsForward));
        const candidates = this.listEditionStoryCandidates(normalized, yearsForward);
        if (!candidates.length) continue;

        let plan;
        const keyCount = config.keyStoriesPerEdition;
        const prompt = buildEditionCurationPrompt({
          day: normalized,
          yearsForward,
          editionDate,
          candidates,
          snapshot,
          keyCount
        });
        editionPrompts[String(yearsForward)] = { yearsForward, editionDate, keyCount, prompt };

        const editionStartedAtMs = Date.now();
        this.traceEvent(normalized, 'curate.edition.start', { yearsForward, editionDate, candidates: candidates.length, mode: config.mode, model: config.model });
        try {
          plan = await generateEditionCurationPlan({
            day: normalized,
            yearsForward,
            editionDate,
            candidates,
            snapshot,
            keyCount,
            prompt,
            config
          });
        } catch (err) {
          const errorText = String(err?.message || err);
          errors.push({ yearsForward, error: errorText });
          this.traceEvent(normalized, 'curate.edition.error', {
            yearsForward,
            editionDate,
            elapsedMs: Date.now() - editionStartedAtMs,
            error: errorText
          });
          continue;
        }
        editionPlans[String(yearsForward)] = plan;
        this.traceEvent(normalized, 'curate.edition.end', {
          yearsForward,
          editionDate,
          elapsedMs: Date.now() - editionStartedAtMs,
          stories: Array.isArray(plan?.stories) ? plan.stories.length : 0,
          keyStoryIds: Array.isArray(plan?.keyStoryIds) ? plan.keyStoryIds.slice(0, 10) : []
        });

        const planStories = Array.isArray(plan?.stories) ? plan.stories : [];
        const byId = new Map(planStories.map((s) => [String(s?.storyId || '').trim(), s]));
        const keySet = new Set((Array.isArray(plan?.keyStoryIds) ? plan.keyStoryIds : []).map((s) => String(s || '').trim()));

        const generatedAt = isoNow();
        const model = String(plan?.model || config.model || '').trim() || config.model;

        for (const candidate of candidates) {
          const storyId = String(candidate.storyId || '').trim();
          if (!storyId) continue;
          const entry = byId.get(storyId) || {};
          const key = Boolean(entry.key) || keySet.has(storyId);
          const hero = Boolean(entry.hero);
          const curatedTitle = String(entry.curatedTitle || '').trim() || String(candidate.title || '').trim();
          const curatedDek = String(entry.curatedDek || '').trim() || String(candidate.dek || '').trim();

          const normalizedPlan = {
            schema: 1,
            day: normalized,
            yearsForward,
            editionDate,
            storyId,
            section: candidate.section,
            rank: candidate.rank,
            angle: candidate.angle,
            curatedTitle,
            curatedDek,
            key,
            hero,
            topicTitle:
              String(entry.topicTitle || entry.topicSeed || '').trim() ||
              String(candidate?.evidencePack?.topic?.theme || candidate?.evidencePack?.topic?.label || '').trim(),
            sparkDirections: String(entry.sparkDirections || '').trim(),
            futureEventSeed: String(entry.futureEventSeed || '').trim(),
            outline: Array.isArray(entry.outline) ? entry.outline.slice(0, 8) : [],
            extrapolationTrace: Array.isArray(entry.extrapolationTrace)
              ? entry.extrapolationTrace.slice(0, 8).map((x) => String(x || '').trim()).filter(Boolean)
              : [],
            rationale: Array.isArray(entry.rationale) ? entry.rationale.slice(0, 8).map((x) => String(x || '').trim()).filter(Boolean) : [],
            confidence: Number.isFinite(Number(entry.confidence)) ? Math.max(0, Math.min(100, Math.round(Number(entry.confidence)))) : 0,
            draftArticle: entry.draftArticle && typeof entry.draftArticle === 'object' ? {
              title: String(entry.draftArticle.title || '').trim(),
              dek: String(entry.draftArticle.dek || '').trim(),
              body: String(entry.draftArticle.body || '').trim()
            } : null
          };

          let articleJson = null;
          const draft = entry && entry.draftArticle && typeof entry.draftArticle === 'object' ? entry.draftArticle : null;
          const draftBody = draft ? String(draft.body || '').trim() : '';
          if (draftBody) {
            const pack = candidate.evidencePack || {};
            const signals = Array.isArray(pack.signals) ? pack.signals : [];
            const markets = Array.isArray(pack.markets) ? pack.markets : [];
            const citations = Array.isArray(pack.citations) ? pack.citations : [];
            const title = String(draft.title || curatedTitle || candidate.title || '').trim() || curatedTitle;
            const dek = String(draft.dek || curatedDek || candidate.dek || '').trim() || curatedDek;
            const meta = `${candidate.section} • ${editionDate}`;
            articleJson = {
              id: storyId,
              section: candidate.section,
              title,
              dek,
              meta,
              image: '',
              body: draftBody,
              signals,
              markets,
              prompt: `Editorial photo illustration prompt: ${title}. Documentary realism. Dated ${editionDate}.`,
              citations,
              stats: { econ: pack.econ || {}, markets: pack.markets || [] },
              editionDate,
              generatedFrom: `opus-curator / ${normalized}`,
              generatedAt,
              curationGeneratedAt: generatedAt,
              yearsForward
            };

            // Generate an image for hero/key stories if OpenAI key is available
            if (hero || key) {
              const imgPrompt = String(draft.title || curatedTitle || '').trim();
              const imgPath = await generateStoryImage(imgPrompt, storyId);
              if (imgPath) {
                articleJson.image = imgPath;
              }
            }

            // Pre-populate the render cache so this story loads instantly.
            this.storeRendered(storyId, articleJson, { curationGeneratedAt: generatedAt });
            if (key) keyStories++;
          }

          upsert.run(
            storyId,
            normalized,
            yearsForward,
            candidate.section,
            candidate.rank,
            generatedAt,
            model,
            key ? 1 : 0,
            safeJson(normalizedPlan, {}),
            articleJson ? safeJson(articleJson, {}) : null
          );
          curatedStories++;
        }

        // ── Backfill: generate bodies for curated stories that came back without draftArticle ──
        const cappedIds = (candidates || []).slice(0, 20);
        const missingBodyStories = [];
        for (const candidate of cappedIds) {
          const sid = String(candidate.storyId || '').trim();
          if (!sid) continue;
          const entry = byId.get(sid) || {};
          const hasDraft = entry.draftArticle && typeof entry.draftArticle === 'object' && String(entry.draftArticle.body || '').trim().length > 50;
          if (!hasDraft) {
            const conf = Number(entry.confidence ?? 0);
            if (conf <= 0) continue;
            missingBodyStories.push({
              storyId: sid,
              title: String(entry.curatedTitle || candidate.title || '').trim(),
              dek: String(entry.curatedDek || candidate.dek || '').trim(),
              sparkDirections: String(entry.sparkDirections || '').trim(),
              editionDate,
              section: candidate.section
            });
          }
        }

        if (missingBodyStories.length > 0) {
          this.traceEvent(normalized, 'curate.backfill.start', { yearsForward, missing: missingBodyStories.length });
          const backfilled = await generateMissingArticleBodies(missingBodyStories, config);
          this.traceEvent(normalized, 'curate.backfill.end', { yearsForward, generated: backfilled.size });

          for (const [sid, draft] of backfilled) {
            const candidate = cappedIds.find(c => String(c.storyId || '').trim() === sid);
            if (!candidate) continue;
            const entry = byId.get(sid) || {};
            const curatedTitle = String(entry.curatedTitle || candidate.title || '').trim();
            const curatedDek = String(entry.curatedDek || candidate.dek || '').trim();
            const pack = candidate.evidencePack || {};
            const title = String(draft.title || curatedTitle).trim() || curatedTitle;
            const dek = String(draft.dek || curatedDek).trim() || curatedDek;
            const backfilledArticle = {
              id: sid,
              section: candidate.section,
              title,
              dek,
              meta: `${candidate.section} • ${editionDate}`,
              image: '',
              body: draft.body,
              signals: Array.isArray(pack.signals) ? pack.signals : [],
              markets: Array.isArray(pack.markets) ? pack.markets : [],
              prompt: `Editorial photo illustration prompt: ${title}. Documentary realism. Dated ${editionDate}.`,
              citations: Array.isArray(pack.citations) ? pack.citations : [],
              stats: { econ: pack.econ || {}, markets: pack.markets || [] },
              editionDate,
              generatedFrom: `sonnet-backfill / ${normalized}`,
              generatedAt: isoNow(),
              curationGeneratedAt: generatedAt,
              yearsForward
            };
            this.storeRendered(sid, backfilledArticle, { curationGeneratedAt: generatedAt });

            // Update the story_curations row with the backfilled article
            const existingRow = this.db.prepare('SELECT plan_json FROM story_curations WHERE story_id=? AND day=? AND years_forward=?').get(sid, normalized, yearsForward);
            if (existingRow) {
              const existingPlan = safeParseJson(existingRow.plan_json, {});
              existingPlan.draftArticle = { title, dek, body: draft.body };
              this.db.prepare('UPDATE story_curations SET plan_json=?, article_json=? WHERE story_id=? AND day=? AND years_forward=?')
                .run(safeJson(existingPlan, {}), safeJson(backfilledArticle, {}), sid, normalized, yearsForward);
            }
          }
        }

        editionCount++;

        // Bake curated titles/deks into the stored edition payload so that
        // subsequent cold-start Vercel instances serve curated content
        // without needing access to story_curations in /tmp.
        const editionRow = this.db.prepare('SELECT payload_json FROM editions WHERE day=? AND years_forward=?').get(normalized, yearsForward);
        if (editionRow) {
          const editionPayload = safeParseJson(editionRow.payload_json, null);
          if (editionPayload) {
            const patched = this.applyStoryCurationsToEditionPayload(editionPayload);
            this.db.prepare(`
              UPDATE editions SET payload_json=?, generated_at=? WHERE day=? AND years_forward=?
            `).run(safeJson(patched, {}), isoNow(), normalized, yearsForward);
          }
        }
      }

      this.lastCuration = isoNow();
      const hasFatalError = curatedStories === 0 && errors.length > 0;
      this.storeDayCuration(normalized, {
        provider: String(config.mode || 'mock'),
        model: String(config.model || ''),
        prompt: {
          schema: 1,
          day: normalized,
          generatedAt: isoNow(),
          systemPrompt: config.systemPrompt || null,
          editions: editionPrompts
        },
        payload: {
          schema: 1,
          day: normalized,
          generatedAt: isoNow(),
          provider: String(config.mode || ''),
          model: String(config.model || ''),
          keyStoriesPerEdition: config.keyStoriesPerEdition,
          editions: editionPlans,
          stats: { editions: editionCount, curatedStories, keyStories, errors }
        },
        error: hasFatalError ? JSON.stringify(errors).slice(0, 5000) : null
      });
      this.traceEvent(normalized, 'curate.end', {
        day: normalized,
        force,
        elapsedMs: Date.now() - startedAtMs,
        editions: editionCount,
        curatedStories,
        keyStories,
        errors: errors.length
      });
      return {
        ok: true,
        day: normalized,
        mode: config.mode,
        model: config.model,
        editions: editionCount,
        curatedStories,
        keyStories,
        errors
      };
    })().finally(() => {
      this.curationInFlight = null;
    });

    return this.curationInFlight;
  }

  listStoryCurations(day, options = {}) {
    const normalized = normalizeDay(day) || formatDay();
    const yearsForward = options.yearsForward;
    const limit = Math.max(10, Math.min(2000, Number(options.limit) || 800));
    const rows =
      yearsForward == null
        ? this.db
            .prepare(
              `
              SELECT story_id, day, years_forward, section, rank, generated_at, model, key_story, plan_json, article_json
              FROM story_curations
              WHERE day=?
              ORDER BY years_forward ASC, section ASC, rank ASC
              LIMIT ?;
            `
            )
            .all(normalized, limit)
        : this.db
            .prepare(
              `
              SELECT story_id, day, years_forward, section, rank, generated_at, model, key_story, plan_json, article_json
              FROM story_curations
              WHERE day=? AND years_forward=?
              ORDER BY section ASC, rank ASC
              LIMIT ?;
            `
            )
            .all(normalized, Number(yearsForward), limit);

    return (rows || []).map((r) => ({
      storyId: r.story_id,
      day: r.day,
      yearsForward: r.years_forward,
      section: r.section,
      rank: r.rank,
      generatedAt: r.generated_at,
      model: r.model,
      key: Boolean(r.key_story),
      plan: safeParseJson(r.plan_json, null),
      article: r.article_json ? safeParseJson(r.article_json, null) : null
    }));
  }

  overrideStoryCuration(storyId, patch = {}) {
    const id = String(storyId || '').trim();
    if (!id) throw new Error('storyId is required');
    const existing = this.db
      .prepare('SELECT story_id, day, years_forward, section, rank, model, key_story, plan_json, article_json FROM story_curations WHERE story_id=? LIMIT 1')
      .get(id);
    if (!existing) throw new Error('story_curation_not_found');

    const plan = safeParseJson(existing.plan_json, {}) || {};
    const nextPlan = {
      ...plan,
      curatedTitle: patch.curatedTitle != null ? String(patch.curatedTitle) : plan.curatedTitle,
      curatedDek: patch.curatedDek != null ? String(patch.curatedDek) : plan.curatedDek,
      topicTitle: patch.topicTitle != null ? String(patch.topicTitle) : plan.topicTitle,
      sparkDirections: patch.sparkDirections != null ? String(patch.sparkDirections) : plan.sparkDirections,
      futureEventSeed: patch.futureEventSeed != null ? String(patch.futureEventSeed) : plan.futureEventSeed,
      outline: Array.isArray(patch.outline) ? patch.outline.slice(0, 10) : plan.outline,
      extrapolationTrace: Array.isArray(patch.extrapolationTrace) ? patch.extrapolationTrace.slice(0, 10) : plan.extrapolationTrace,
      rationale: Array.isArray(patch.rationale) ? patch.rationale.slice(0, 10) : plan.rationale,
      key: patch.key != null ? Boolean(patch.key) : Boolean(plan.key),
      hero: patch.hero != null ? Boolean(patch.hero) : Boolean(plan.hero)
    };

    const keyStory = nextPlan.key ? 1 : 0;
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO story_curations(
        story_id, day, years_forward, section, rank, generated_at, model, key_story, plan_json, article_json
      )
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    `);
    const generatedAt = isoNow();
    stmt.run(
      id,
      existing.day,
      existing.years_forward,
      existing.section,
      existing.rank,
      generatedAt,
      existing.model,
      keyStory,
      safeJson(nextPlan, {}),
      existing.article_json
    );

    this.traceEvent(existing.day, 'curate.story.override', { storyId: id, key: Boolean(nextPlan.key), hero: Boolean(nextPlan.hero) });
    return { ok: true, storyId: id, day: existing.day, yearsForward: existing.years_forward, plan: nextPlan };
  }

  async curateEditionFromPrompt(day, yearsForward, options = {}) {
    const normalized = normalizeDay(day) || formatDay();
    const y = Number(yearsForward);
    if (!Number.isFinite(y) || y < 0 || y > 10) throw new Error('yearsForward must be 0..10');

    await this.ensureDayBuilt(normalized);
    const edition = this.getEdition(normalized, y, { applyCuration: false });
    if (!edition) throw new Error('edition_not_found');
    const editionDate = String(edition.date || formatEditionDate(normalized, y));
    const candidates = this.listEditionStoryCandidates(normalized, y);
    if (!candidates.length) throw new Error('no_candidates');

    const config = getOpusCurationConfigFromEnv();
    if (options.systemPrompt != null) {
      config.systemPrompt = String(options.systemPrompt || '').trim() || config.systemPrompt;
    }
    const snapshot = this.buildCurationSnapshot(normalized);
    const keyCount = Number.isFinite(Number(options.keyCount)) ? Number(options.keyCount) : config.keyStoriesPerEdition;
    let prompt = String(options.prompt || '').trim();
    if (!prompt) {
      // Auto-generate the standard curation prompt if none provided
      prompt = buildEditionCurationPrompt({
        day: normalized, yearsForward: y, editionDate, candidates, keyCount, snapshot
      });
    }

    this.traceEvent(normalized, 'curate.edition.custom.start', { yearsForward: y, editionDate, keyCount, mode: config.mode, model: config.model });
    const plan = await generateEditionCurationPlan({
      day: normalized,
      yearsForward: y,
      editionDate,
      candidates,
      snapshot,
      keyCount,
      prompt,
      config
    });
    this.traceEvent(normalized, 'curate.edition.custom.end', { yearsForward: y, editionDate, stories: Array.isArray(plan?.stories) ? plan.stories.length : 0 });

    // Store just like the normal loop.
    const upsert = this.db.prepare(`
      INSERT OR REPLACE INTO story_curations(
        story_id, day, years_forward, section, rank, generated_at, model, key_story, plan_json, article_json
      )
      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    `);
    const planStories = Array.isArray(plan?.stories) ? plan.stories : [];
    const byId = new Map(planStories.map((s) => [String(s?.storyId || '').trim(), s]));
    const keySet = new Set((Array.isArray(plan?.keyStoryIds) ? plan.keyStoryIds : []).map((s) => String(s || '').trim()));
    const generatedAt = isoNow();
    const model = String(plan?.model || config.model || '').trim() || config.model;

    for (const candidate of candidates) {
      const storyId = String(candidate.storyId || '').trim();
      if (!storyId) continue;
      const entry = byId.get(storyId) || {};
      const key = Boolean(entry.key) || keySet.has(storyId);
      const hero = Boolean(entry.hero);
      const curatedTitle = String(entry.curatedTitle || '').trim() || String(candidate.title || '').trim();
      const curatedDek = String(entry.curatedDek || '').trim() || String(candidate.dek || '').trim();

      const normalizedPlan = {
        schema: 1,
        day: normalized,
        yearsForward: y,
        editionDate,
        storyId,
        section: candidate.section,
        rank: candidate.rank,
        angle: candidate.angle,
        curatedTitle,
        curatedDek,
        key,
        hero,
        topicTitle:
          String(entry.topicTitle || entry.topicSeed || '').trim() ||
          String(candidate?.evidencePack?.topic?.theme || candidate?.evidencePack?.topic?.label || '').trim(),
        sparkDirections: String(entry.sparkDirections || '').trim(),
        futureEventSeed: String(entry.futureEventSeed || '').trim(),
        outline: Array.isArray(entry.outline) ? entry.outline.slice(0, 8) : [],
        extrapolationTrace: Array.isArray(entry.extrapolationTrace)
          ? entry.extrapolationTrace.slice(0, 8).map((x) => String(x || '').trim()).filter(Boolean)
          : [],
        rationale: Array.isArray(entry.rationale) ? entry.rationale.slice(0, 8).map((x) => String(x || '').trim()).filter(Boolean) : [],
        confidence: Number.isFinite(Number(entry.confidence)) ? Math.max(0, Math.min(100, Math.round(Number(entry.confidence)))) : 0,
        draftArticle: entry.draftArticle && typeof entry.draftArticle === 'object' ? {
          title: String(entry.draftArticle.title || '').trim(),
          dek: String(entry.draftArticle.dek || '').trim(),
          body: String(entry.draftArticle.body || '').trim()
        } : null
      };

      let articleJson = null;
      const draft = entry.draftArticle && typeof entry.draftArticle === 'object' ? entry.draftArticle : null;
      const draftBody = draft ? String(draft.body || '').trim() : '';
      if (draftBody) {
        const pack = candidate.evidencePack || {};
        const title = String(draft.title || curatedTitle || candidate.title || '').trim() || curatedTitle;
        const dek = String(draft.dek || curatedDek || candidate.dek || '').trim() || curatedDek;
        articleJson = {
          id: storyId,
          section: candidate.section,
          title,
          dek,
          meta: `${candidate.section} • ${editionDate}`,
          image: '',
          body: draftBody,
          signals: Array.isArray(pack.signals) ? pack.signals : [],
          markets: Array.isArray(pack.markets) ? pack.markets : [],
          prompt: `Editorial photo illustration prompt: ${title}. Documentary realism. Dated ${editionDate}.`,
          citations: Array.isArray(pack.citations) ? pack.citations : [],
          stats: { econ: pack.econ || {}, markets: pack.markets || [] },
          editionDate,
          generatedFrom: `opus-curator / ${normalized}`,
          generatedAt,
          curationGeneratedAt: generatedAt,
          yearsForward: y
        };
        this.storeRendered(storyId, articleJson, { curationGeneratedAt: generatedAt });
      }

      upsert.run(
        storyId,
        normalized,
        y,
        candidate.section,
        candidate.rank,
        generatedAt,
        model,
        key ? 1 : 0,
        safeJson(normalizedPlan, {}),
        articleJson ? safeJson(articleJson, {}) : null
      );
    }

    // Update (or create) day_curations record with this prompt+plan for just this edition.
    const existing = this.getDayCuration(normalized);
    const promptRecord = existing?.prompt && typeof existing.prompt === 'object' ? existing.prompt : { schema: 1, day: normalized, generatedAt: isoNow() };
    const payloadRecord = existing?.payload && typeof existing.payload === 'object' ? existing.payload : { schema: 1, day: normalized, generatedAt: isoNow() };
    promptRecord.editions = promptRecord.editions && typeof promptRecord.editions === 'object' ? promptRecord.editions : {};
    payloadRecord.editions = payloadRecord.editions && typeof payloadRecord.editions === 'object' ? payloadRecord.editions : {};
    promptRecord.systemPrompt = config.systemPrompt || promptRecord.systemPrompt || null;
    promptRecord.editions[String(y)] = { yearsForward: y, editionDate, keyCount, prompt };
    payloadRecord.provider = String(config.mode || payloadRecord.provider || '');
    payloadRecord.model = String(config.model || payloadRecord.model || '');
    payloadRecord.editions[String(y)] = plan;
    this.storeDayCuration(normalized, {
      provider: payloadRecord.provider || String(config.mode || ''),
      model: payloadRecord.model || String(config.model || ''),
      prompt: promptRecord,
      payload: payloadRecord,
      error: existing?.error || null
    });

    return { ok: true, day: normalized, yearsForward: y, editionDate, plan };
  }
}

async function fetchWithTimeout(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 9000);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, {
      headers: options.headers || {},
      signal: controller.signal
    });
  } finally {
    clearTimeout(timeout);
  }
}
