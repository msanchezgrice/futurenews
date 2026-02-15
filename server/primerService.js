import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { EDITION_SEED } from './editionData.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, '..');
const DATA_DIR = path.resolve(PROJECT_ROOT, 'data');
const PRIMER_FILE = path.resolve(DATA_DIR, 'primer.json');

const DEFAULT_REFRESH_MS = Number(process.env.PRIMER_REFRESH_MS || 1000 * 60 * 30);
const DEFAULT_PRIMER_COUNT = Number(process.env.PRIMER_COUNT_PER_SECTION || 7);

const SECTION_FEEDS = {
  'U.S.': [
    process.env.FEED_US || 'https://www.reuters.com/world/us/feed',
    process.env.FEED_US_ALT || 'https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/business/rss.xml'
  ],
  World: [
    process.env.FEED_WORLD || 'https://www.reuters.com/world/world-news/feed',
    process.env.FEED_WORLD_ALT || 'https://feeds.bbci.co.uk/news/world/rss.xml'
  ],
  Business: [
    process.env.FEED_BUSINESS || 'https://www.reuters.com/finance/technology/feed',
    process.env.FEED_BUSINESS_ALT || 'https://feeds.marketwatch.com/marketwatch/topstories/'
  ],
  Arts: [
    process.env.FEED_ARTS || 'https://www.reuters.com/culture/movies/feed',
    process.env.FEED_ARTS_ALT || 'https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/arts/rss.xml'
  ],
  Lifestyle: [
    process.env.FEED_LIFESTYLE || 'https://www.reuters.com/lifestyle/health/feed',
    process.env.FEED_LIFESTYLE_ALT || 'https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/health/rss.xml'
  ],
  Opinion: [
    process.env.FEED_OPINION || 'https://www.reuters.com/world/us/feed',
    process.env.FEED_OPINION_ALT || 'https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/opinion/rss.xml'
  ]
};

const state = {
  inFlight: null,
  lastLoaded: null,
  cached: null,
  lastFetchedAt: 0
};

function normalizeSection(section) {
  if (section === 'US') {
    return 'U.S.';
  }
  return section || 'U.S.';
}

function decodeHtml(value) {
  return String(value || '')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function cleanTag(value) {
  return String(value || '')
    .replace(/<[^>]*>/g, '')
    .replace(/<!\[CDATA\[|\]\]>/g, '')
    .trim();
}

function extractTag(block, tagName) {
  const pattern = new RegExp(`<${tagName}[^>]*>([\\s\\S]*?)<\\/${tagName}>`, 'i');
  const match = block.match(pattern);
  if (!match) return '';
  return decodeHtml(cleanTag(match[1]));
}

function extractDate(block) {
  const raw =
    extractTag(block, 'pubDate') ||
    extractTag(block, 'updated') ||
    extractTag(block, 'dc:date') ||
    '';
  const parsed = raw ? Date.parse(raw) : NaN;
  return Number.isFinite(parsed) ? parsed : Date.now();
}

function parseFeedItems(xml) {
  if (!xml || typeof xml !== 'string') return [];
  const itemBlocks = xml.match(/<item[\s\S]*?<\/item>/gi) || [];
  return itemBlocks.map((itemBlock) => {
    const title = extractTag(itemBlock, 'title');
    const description = extractTag(itemBlock, 'description');
    const link = extractTag(itemBlock, 'link');
    return {
      title,
      description,
      link,
      publishedAt: extractDate(itemBlock)
    };
  }).filter((entry) => entry.title && entry.description);
}

function readPrimerFromDisk() {
  try {
    if (!fs.existsSync(PRIMER_FILE)) return null;
    const raw = fs.readFileSync(PRIMER_FILE, 'utf8');
    const payload = JSON.parse(raw);
    if (!payload || !payload.generatedAt || !payload.sections) return null;
    return payload;
  } catch {
    return null;
  }
}

async function writePrimerToDisk(payload) {
  try {
    await fs.promises.mkdir(DATA_DIR, { recursive: true });
    await fs.promises.writeFile(PRIMER_FILE, JSON.stringify(payload, null, 2), 'utf8');
  } catch {
    // if writing fails, fallback to in-memory cache only.
  }
}

function buildFallback(seedSections, sectionName) {
  const bucket = [];
  seedSections
    .filter((entry) => entry.section === sectionName)
    .forEach((entry, index) => {
      bucket.push({
        title: `Seed theme ${index + 1}: ${entry.title}`,
        description: entry.dek || entry.baseMeta,
        link: '',
        publishedAt: Date.now() - index * 3600 * 1000
      });
    });
  return bucket;
}

function buildThemesFromItems(items) {
  const seen = new Set();
  const compact = [];
  for (const item of items) {
    const key = `${item.title}`.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    compact.push({
      title: item.title,
      summary: `${item.description}`.replace(/\s+/g, ' ').slice(0, 280),
      source: item.link || 'live feed',
      publishedAt: item.publishedAt || Date.now()
    });
    if (compact.length >= DEFAULT_PRIMER_COUNT) break;
  }
  return compact;
}

async function fetchFeed(url) {
  try {
    const response = await fetch(url, { headers: { 'user-agent': 'FutureTimesBot/1.0' } });
    if (!response.ok) {
      return [];
    }
    const xml = await response.text();
    return parseFeedItems(xml);
  } catch {
    return [];
  }
}

async function loadSectionNews(sectionName, sectionFeeds) {
  const normalSection = normalizeSection(sectionName);
  const allItems = [];
  const urls = Array.from(new Set(sectionFeeds));

  for (const url of urls) {
    const feedItems = await fetchFeed(url);
    allItems.push(...feedItems);
  }

  const curated = buildThemesFromItems(
    allItems.sort((a, b) => b.publishedAt - a.publishedAt)
  );

  if (!curated.length) {
    return buildFallback(EDITION_SEED.articles, normalSection);
  }

  return curated;
}

async function buildPrimerPayload() {
  const sections = {};
  for (const [sectionName, urls] of Object.entries(SECTION_FEEDS)) {
    const cleanSection = normalizeSection(sectionName);
    sections[cleanSection] = await loadSectionNews(cleanSection, urls);
  }

  return {
    generatedAt: new Date().toISOString(),
    generatedDay: new Date().toISOString().slice(0, 10),
    sections,
    schema: 1
  };
}

function isFresh(payload) {
  if (!payload || !payload.generatedAt) return false;
  const age = Date.now() - Date.parse(payload.generatedAt);
  return age >= 0 && age < DEFAULT_REFRESH_MS;
}

export async function getPrimerSnapshot(force = false) {
  const now = Date.now();
  if (!force && state.cached && now - state.lastFetchedAt < DEFAULT_REFRESH_MS) {
    return state.cached;
  }

  if (!force && state.inFlight) {
    return state.inFlight;
  }

  const fromDisk = readPrimerFromDisk();
  if (!force && fromDisk && isFresh(fromDisk)) {
    state.cached = fromDisk;
    state.lastFetchedAt = now;
    return state.cached;
  }

  state.inFlight = (async () => {
    const payload = await buildPrimerPayload();
    state.cached = payload;
    state.lastFetchedAt = now;
    state.inFlight = null;
    await writePrimerToDisk(payload);
    return payload;
  })();

  return state.inFlight;
}

export function getCachedPrimer() {
  return state.cached || readPrimerFromDisk();
}

export function getSectionPrimer(section, primer) {
  const resolvedPrimer = primer || getCachedPrimer();
  if (!resolvedPrimer || !resolvedPrimer.sections) return [];
  const normalized = normalizeSection(section);
  return resolvedPrimer.sections[normalized] || [];
}

export function getPrimerInfo() {
  return {
    file: PRIMER_FILE,
    ttlMs: DEFAULT_REFRESH_MS,
    intervalMs: DEFAULT_REFRESH_MS
  };
}

