import crypto from 'node:crypto';

export const SECTION_ORDER = ['U.S.', 'World', 'Business', 'Technology', 'AI', 'Arts', 'Lifestyle', 'Opinion'];
export const ANGLES = ['impact', 'markets', 'policy', 'tech', 'society'];

export function clampYears(value) {
  const years = Number(value);
  if (!Number.isFinite(years)) return 5;
  return Math.max(0, Math.min(10, Math.round(years)));
}

export function formatDay(dateLike = new Date()) {
  const date = dateLike instanceof Date ? dateLike : new Date(dateLike);
  const yyyy = String(date.getFullYear()).padStart(4, '0');
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

export function normalizeDay(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return '';
  return raw;
}

export function isoNow() {
  return new Date().toISOString();
}

export function sha256Hex(text) {
  return crypto.createHash('sha256').update(String(text || ''), 'utf8').digest('hex');
}

export function stableHash(value) {
  const str = String(value || '');
  let hash = 2166136261;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0;
}

export function slugify(value, maxLen = 60) {
  const raw = String(value || '')
    .toLowerCase()
    .replace(/['"]/g, '')
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  if (!raw) return 'topic';
  return raw.length > maxLen ? raw.slice(0, maxLen).replace(/-+$/g, '') : raw;
}

export function canonicalizeUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  let url;
  try {
    url = new URL(raw);
  } catch {
    return raw;
  }
  url.hash = '';
  const drop = new Set(['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'ref', 'fbclid', 'gclid']);
  for (const key of Array.from(url.searchParams.keys())) {
    if (drop.has(key)) url.searchParams.delete(key);
  }
  return url.toString();
}

export function tokenize(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .map((w) => w.trim())
    .filter((w) => w.length >= 3 && !STOPWORDS.has(w));
}

const STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'into', 'over', 'after', 'their', 'they', 'them', 'will', 'what',
  'when', 'where', 'who', 'why', 'how', 'are', 'was', 'were', 'has', 'have', 'had', 'new', 'now', 'more', 'than', 'its',
  'as', 'at', 'by', 'in', 'on', 'to', 'of', 'a', 'an', 'is', 'it', 'be', 'or', 'up', 'down', 'out', 'about', 'not', 'no'
]);

export function jaccard(tokensA, tokensB) {
  const a = new Set(tokensA || []);
  const b = new Set(tokensB || []);
  if (!a.size || !b.size) return 0;
  let intersection = 0;
  for (const t of a) {
    if (b.has(t)) intersection++;
  }
  const union = a.size + b.size - intersection;
  return union ? intersection / union : 0;
}

export function generateFutureDescriptor(years) {
  if (years <= 2) return 'early-adjacent';
  if (years <= 5) return 'next-wave';
  if (years <= 8) return 'mid-cycle';
  return 'late-cycle';
}

export function formatEditionDate(day, yearsForward) {
  const base = new Date(`${day}T12:00:00.000Z`);
  if (Number.isFinite(Number(yearsForward))) {
    base.setUTCFullYear(base.getUTCFullYear() + Number(yearsForward));
  }
  const formatter = new Intl.DateTimeFormat('en-US', { month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
  return formatter.format(base);
}

export function pickDeterministic(items, seed) {
  if (!Array.isArray(items) || !items.length) return null;
  const idx = stableHash(seed) % items.length;
  return items[idx];
}

