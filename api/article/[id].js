import { getPipeline } from '../_lib/pipeline.js';
import { sendJson } from '../_lib/response.js';
import { clampYears, formatDay, normalizeDay } from '../../server/pipeline/utils.js';

function extractStoryId(req) {
  const fromQuery = req?.query?.id;
  if (fromQuery) return String(fromQuery);

  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const parts = url.pathname.split('/').filter(Boolean);
  return parts.length ? decodeURIComponent(parts[parts.length - 1]) : '';
}

function escapeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function buildSourcesList(citations) {
  const items = Array.isArray(citations) ? citations : [];
  if (!items.length) return '';
  const lines = ['Sources', ''];
  for (const c of items.slice(0, 12)) {
    const title = escapeText(c?.title || c?.url || 'Source');
    const source = escapeText(c?.source || '');
    const url = escapeText(c?.url || '');
    const suffix = [source, url].filter(Boolean).join(' • ');
    lines.push(`- ${title}${suffix ? ` (${suffix})` : ''}`);
  }
  return lines.join('\n');
}

function buildMockBody(story) {
  const pack = story?.evidencePack || {};
  const topic = pack.topic || {};
  const editionDate = escapeText(pack.editionDate || '') || escapeText(story?.day || formatDay());
  const yearsForward = Number.isFinite(Number(story?.yearsForward)) ? Number(story.yearsForward) : Number(pack.yearsForward || 0) || 0;
  const seed = story?.curation?.futureEventSeed || story?.curation?.sparkDirections || '';
  const brief = escapeText(topic.brief || '');
  const headlineHint = escapeText(story?.headlineSeed || topic.label || '');

  const citations = Array.isArray(pack.citations) ? pack.citations : [];
  const signals = Array.isArray(pack.signals) ? pack.signals : [];
  const markets = Array.isArray(pack.markets) ? pack.markets : [];

  const lines = [];
  lines.push(`## ${headlineHint || 'A future-dated dispatch'}`);
  lines.push('');
  if (seed) {
    lines.push(escapeText(seed));
    lines.push('');
  } else if (brief) {
    lines.push(brief);
    lines.push('');
  }

  lines.push(`By ${editionDate}, the story has shifted from signal to consequence.`);
  lines.push(`What began as a baseline item in ${escapeText(story?.day || '')} now plays out as policy, markets, and institutions adapt over ${yearsForward === 1 ? 'a year' : `${yearsForward} years`}.`);
  lines.push('');

  if (signals.length) {
    lines.push('### Signals');
    lines.push('');
    for (const s of signals.slice(0, 8)) {
      const label = escapeText(s?.label || '');
      const value = escapeText(s?.value || '');
      lines.push(`- ${label}${value ? ` (${value})` : ''}`);
    }
    lines.push('');
  }

  if (markets.length) {
    lines.push('### Market snapshot');
    lines.push('');
    for (const m of markets.slice(0, 8)) {
      const label = escapeText(m?.label || '');
      const prob = escapeText(m?.prob || '');
      lines.push(`- ${label}${prob ? `: ${prob}` : ''}`);
    }
    lines.push('');
  }

  const sources = buildSourcesList(citations);
  if (sources) {
    lines.push(sources);
    lines.push('');
  }

  return lines.join('\n');
}

function buildArticleFromStory(story) {
  const pack = story?.evidencePack || {};
  const editionDate = escapeText(pack.editionDate || '');
  const title = escapeText(story?.headlineSeed || 'Future Times story');
  const dek = escapeText(story?.dekSeed || '');
  const meta = [escapeText(story?.section || ''), editionDate].filter(Boolean).join(' • ') || escapeText(story?.section || '');

  return {
    id: story.storyId,
    section: story.section,
    title,
    dek,
    meta,
    image: 'assets/img/humanoids-labor-market.svg',
    body: buildMockBody(story),
    signals: Array.isArray(pack.signals) ? pack.signals : [],
    markets: Array.isArray(pack.markets) ? pack.markets : [],
    prompt: escapeText(story?.curation?.futureEventSeed)
      ? `Editorial photo illustration of: ${escapeText(story.curation.futureEventSeed)}. Newspaper photography style. Dated ${editionDate}.`
      : `Editorial photo illustration of: ${title}. Newspaper photography style. Dated ${editionDate}.`,
    citations: Array.isArray(pack.citations) ? pack.citations : [],
    stats: { econ: pack.econ || {}, markets: Array.isArray(pack.markets) ? pack.markets : [] },
    editionDate,
    generatedFrom: `vercel-api / ${escapeText(story?.day || '')}`,
    generatedAt: new Date().toISOString(),
    curationGeneratedAt: story?.curation?.generatedAt || null,
    yearsForward: story?.yearsForward
  };
}

export default async function handler(req, res) {
  if (req.method !== 'GET' && req.method !== 'POST') {
    sendJson(res, { error: 'method_not_allowed' }, 405);
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const years = clampYears(url.searchParams.get('years'));
  const day = normalizeDay(url.searchParams.get('day'));
  void years; // the story id is edition-specific; years is used by the client for navigation only.
  void day;

  const storyId = extractStoryId(req);
  if (!storyId) {
    sendJson(res, { status: 'not_found', error: 'Missing story id' }, 404);
    return;
  }

  const pipeline = getPipeline();

  let story = pipeline.getStory(storyId);
  if (!story) {
    const match = String(storyId).match(/^ft-(\\d{4}-\\d{2}-\\d{2})-y(\\d+)/);
    if (match) {
      try {
        await pipeline.ensureDayBuilt(match[1]);
      } catch {
        // ignore
      }
      story = pipeline.getStory(storyId);
    }
  }

  if (!story) {
    sendJson(res, { status: 'not_found', storyId, error: 'Unknown story id' }, 404);
    return;
  }

  const curationAt = story?.curation?.generatedAt || '';
  const cached =
    pipeline.getRenderedVariant(storyId, { curationGeneratedAt: curationAt }) ||
    pipeline.getStoryCuration(storyId)?.article ||
    null;

  if (cached) {
    sendJson(res, { status: 'ready', article: cached });
    return;
  }

  const article = buildArticleFromStory(story);
  try {
    pipeline.storeRendered(storyId, article, { curationGeneratedAt: curationAt });
  } catch {
    // ignore
  }

  sendJson(res, { status: 'ready', article });
}

