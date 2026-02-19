import { formatDay, normalizeDay } from '../pipeline/utils.js';

import { callAnthropicJson } from './anthropic.js';
import { getFutureImagesFlags, getIdeasModelCandidates } from './config.js';
import { stableStringify } from './json.js';
import { ensureFutureImagesSchema, pgQuery } from './postgres.js';
import { enqueueJob } from './jobs.js';

const IDEAS_TOOL = {
  name: 'deliver_future_ideas',
  description: 'Return the ranked future-object idea list for The Future Times as a JSON object.',
  inputSchema: {
    type: 'object',
    additionalProperties: false,
    properties: {
      schema: { type: 'integer' },
      day: { type: 'string' },
      yearsForward: { type: 'integer' },
      targetYear: { type: 'integer' },
      generatedAt: { type: 'string' },
      model: { type: 'string' },
      ideas: {
        type: 'array',
        items: {
          type: 'object',
          additionalProperties: false,
          properties: {
            rank: { type: 'integer' },
            score: { type: 'number' },
            confidence: { type: 'integer' },
            title: { type: 'string' },
            objectType: { type: 'string' },
            description: { type: 'string' },
            scene: { type: 'string' },
            sources: {
              type: 'object',
              additionalProperties: true,
              properties: {
                storyIds: { type: 'array', items: { type: 'string' } },
                signals: { type: 'array', items: { type: 'string' } }
              }
            }
          },
          required: ['rank', 'score', 'confidence', 'title', 'objectType', 'description', 'scene']
        }
      }
    },
    required: ['schema', 'day', 'yearsForward', 'targetYear', 'generatedAt', 'ideas']
  }
};

function clampText(value, max = 600) {
  const str = String(value || '').replace(/\s+/g, ' ').trim();
  if (!str) return '';
  if (str.length <= max) return str;
  return str.slice(0, max).replace(/[,\s;:.]+$/g, '').trim();
}

function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeIdea(idea, idx, baseRank = 1) {
  const rank = Number.isFinite(Number(idea?.rank))
    ? Math.max(1, Math.round(Number(idea.rank)))
    : Math.max(1, Math.round(Number(baseRank) || 1) + idx);
  const score = toNumber(idea?.score, 0);
  const confidence = Number.isFinite(Number(idea?.confidence))
    ? Math.max(0, Math.min(100, Math.round(Number(idea.confidence))))
    : 0;
  const title = clampText(idea?.title, 180) || `Idea ${rank}`;
  const objectType = clampText(idea?.objectType || idea?.object_type, 60) || 'object';
  const description = clampText(idea?.description, 900) || '';
  const scene = clampText(idea?.scene, 600) || '';
  const prompt = idea?.prompt && typeof idea.prompt === 'object' ? idea.prompt : {};
  const visualPrompt = clampText(prompt?.visualPrompt || prompt?.prompt, 1200) || clampText(description, 600) || title;
  const negativePrompt = clampText(
    prompt?.negativePrompt ||
      'no text, no logos, no watermarks, no UI overlays, no typographic elements, no charts, no memes, no frames',
    900
  );
  const size = clampText(prompt?.size || '1792x1024', 20) || '1792x1024';
  const style = clampText(prompt?.style || 'editorial_photo', 40) || 'editorial_photo';
  const sources = idea?.sources && typeof idea.sources === 'object' ? idea.sources : {};

  return {
    rank,
    score,
    confidence,
    title,
    objectType,
    description,
    scene,
    prompt: { visualPrompt, negativePrompt, size, style },
    sources
  };
}

function buildAttemptCounts(requestedCount) {
  const base = Math.max(1, Math.min(200, Math.round(Number(requestedCount) || 50)));
  if (base <= 30) return [base];
  return [base, 30];
}

function buildIdeasPrompt({ day, yearsForward, edition, snapshot, storyCurations, count, rankStart = 1, rankEnd = null, excludeTitles = [] }) {
  const editionDate = clampText(edition?.date || '', 60);
  const baselineYear = String(day).slice(0, 4) || '2026';
  const targetYear = String(Number(baselineYear) + Number(yearsForward));
  const rangeStart = Math.max(1, Math.round(Number(rankStart) || 1));
  const rangeEnd = rankEnd ? Math.max(rangeStart, Math.round(Number(rankEnd) || rangeStart)) : null;
  const rangeCount = rangeEnd ? rangeEnd - rangeStart + 1 : Math.max(1, Math.round(Number(count) || 1));
  const excludeList = Array.isArray(excludeTitles) ? excludeTitles.map((t) => clampText(t, 120)).filter(Boolean).slice(0, 60) : [];
  const articles = Array.isArray(edition?.articles) ? edition.articles : [];
  const topPerSection = new Map();
  for (const a of articles) {
    const sec = String(a?.section || '').trim();
    if (!sec || topPerSection.has(sec)) continue;
    topPerSection.set(sec, a);
  }

  const storyById = new Map();
  for (const s of storyCurations || []) {
    const id = String(s?.storyId || '').trim();
    if (!id) continue;
    storyById.set(id, s);
  }

  const sectionLines = Array.from(topPerSection.entries())
    .slice(0, 12)
    .map(([sec, a]) => {
      const id = String(a?.id || '').trim();
      const c = id ? storyById.get(id) : null;
      const plan = c?.plan || null;
      const conf = plan ? Number(plan.confidence) || 0 : Number(a?.confidence) || 0;
      const topic = clampText(plan?.topicTitle || plan?.topicSeed || a?.topicLabel || '', 80);
      const eventSeed = clampText(plan?.futureEventSeed || '', 160);
      const directions = clampText(plan?.sparkDirections || '', 220);
      return [
        `- section: ${sec}`,
        `  storyId: ${id}`,
        `  headline: ${clampText(a?.title, 140)}`,
        `  dek: ${clampText(a?.dek, 180)}`,
        topic ? `  topic: ${topic}` : null,
        eventSeed ? `  futureEventSeed: ${eventSeed}` : null,
        directions ? `  sparkDirections: ${directions}` : null,
        `  confidence: ${conf}`
      ]
        .filter(Boolean)
        .join('\n');
    })
    .join('\n\n');

  const sig = snapshot || {};
  const topicsBySection = sig.topicsBySection || {};
  const topSignals = Array.isArray(sig.topSignals) ? sig.topSignals : [];
  const marketSignals = Array.isArray(sig.marketSignals) ? sig.marketSignals : [];
  const econSignals = Array.isArray(sig.econSignals) ? sig.econSignals : [];

  const topicLines = Object.entries(topicsBySection)
    .slice(0, 10)
    .map(([sec, list]) => {
      const bullets = (Array.isArray(list) ? list : [])
        .slice(0, 5)
        .map((t) => `- ${clampText(t?.label, 120)} :: ${clampText(t?.brief, 160)}`)
        .join('\n');
      return `## ${sec}\n${bullets || '- (none)'}`;
    })
    .join('\n\n');

  const sigLines = (arr, max) =>
    (arr || [])
      .slice(0, max)
      .map((s) => `- ${clampText(s?.title, 160)} (${clampText(s?.source, 40)})`)
      .join('\n');

  return [
    `You are Sonnet 3.7 acting as a high-quality daily "future objects" editor for The Future Times.`,
    `Return JSON only. No markdown fences. No extra text.`,
    ``,
    `Goal: produce a stack-ranked list of ${rangeCount} HIGH-CONFIDENCE physical/digital objects that plausibly exist by the edition date.`,
    `These objects should be directly suggested by today's baseline signals and the +${yearsForward}y newspaper edition themes.`,
    `Do NOT include any image prompt fields. We derive image prompts later.`,
    rangeEnd ? `You are generating ranks ${rangeStart}-${rangeEnd} (inclusive). Set idea.rank accordingly.` : `Ranks should start at ${rangeStart}.`,
    excludeList.length ? `Do NOT repeat any of these titles: ${excludeList.map((x) => `"${x}"`).join(', ')}` : null,
    ``,
    `Edition context:`,
    `- Baseline day: ${day}`,
    `- Years forward: ${yearsForward}`,
    `- Edition date: ${editionDate || '(unknown)'}`,
    `- Target year: ${targetYear}`,
    ``,
    `Hard constraints:`,
    `- Each idea must be a concrete object or artifact that can be photographed or depicted (device, building, vehicle, UI, infrastructure, consumer product, lab instrument, robot, chip module, etc).`,
    `- Avoid abstract trends or policies unless you can turn them into an object (e.g., "EU compliance kiosk" not "EU policy").`,
    `- Confidence must be 0-100; prefer 70+ ideas.`,
    `- Prompts must be editorial photo realism. No text, no logos, no watermarks.`,
    ``,
    `Brevity constraints (required):`,
    `- title: <= 80 chars`,
    `- description: <= 240 chars`,
    `- scene: <= 220 chars`,
    `- sources.storyIds: max 3 ids`,
    `- sources.signals: max 6 strings`,
    ``,
    `Output JSON schema:`,
    `{"schema":1,"day":"${day}","yearsForward":${yearsForward},"targetYear":${targetYear},"generatedAt":"ISO","model":"claude-3-7-sonnet-20250219","ideas":[{"rank":1,"score":92.3,"confidence":85,"title":"...","objectType":"device|building|vehicle|ui|infrastructure|consumer_product|...","description":"...","scene":"...","sources":{"storyIds":["..."],"signals":["..."]}}]}`,
    ``,
    `Baseline topic clusters (today):`,
    topicLines || '(none)',
    ``,
    `Top signals:`,
    sigLines(topSignals, 14) || '(none)',
    ``,
    `Markets:`,
    sigLines(marketSignals, 8) || '(none)',
    ``,
    `Macro:`,
    sigLines(econSignals, 8) || '(none)',
    ``,
    `+${yearsForward}y edition: top story per section (use this to ground objects):`,
    sectionLines || '(none)'
  ].join('\n');
}

export async function refreshIdeas({ day, pipeline, yearsForward = 5, count = 50, force = false } = {}) {
  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled || !flags.ideasEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IMAGES_ENABLED=true and FT_IDEAS_ENABLED=true' };
  }

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return schema;

  const normalized = normalizeDay(day) || formatDay();
  const builtDay = await pipeline.ensureDayBuilt(normalized);
  console.log('[future_images] refreshIdeas', { day: builtDay, yearsForward, count: Number(count) || 0, force: Boolean(force) });
  const edition = pipeline.getEdition(builtDay, yearsForward);
  if (!edition) return { ok: false, error: 'edition_not_found', day: builtDay, yearsForward };

  const snapshot = pipeline.ensureDaySignalSnapshot(builtDay);
  const storyCurations = pipeline.listStoryCurations(builtDay, { yearsForward });

  const system =
    'You are a careful editor. Respond by calling the deliver_future_ideas tool with a single JSON object. Do not include any text.';
  const requestedCount = Math.max(1, Math.min(200, Math.round(Number(count) || 30)));
  const startedAt = Date.now();
  const maxTotalMs = 220000; // Keep well under Vercel maxDuration.

  const candidatesAll = getIdeasModelCandidates();
  const primaryModel = String(candidatesAll[0] || '').trim();
  const fallbackModels = candidatesAll.filter((m) => String(m || '').trim() && String(m || '').trim() !== primaryModel);

  async function callIdeasOnce({ prompt, timeoutMs, maxTokens }) {
    let resp;
    try {
      resp = await callAnthropicJson({
        modelCandidates: primaryModel ? [primaryModel] : candidatesAll,
        system,
        user: prompt,
        maxTokens,
        temperature: 0.4,
        timeoutMs,
        tool: IDEAS_TOOL
      });
    } catch (err) {
      if (String(err?.code || '') === 'anthropic_timeout' && fallbackModels.length) {
        resp = await callAnthropicJson({
          modelCandidates: fallbackModels,
          system,
          user: prompt,
          maxTokens,
          temperature: 0.4,
          timeoutMs: Math.min(75000, Math.max(25000, timeoutMs - 15000)),
          tool: IDEAS_TOOL
        });
      } else {
        throw err;
      }
    }
    return resp;
  }

  const chunkSize = requestedCount <= 8 ? requestedCount : 8;
  const allIdeas = [];
  const seenTitles = new Set();
  let model = '';

  for (let startRank = 1; startRank <= requestedCount; startRank += chunkSize) {
    const endRank = Math.min(requestedCount, startRank + chunkSize - 1);
    const chunkCount = endRank - startRank + 1;
    const elapsed = Date.now() - startedAt;
    const remaining = maxTotalMs - elapsed;
    if (remaining < 25000) {
      return { ok: false, error: 'ideas_generate_failed', code: 'time_budget_exhausted', detail: `Remaining budget ${remaining}ms`, attemptedCounts: [requestedCount] };
    }
    const timeoutMs = Math.min(110000, Math.max(25000, remaining - 5000));
    const maxTokens = 2600;
    const excludeTitles = Array.from(seenTitles).slice(0, 60);
    const prompt = buildIdeasPrompt({
      day: builtDay,
      yearsForward,
      edition,
      snapshot,
      storyCurations,
      count: chunkCount,
      rankStart: startRank,
      rankEnd: endRank,
      excludeTitles
    });

    console.log('[future_images] refreshIdeas chunk', { startRank, endRank, timeoutMs, maxTokens });
    let resp;
    try {
      resp = await callIdeasOnce({ prompt, timeoutMs, maxTokens });
    } catch (err) {
      return {
        ok: false,
        error: 'ideas_generate_failed',
        code: String(err?.code || ''),
        detail: String(err?.message || err || 'Anthropic ideas generation failed'),
        attemptedCounts: [requestedCount]
      };
    }

    model = resp.model || model;
    const parsed = resp.parsed;
    const toolUsed = Boolean(resp.toolUsed);
    const rawIdeas = Array.isArray(parsed?.ideas)
      ? parsed.ideas
      : Array.isArray(parsed)
        ? parsed
        : Array.isArray(parsed?.items)
          ? parsed.items
          : [];
    const chunkIdeas = rawIdeas.slice(0, Math.max(1, Math.min(50, chunkCount))).map((x, i) => normalizeIdea(x, i, startRank));

    if (!chunkIdeas.length) {
      const parsedType = Array.isArray(parsed) ? 'array' : typeof parsed;
      const keys = parsed && typeof parsed === 'object' ? Object.keys(parsed).slice(0, 24) : [];
      return { ok: false, error: 'no_ideas_returned', day: builtDay, yearsForward, toolUsed, parsedType, keys, chunk: { startRank, endRank } };
    }

    allIdeas.push(...chunkIdeas);
    for (const it of chunkIdeas) {
      const t = String(it?.title || '').trim();
      if (t) seenTitles.add(t);
    }
  }

  const ideas = allIdeas
    .filter((x) => x && typeof x === 'object')
    .sort((a, b) => (Number(a.rank) || 0) - (Number(b.rank) || 0))
    .slice(0, requestedCount);

  if (!ideas.length) {
    return { ok: false, error: 'no_ideas_returned', day: builtDay, yearsForward, parsedType: 'none', keys: [] };
  }

  // Persist ideas (stable IDs per day+rank so on-demand images stay attached across refreshes).
  const generatedAt = new Date().toISOString();
  const providerModel = String(model || '').trim() || 'claude-3-7-sonnet-20250219';

  // Best-effort: clear out existing ranks for this day.
  await pgQuery(`DELETE FROM ft_future_ideas WHERE day=$1 AND years_forward=$2;`, [builtDay, yearsForward]);

  for (const idea of ideas) {
    const ideaId = `fi-${builtDay}-y${yearsForward}-r${idea.rank}`;
    await pgQuery(
      `
        INSERT INTO ft_future_ideas(
          idea_id, day, years_forward, rank, score, confidence, title, object_type, description, scene,
          prompt_json, sources_json, model, generated_at, status
        )
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'active')
        ON CONFLICT (idea_id)
        DO UPDATE SET
          day=EXCLUDED.day,
          years_forward=EXCLUDED.years_forward,
          rank=EXCLUDED.rank,
          score=EXCLUDED.score,
          confidence=EXCLUDED.confidence,
          title=EXCLUDED.title,
          object_type=EXCLUDED.object_type,
          description=EXCLUDED.description,
          scene=EXCLUDED.scene,
          prompt_json=EXCLUDED.prompt_json,
          sources_json=EXCLUDED.sources_json,
          model=EXCLUDED.model,
          generated_at=EXCLUDED.generated_at,
          status='active';
      `,
      [
        ideaId,
        builtDay,
        yearsForward,
        idea.rank,
        idea.score,
        idea.confidence,
        idea.title,
        idea.objectType,
        idea.description,
        idea.scene,
        stableStringify({ ...idea.prompt, altText: idea.title }),
        stableStringify(idea.sources || {}),
        providerModel,
        generatedAt
      ]
    );
  }

  // Auto-enqueue top N images.
  const topN = flags.autoTopN;
  let enqueued = 0;
  for (const idea of ideas.slice(0, topN)) {
    const ideaId = `fi-${builtDay}-y${yearsForward}-r${idea.rank}`;
    const promptJson = { ...idea.prompt, altText: idea.title };
    const p = force ? { ...promptJson, _regen: new Date().toISOString() } : promptJson;
    const job = await enqueueJob({
      kind: 'idea_image',
      day: builtDay,
      yearsForward,
      ideaId,
      provider: undefined,
      model: undefined,
      promptJson: p,
      priority: 20 + idea.rank
    });
    if (job && job.ok && job.inserted) enqueued++;
  }

  return { ok: true, day: builtDay, yearsForward, ideas: ideas.length, enqueuedTopN: enqueued, model: providerModel };
}
