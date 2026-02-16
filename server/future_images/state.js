import { formatDay, normalizeDay, SECTION_ORDER } from '../pipeline/utils.js';

import { getFutureImagesFlags, hasBlobConfig, hasPostgresConfig } from './config.js';
import { ensureFutureImagesSchema, getPoolInitError, pgQuery } from './postgres.js';

function clampText(value, max = 260) {
  const str = String(value || '').replace(/\s+/g, ' ').trim();
  if (!str) return '';
  if (str.length <= max) return str;
  return str.slice(0, max).replace(/[,\s;:.]+$/g, '').trim();
}

async function getLatestStoryAssets(day, yearsForward) {
  const q = `
    SELECT DISTINCT ON (story_id) story_id, blob_url, alt_text, created_at
    FROM ft_image_assets
    WHERE day=$1 AND years_forward=$2 AND kind='story_section_hero' AND story_id IS NOT NULL
    ORDER BY story_id, created_at DESC;
  `;
  const resp = await pgQuery(q, [day, yearsForward]);
  const map = new Map();
  for (const row of resp.rows || []) {
    map.set(String(row.story_id || ''), { url: row.blob_url, alt: row.alt_text, createdAt: row.created_at });
  }
  return map;
}

async function getLatestIdeaAssets(day, yearsForward) {
  const q = `
    SELECT DISTINCT ON (idea_id) idea_id, blob_url, alt_text, created_at
    FROM ft_image_assets
    WHERE day=$1 AND years_forward=$2 AND kind='idea_image' AND idea_id IS NOT NULL
    ORDER BY idea_id, created_at DESC;
  `;
  const resp = await pgQuery(q, [day, yearsForward]);
  const map = new Map();
  for (const row of resp.rows || []) {
    map.set(String(row.idea_id || ''), { url: row.blob_url, alt: row.alt_text, createdAt: row.created_at });
  }
  return map;
}

async function getLatestJobStatusByStory(day, yearsForward) {
  const q = `
    SELECT DISTINCT ON (story_id) story_id, status, last_error, created_at
    FROM ft_image_jobs
    WHERE day=$1 AND years_forward=$2 AND kind='story_section_hero' AND story_id IS NOT NULL
    ORDER BY story_id, created_at DESC;
  `;
  const resp = await pgQuery(q, [day, yearsForward]);
  const map = new Map();
  for (const row of resp.rows || []) {
    map.set(String(row.story_id || ''), {
      status: row.status,
      error: row.last_error || null,
      createdAt: row.created_at
    });
  }
  return map;
}

async function getLatestJobStatusByIdea(day, yearsForward) {
  const q = `
    SELECT DISTINCT ON (idea_id) idea_id, status, last_error, created_at
    FROM ft_image_jobs
    WHERE day=$1 AND years_forward=$2 AND kind='idea_image' AND idea_id IS NOT NULL
    ORDER BY idea_id, created_at DESC;
  `;
  const resp = await pgQuery(q, [day, yearsForward]);
  const map = new Map();
  for (const row of resp.rows || []) {
    map.set(String(row.idea_id || ''), {
      status: row.status,
      error: row.last_error || null,
      createdAt: row.created_at
    });
  }
  return map;
}

async function getQueueCounts(day) {
  const q = `SELECT status, COUNT(1)::int AS n FROM ft_image_jobs WHERE day=$1 GROUP BY status;`;
  const resp = await pgQuery(q, [day]);
  const counts = {};
  for (const row of resp.rows || []) {
    counts[String(row.status || '')] = Number(row.n) || 0;
  }
  return counts;
}

async function getRecentFailures(day) {
  const q = `
    SELECT job_id, kind, story_id, section, idea_id, provider, model, status, last_error, created_at, finished_at
    FROM ft_image_jobs
    WHERE day=$1 AND status='failed'
    ORDER BY finished_at DESC NULLS LAST, created_at DESC
    LIMIT 20;
  `;
  const resp = await pgQuery(q, [day]);
  return (resp.rows || []).map((r) => ({
    jobId: r.job_id,
    kind: r.kind,
    storyId: r.story_id || null,
    section: r.section || null,
    ideaId: r.idea_id || null,
    provider: r.provider,
    model: r.model,
    error: r.last_error || null,
    createdAt: r.created_at || null,
    finishedAt: r.finished_at || null
  }));
}

export async function getImagesAdminState({ day, pipeline, yearsForward = 5 } = {}) {
  const flags = getFutureImagesFlags();
  const normalized = normalizeDay(day) || pipeline.getLatestDay?.() || formatDay();
  const builtDay = await pipeline.ensureDayBuilt(normalized);
  const edition = pipeline.getEdition(builtDay, yearsForward);
  const articles = Array.isArray(edition?.articles) ? edition.articles : [];

  const sectionHeroes = SECTION_ORDER.map((section) => {
    const a = articles.find((x) => String(x?.section || '').trim() === section) || null;
    return {
      section,
      storyId: a ? String(a.id || '').trim() : '',
      title: a ? clampText(a.title, 140) : '',
      dek: a ? clampText(a.dek, 180) : '',
      image: a ? String(a.image || '') : ''
    };
  });

  const out = {
    ok: true,
    day: builtDay,
    yearsForward,
    config: {
      flags,
      postgresConfigured: hasPostgresConfig(),
      blobConfigured: hasBlobConfig(),
      postgresInitError: getPoolInitError() || null
    },
    edition: edition
      ? {
          date: edition.date || null,
          curationGeneratedAt: edition.curationGeneratedAt || null,
          heroId: edition.heroId || edition.heroStoryId || null
        }
      : null,
    sectionHeroes,
    ideas: [],
    queue: {},
    failures: []
  };

  if (!hasPostgresConfig()) return out;

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) {
    out.config.postgresInitError = schema.detail || schema.error || 'schema_error';
    return out;
  }

  const [storyAssets, storyJobs, ideaAssets, ideaJobs, queue, failures] = await Promise.all([
    getLatestStoryAssets(builtDay, yearsForward),
    getLatestJobStatusByStory(builtDay, yearsForward),
    getLatestIdeaAssets(builtDay, yearsForward),
    getLatestJobStatusByIdea(builtDay, yearsForward),
    getQueueCounts(builtDay),
    getRecentFailures(builtDay)
  ]);

  out.sectionHeroes = sectionHeroes.map((h) => {
    const asset = h.storyId ? storyAssets.get(h.storyId) : null;
    const job = h.storyId ? storyJobs.get(h.storyId) : null;
    return {
      ...h,
      assetUrl: asset?.url || '',
      assetAlt: asset?.alt || '',
      status: asset?.url ? 'ready' : job?.status || 'missing',
      lastError: job?.error || null
    };
  });

  const ideasResp = await pgQuery(
    `SELECT idea_id, rank, score, confidence, title, object_type, description, scene, prompt_json, sources_json, model, generated_at, status
     FROM ft_future_ideas
     WHERE day=$1 AND years_forward=$2
     ORDER BY rank ASC
     LIMIT 80;`,
    [builtDay, yearsForward]
  );
  out.ideas = (ideasResp.rows || []).map((r) => {
    const ideaId = String(r.idea_id || '');
    const asset = ideaAssets.get(ideaId) || null;
    const job = ideaJobs.get(ideaId) || null;
    return {
      ideaId,
      rank: r.rank,
      score: r.score,
      confidence: r.confidence,
      title: r.title,
      objectType: r.object_type,
      description: r.description,
      scene: r.scene,
      status: asset?.url ? 'ready' : job?.status || 'missing',
      assetUrl: asset?.url || '',
      lastError: job?.error || null,
      generatedAt: r.generated_at || null
    };
  });

  out.queue = queue;
  out.failures = failures;
  return out;
}

