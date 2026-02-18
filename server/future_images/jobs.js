import { createHash, randomUUID } from 'node:crypto';

import { formatDay, normalizeDay, SECTION_ORDER } from '../pipeline/utils.js';

import { getFutureImagesFlags, getGeminiConfig, getNanoBananaConfig, hasBlobConfig } from './config.js';
import { stableStringify } from './json.js';
import { ensureFutureImagesSchema, pgQuery } from './postgres.js';
import { putImageBlob } from './blob.js';
import { extractImagePromptFromArticle, parseSize } from './prompts.js';
import { generateWithDalle, generateWithGemini, generateWithNanoBanana } from './providers.js';

function isoNow() {
  return new Date().toISOString();
}

function sanitizePathSegment(value, maxLen = 140) {
  const raw = String(value || '').trim();
  if (!raw) return 'x';
  const cleaned = raw.replace(/[^a-zA-Z0-9._-]+/g, '_').replace(/^_+|_+$/g, '');
  const out = cleaned || 'x';
  return out.length <= maxLen ? out : out.slice(0, maxLen);
}

function sha256Hex(text) {
  return createHash('sha256').update(String(text || ''), 'utf8').digest('hex');
}

function resolveDefaultProvider() {
  const nano = getNanoBananaConfig();
  if (nano.apiUrl && nano.apiKey) return { provider: 'nano_banana', model: nano.model || 'nano-banana-3-pro' };
  const gemini = getGeminiConfig();
  // "Nano Banana" runs on Gemini image models as the default cloud path.
  if (gemini.apiKey) return { provider: 'nano_banana', model: gemini.model || 'gemini-3-pro-image-preview' };
  const openai = String(process.env.OPENAI_API_KEY || '').trim();
  if (openai) return { provider: 'dalle', model: 'dall-e-3' };
  return { provider: 'nano_banana', model: gemini.model || 'gemini-3-pro-image-preview' };
}

function getWorkerAllowedKinds(flags) {
  const kinds = [];
  if (flags.storyHeroEnabled) kinds.push('story_section_hero');
  if (flags.ideasEnabled) kinds.push('idea_image');
  return kinds;
}

export async function enqueueJob(params) {
  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return schema;

  const kind = String(params?.kind || '').trim();
  const day = normalizeDay(params?.day) || formatDay();
  const yearsForward = Number(params?.yearsForward ?? params?.years_forward ?? 5) || 5;
  const storyId = params?.storyId != null ? String(params.storyId || '').trim() : '';
  const section = params?.section != null ? String(params.section || '').trim() : '';
  const ideaId = params?.ideaId != null ? String(params.ideaId || '').trim() : '';

  if (!kind) throw new Error('kind required');
  if (!day) throw new Error('day required');
  if (!Number.isFinite(yearsForward)) throw new Error('yearsForward invalid');

  const flags = getFutureImagesFlags();
  if (kind === 'story_section_hero' && !flags.storyHeroEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IMAGES_STORY_HERO_ENABLED=true to enqueue story hero jobs.' };
  }
  if (kind === 'idea_image' && !flags.ideasEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IDEAS_ENABLED=true to enqueue idea image jobs.' };
  }

  const promptObj = params?.promptJson && typeof params.promptJson === 'object' ? params.promptJson : {};
  const normalizedPromptJson = stableStringify(promptObj);
  const promptHash = sha256Hex(normalizedPromptJson);

  const provider = String(params?.provider || '').trim() || resolveDefaultProvider().provider;
  const model = String(params?.model || '').trim() || resolveDefaultProvider().model;
  const priority = Number.isFinite(Number(params?.priority)) ? Number(params.priority) : 100;

  const jobId = randomUUID();
  const createdAt = isoNow();

  const insertSql = `
    INSERT INTO ft_image_jobs(
      job_id, kind, day, years_forward, story_id, section, idea_id,
      provider, model, prompt_hash, prompt_json,
      status, attempts, priority, created_at
    )
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'queued',0,$12,$13)
    ON CONFLICT ON CONSTRAINT idx_ft_image_jobs_unique DO NOTHING
    RETURNING job_id;
  `;

  // Note: unique constraint is created as a unique index (not a table constraint). Postgres
  // doesn't allow ON CONFLICT ON CONSTRAINT for indexes, so use ON CONFLICT DO NOTHING instead.
  // We'll rely on the unique index to reject duplicates.
  const insertSqlFixed = `
    INSERT INTO ft_image_jobs(
      job_id, kind, day, years_forward, story_id, section, idea_id,
      provider, model, prompt_hash, prompt_json,
      status, attempts, priority, created_at
    )
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'queued',0,$12,$13)
    ON CONFLICT DO NOTHING
    RETURNING job_id;
  `;

  try {
    const inserted = await pgQuery(insertSqlFixed, [
      jobId,
      kind,
      day,
      yearsForward,
      storyId || null,
      section || null,
      ideaId || null,
      provider,
      model,
      promptHash,
      normalizedPromptJson,
      priority,
      createdAt
    ]);

    const wasInserted = Array.isArray(inserted?.rows) && inserted.rows.length > 0;
    return { ok: true, inserted: wasInserted, jobId: wasInserted ? inserted.rows[0].job_id : null, promptHash };
  } catch (err) {
    return { ok: false, error: 'postgres_error', detail: String(err?.message || err) };
  }
}

function resolveStoryArticleText(pipeline, storyId, story) {
  const curationAt = story?.curation?.generatedAt || '';
  const rendered = pipeline.getRenderedVariant(storyId, { curationGeneratedAt: curationAt });
  const renderedBody = String(rendered?.body || '').trim();
  if (renderedBody && renderedBody.length > 400) return renderedBody;
  const draftBody = String(story?.curation?.draftArticle?.body || story?.curation?.draftBody || '').trim();
  if (draftBody && draftBody.length > 400) return draftBody;
  return draftBody || renderedBody || '';
}

export async function enqueueSectionHeroJobs({ day, pipeline, yearsForward = 5, force = false, includeGlobalHero = true } = {}) {
  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled || !flags.storyHeroEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IMAGES_ENABLED=true and FT_IMAGES_STORY_HERO_ENABLED=true' };
  }

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return schema;

  const requestedDay = normalizeDay(day) || formatDay();
  const builtDay = await pipeline.ensureDayBuilt(requestedDay);
  const payload = pipeline.getEdition(builtDay, yearsForward);
  if (!payload) return { ok: false, error: 'edition_not_found', day: builtDay, yearsForward };

  const editionDate = String(payload.date || '').trim();
  const baselineYear = builtDay.slice(0, 4) || '2026';
  const targetYear = String(Number(baselineYear) + Number(yearsForward));

  const articles = Array.isArray(payload.articles) ? payload.articles : [];
  const targets = [];
  const seen = new Set();

  for (const section of SECTION_ORDER) {
    const a = articles.find((x) => String(x?.section || '').trim() === section);
    if (!a) continue;
    const id = String(a?.id || '').trim();
    if (!id || seen.has(id)) continue;
    seen.add(id);
    targets.push({ storyId: id, section });
  }

  if (includeGlobalHero) {
    const heroId = String(payload.heroId || payload.heroStoryId || '').trim();
    if (heroId && !seen.has(heroId)) {
      const story = pipeline.getStory(heroId);
      const sec = String(story?.section || '').trim() || String(articles.find((x) => x?.id === heroId)?.section || '').trim();
      if (sec) {
        seen.add(heroId);
        targets.push({ storyId: heroId, section: sec });
      }
    }
  }

  const results = [];
  for (const t of targets) {
    const story = pipeline.getStory(t.storyId);
    if (!story) {
      results.push({ storyId: t.storyId, section: t.section, ok: false, error: 'story_not_found' });
      continue;
    }
    const articleText = resolveStoryArticleText(pipeline, t.storyId, story);
    const title = String(payload.articles?.find((x) => x?.id === t.storyId)?.title || story?.headlineSeed || '').trim();
    const dek = String(payload.articles?.find((x) => x?.id === t.storyId)?.dek || story?.dekSeed || '').trim();

    let promptJson;
    try {
      promptJson = await extractImagePromptFromArticle({
        title,
        dek,
        section: t.section,
        day: builtDay,
        editionDate,
        targetYear,
        articleText
      });
    } catch (err) {
      results.push({ storyId: t.storyId, section: t.section, ok: false, error: String(err?.message || err) });
      continue;
    }

    if (force) {
      // Ensure a new job is created even if prompt is identical.
      promptJson = { ...promptJson, _regen: isoNow() };
    }

    try {
      const enq = await enqueueJob({
        kind: 'story_section_hero',
        day: builtDay,
        yearsForward,
        storyId: t.storyId,
        section: t.section,
        provider: resolveDefaultProvider().provider,
        model: resolveDefaultProvider().model,
        promptJson,
        priority: 10
      });
      results.push({ storyId: t.storyId, section: t.section, ok: Boolean(enq.ok), enqueued: Boolean(enq.inserted), promptHash: enq.promptHash || null, error: enq.ok ? null : (enq.error || enq.detail || 'enqueue_failed') });
    } catch (err) {
      results.push({ storyId: t.storyId, section: t.section, ok: false, error: String(err?.message || err) });
    }
  }

  return {
    ok: true,
    day: builtDay,
    yearsForward,
    targets: targets.length,
    queued: results.filter((r) => r.ok && r.enqueued).length,
    results
  };
}

async function claimNextJob({ day = null, kinds = null } = {}) {
  const now = isoNow();
  const params = [];
  let filter = `status='queued'`;
  if (day) {
    params.push(day);
    filter += ` AND day=$${params.length}`;
  }
  const allowedKinds = Array.isArray(kinds) ? kinds.map((k) => String(k || '').trim()).filter(Boolean) : [];
  if (allowedKinds.length > 0) {
    params.push(allowedKinds);
    filter += ` AND kind = ANY($${params.length}::text[])`;
  }
  params.push(now);
  const nowIdx = params.length;

  const q = `
    WITH next AS (
      SELECT job_id
      FROM ft_image_jobs
      WHERE ${filter}
      ORDER BY priority ASC, created_at ASC
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE ft_image_jobs j
    SET status='running', attempts = attempts + 1, started_at=$${nowIdx}
    FROM next
    WHERE j.job_id = next.job_id
    RETURNING j.*;
  `;
  const resp = await pgQuery(q, params);
  const row = resp?.rows?.[0] || null;
  return row;
}

async function markJob(jobId, patch) {
  const keys = Object.keys(patch || {});
  if (!keys.length) return;
  const sets = [];
  const vals = [];
  for (const k of keys) {
    vals.push(patch[k]);
    sets.push(`${k}=$${vals.length}`);
  }
  vals.push(jobId);
  const sql = `UPDATE ft_image_jobs SET ${sets.join(', ')} WHERE job_id=$${vals.length};`;
  await pgQuery(sql, vals);
}

async function insertAsset({ job, blobUrl, mimeType, width, height, altText }) {
  const assetId = randomUUID();
  const createdAt = isoNow();
  await pgQuery(
    `
      INSERT INTO ft_image_assets(
        asset_id, job_id, day, years_forward, kind,
        story_id, section, idea_id,
        provider, model, prompt_json,
        blob_url, mime_type, width, height, alt_text, created_at
      )
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17);
    `,
    [
      assetId,
      job.job_id,
      job.day,
      job.years_forward,
      job.kind,
      job.story_id || null,
      job.section || null,
      job.idea_id || null,
      job.provider,
      job.model,
      job.prompt_json,
      blobUrl,
      mimeType,
      width,
      height,
      altText,
      createdAt
    ]
  );
  return { assetId, createdAt };
}

function buildBlobPath(job) {
  const day = sanitizePathSegment(job.day);
  const yearsForward = Number(job.years_forward) || 5;
  const promptHash = sanitizePathSegment(job.prompt_hash);
  if (job.kind === 'story_section_hero') {
    const section = sanitizePathSegment(job.section || 'section');
    const storyId = sanitizePathSegment(job.story_id || 'story');
    return `ft/${day}/y${yearsForward}/story-heroes/${section}/${storyId}/${promptHash}.png`;
  }
  const ideaId = sanitizePathSegment(job.idea_id || 'idea');
  return `ft/${day}/y${yearsForward}/ideas/${ideaId}/${promptHash}.png`;
}

async function generateImageForJob(job) {
  let promptJson = {};
  try {
    promptJson = JSON.parse(job.prompt_json || '{}');
  } catch {
    promptJson = {};
  }

  const providerRequested = String(job?.provider || '').trim().toLowerCase();
  const nano = getNanoBananaConfig();
  const hasNano = Boolean(nano.apiUrl && nano.apiKey);
  const gemini = getGeminiConfig();
  const hasGemini = Boolean(gemini.apiKey);
  const hasOpenAi = Boolean(String(process.env.OPENAI_API_KEY || '').trim());

  // Explicit provider if configured, otherwise fail over in a fixed order.
  // nano_banana can be either a direct Nano Banana endpoint or Gemini image models.
  if (providerRequested === 'nano_banana') {
    if (hasNano) return generateWithNanoBanana(promptJson);
    if (hasGemini) return generateWithGemini(promptJson);
  }
  if (providerRequested === 'gemini' && hasGemini) return generateWithGemini(promptJson);
  if (providerRequested === 'dalle' && hasOpenAi) return generateWithDalle(promptJson);

  if (hasNano) return generateWithNanoBanana(promptJson);
  if (hasGemini) return generateWithGemini(promptJson);
  if (hasOpenAi) return generateWithDalle(promptJson);
  throw new Error('No configured image provider (need Nano Banana, Gemini, or OpenAI)');
}

export async function runImageWorker({ limit = 3, maxMs = 220000, day = null } = {}) {
  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IMAGES_ENABLED=true' };
  }

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return schema;

  if (!hasBlobConfig()) {
    return { ok: false, error: 'blob_not_configured', detail: 'Set BLOB_READ_WRITE_TOKEN to enable image uploads.' };
  }

  const nano = getNanoBananaConfig();
  const gemini = getGeminiConfig();
  const openai = String(process.env.OPENAI_API_KEY || '').trim();
  if (!(nano.apiUrl && nano.apiKey) && !gemini.apiKey && !openai) {
    return {
      ok: false,
      error: 'image_provider_not_configured',
      detail: 'Set GEMINI_API_KEY (Nano Banana via Gemini), or NANOBANANA_API_URL + NANOBANANA_API_KEY, or OPENAI_API_KEY.'
    };
  }

  const allowedKinds = getWorkerAllowedKinds(flags);
  if (!allowedKinds.length) {
    return { ok: false, error: 'disabled', detail: 'No image job kinds are enabled.' };
  }

  // If idea images are disabled, fail stale queued idea jobs so queue state reflects
  // current policy and workers only process section-hero jobs.
  if (!flags.ideasEnabled) {
    const finishedAt = isoNow();
    await pgQuery(
      `
        UPDATE ft_image_jobs
        SET status='failed',
            finished_at=$1,
            last_error=COALESCE(last_error, 'ideas_disabled')
        WHERE status='queued'
          AND kind='idea_image'
          AND ($2::text IS NULL OR day=$2);
      `,
      [finishedAt, day || null]
    );
  }

  const startedAtMs = Date.now();
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  const jobs = [];

  while (processed < limit && Date.now() - startedAtMs < maxMs) {
    const job = await claimNextJob({ day, kinds: allowedKinds });
    if (!job) break;
    processed++;

    try {
      const img = await generateImageForJob(job);
      const blobPath = buildBlobPath(job);
      const uploaded = await putImageBlob(blobPath, img.bytes, { contentType: img.mimeType || 'image/png' });
      const promptParsed = (() => {
        try { return JSON.parse(job.prompt_json || '{}'); } catch { return {}; }
      })();
      const altText = String(promptParsed?.altText || '').trim() || (job.kind === 'idea_image' ? `Future object idea ${job.idea_id || ''}` : `Future story ${job.story_id || ''}`);
      const size = parseSize(promptParsed?.size || '1792x1024');
      await insertAsset({
        job,
        blobUrl: uploaded.url,
        mimeType: img.mimeType || 'image/png',
        width: Number(img.width) || size.width,
        height: Number(img.height) || size.height,
        altText
      });

      await markJob(job.job_id, { status: 'succeeded', finished_at: isoNow(), last_error: null });
      succeeded++;
      jobs.push({ jobId: job.job_id, status: 'succeeded', url: uploaded.url });
    } catch (err) {
      const msg = String(err?.message || err).slice(0, 1200);
      await markJob(job.job_id, { status: 'failed', finished_at: isoNow(), last_error: msg });
      failed++;
      jobs.push({ jobId: job.job_id, status: 'failed', error: msg });
    }
  }

  return { ok: true, processed, succeeded, failed, jobs };
}

export async function enqueueSingleStoryHeroJob({ day, pipeline, storyId, section, yearsForward = 5, force = false, priority = 10 } = {}) {
  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled || !flags.storyHeroEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IMAGES_ENABLED=true and FT_IMAGES_STORY_HERO_ENABLED=true' };
  }
  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return schema;

  const requestedDay = normalizeDay(day) || formatDay();
  const builtDay = await pipeline.ensureDayBuilt(requestedDay);
  const id = String(storyId || '').trim();
  if (!id) return { ok: false, error: 'storyId_required' };
  const story = pipeline.getStory(id);
  if (!story) return { ok: false, error: 'story_not_found', storyId: id };

  const payload = pipeline.getEdition(builtDay, yearsForward);
  const editionDate = String(payload?.date || story?.evidencePack?.editionDate || '').trim();
  const baselineYear = builtDay.slice(0, 4) || '2026';
  const targetYear = String(Number(baselineYear) + Number(yearsForward));

  const fromEdition = payload && Array.isArray(payload.articles)
    ? payload.articles.find((a) => String(a?.id || '').trim() === id)
    : null;
  const title = String(fromEdition?.title || story?.headlineSeed || '').trim();
  const dek = String(fromEdition?.dek || story?.dekSeed || '').trim();
  const sec = String(section || fromEdition?.section || story?.section || '').trim();

  const articleText = resolveStoryArticleText(pipeline, id, story);
  const promptBase = await extractImagePromptFromArticle({
    title,
    dek,
    section: sec,
    day: builtDay,
    editionDate,
    targetYear,
    articleText
  });
  const promptJson = force ? { ...promptBase, _regen: isoNow() } : promptBase;

  const enq = await enqueueJob({
    kind: 'story_section_hero',
    day: builtDay,
    yearsForward,
    storyId: id,
    section: sec,
    provider: resolveDefaultProvider().provider,
    model: resolveDefaultProvider().model,
    promptJson,
    priority: Number.isFinite(Number(priority)) ? Number(priority) : 10
  });
  if (!enq || enq.ok !== true) {
    return { ok: false, error: enq?.error || 'enqueue_failed', detail: enq?.detail || null };
  }
  return { ok: true, day: builtDay, storyId: id, section: sec, inserted: enq.inserted, promptHash: enq.promptHash };
}

export async function enqueueSingleIdeaJob({ ideaId, force = false } = {}) {
  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IMAGES_ENABLED=true' };
  }
  if (!flags.ideasEnabled) {
    return { ok: false, error: 'disabled', detail: 'Set FT_IDEAS_ENABLED=true to enqueue idea images.' };
  }
  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return schema;
  const id = String(ideaId || '').trim();
  if (!id) return { ok: false, error: 'ideaId_required' };
  const row = await pgQuery(
    `SELECT idea_id, day, years_forward, prompt_json FROM ft_future_ideas WHERE idea_id=$1 LIMIT 1;`,
    [id]
  );
  const found = row?.rows?.[0] || null;
  if (!found) return { ok: false, error: 'idea_not_found', ideaId: id };

  let promptObj = {};
  try { promptObj = JSON.parse(found.prompt_json || '{}'); } catch { promptObj = {}; }
  const promptJson = force ? { ...promptObj, _regen: isoNow() } : promptObj;

  const enq = await enqueueJob({
    kind: 'idea_image',
    day: found.day,
    yearsForward: Number(found.years_forward) || 5,
    ideaId: found.idea_id,
    promptJson,
    priority: 40
  });
  if (!enq || enq.ok !== true) {
    return { ok: false, error: enq?.error || 'enqueue_failed', detail: enq?.detail || null };
  }
  return { ok: true, day: found.day, ideaId: found.idea_id, inserted: enq.inserted, promptHash: enq.promptHash };
}
