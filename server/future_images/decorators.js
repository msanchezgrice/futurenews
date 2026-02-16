import { getFutureImagesFlags } from './config.js';
import { ensureFutureImagesSchema, pgQuery } from './postgres.js';

const BLOCKED_IMAGE_MARKERS = ['humanoids-labor-market.svg'];

function isRenderableArticleImage(value) {
  const raw = String(value || '').trim();
  if (!raw) return false;
  const lower = raw.toLowerCase();
  for (const marker of BLOCKED_IMAGE_MARKERS) {
    if (lower.includes(marker)) return false;
  }
  if (lower.startsWith('http://') || lower.startsWith('https://')) {
    if (lower.includes('.public.blob.vercel-storage.com/')) return true;
    if (lower.includes('/assets/img/generated/')) return true;
    if (lower.includes('/assets/img/library/')) return true;
    return false;
  }
  if (lower.startsWith('assets/img/generated/') || lower.startsWith('/assets/img/generated/')) return true;
  if (lower.startsWith('assets/img/library/') || lower.startsWith('/assets/img/library/')) return true;
  return false;
}

function sanitizeArticleImage(value) {
  const raw = String(value || '').trim();
  return isRenderableArticleImage(raw) ? raw : '';
}

async function lookupStoryAssets(day, yearsForward, storyIds) {
  const ids = Array.isArray(storyIds) ? storyIds.map((x) => String(x || '').trim()).filter(Boolean) : [];
  if (!ids.length) return new Map();

  const q = `
    SELECT DISTINCT ON (story_id) story_id, blob_url, alt_text, created_at
    FROM ft_image_assets
    WHERE day=$1 AND years_forward=$2 AND kind='story_section_hero' AND story_id = ANY($3::text[])
    ORDER BY story_id, created_at DESC;
  `;
  const resp = await pgQuery(q, [day, yearsForward, ids]);
  const map = new Map();
  for (const row of resp.rows || []) {
    map.set(String(row.story_id || ''), { url: row.blob_url, alt: row.alt_text, createdAt: row.created_at });
  }
  return map;
}

async function lookupSingleStoryAsset(day, yearsForward, storyId) {
  const id = String(storyId || '').trim();
  if (!id) return null;
  const q = `
    SELECT blob_url, alt_text, created_at
    FROM ft_image_assets
    WHERE day=$1 AND years_forward=$2 AND kind='story_section_hero' AND story_id=$3
    ORDER BY created_at DESC
    LIMIT 1;
  `;
  const resp = await pgQuery(q, [day, yearsForward, id]);
  const row = resp.rows?.[0] || null;
  return row ? { url: row.blob_url, alt: row.alt_text, createdAt: row.created_at } : null;
}

export async function decorateEditionPayload(payload, { day, yearsForward } = {}) {
  if (!payload || typeof payload !== 'object') return payload;
  const baseArticles = (Array.isArray(payload.articles) ? payload.articles : []).map((a) => ({
    ...a,
    image: sanitizeArticleImage(a?.image)
  }));
  const basePayload = { ...payload, articles: baseArticles };

  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled) return basePayload;
  if (Number(yearsForward) !== 5) return basePayload;

  if (!baseArticles.length) return basePayload;

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return basePayload;

  const ids = baseArticles.map((a) => String(a?.id || '').trim()).filter(Boolean);
  const assets = await lookupStoryAssets(day, yearsForward, ids);
  if (!assets.size) return basePayload;

  const patched = baseArticles.map((a) => {
    const id = String(a?.id || '').trim();
    const asset = id ? assets.get(id) : null;
    if (!asset || !asset.url) return a;
    return { ...a, image: asset.url };
  });

  return { ...basePayload, articles: patched };
}

export async function decorateArticlePayload(article, { day, yearsForward, storyId } = {}) {
  if (!article || typeof article !== 'object') return article;
  const baseArticle = { ...article, image: sanitizeArticleImage(article.image) };

  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled) return baseArticle;
  if (Number(yearsForward) !== 5) return baseArticle;

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return baseArticle;

  const asset = await lookupSingleStoryAsset(day, yearsForward, storyId);
  if (!asset || !asset.url) return baseArticle;

  return { ...baseArticle, image: asset.url };
}
