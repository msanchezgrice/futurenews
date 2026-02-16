import { getFutureImagesFlags } from './config.js';
import { ensureFutureImagesSchema, pgQuery } from './postgres.js';

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
  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled) return payload;
  if (!payload || typeof payload !== 'object') return payload;
  if (Number(yearsForward) !== 5) return payload;

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return payload;

  const articles = Array.isArray(payload.articles) ? payload.articles : [];
  if (!articles.length) return payload;

  const ids = articles.map((a) => String(a?.id || '').trim()).filter(Boolean);
  const assets = await lookupStoryAssets(day, yearsForward, ids);
  if (!assets.size) return payload;

  const patched = articles.map((a) => {
    const id = String(a?.id || '').trim();
    const asset = id ? assets.get(id) : null;
    if (!asset || !asset.url) return a;
    return { ...a, image: asset.url };
  });

  return { ...payload, articles: patched };
}

export async function decorateArticlePayload(article, { day, yearsForward, storyId } = {}) {
  const flags = getFutureImagesFlags();
  if (!flags.imagesEnabled) return article;
  if (!article || typeof article !== 'object') return article;
  if (Number(yearsForward) !== 5) return article;

  const schema = await ensureFutureImagesSchema();
  if (!schema.ok) return article;

  const asset = await lookupSingleStoryAsset(day, yearsForward, storyId);
  if (!asset || !asset.url) return article;

  return { ...article, image: asset.url };
}

