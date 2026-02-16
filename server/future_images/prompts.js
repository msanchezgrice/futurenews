import { getPromptModelCandidates } from './config.js';
import { callAnthropicJson } from './anthropic.js';

function clampText(value, max = 1200) {
  const str = String(value || '').replace(/\s+/g, ' ').trim();
  if (!str) return '';
  if (str.length <= max) return str;
  return str.slice(0, max).replace(/[,\s;:.]+$/g, '').trim();
}

export function parseSize(size) {
  const raw = String(size || '').trim().toLowerCase();
  const m = raw.match(/^(\d{2,4})x(\d{2,4})$/);
  if (!m) return { width: 1792, height: 1024, size: '1792x1024' };
  const width = Math.max(256, Math.min(4096, Number(m[1])));
  const height = Math.max(256, Math.min(4096, Number(m[2])));
  return { width, height, size: `${width}x${height}` };
}

export async function extractImagePromptFromArticle(input) {
  const title = clampText(input?.title, 180);
  const dek = clampText(input?.dek, 260);
  const section = clampText(input?.section, 40);
  const day = clampText(input?.day, 20);
  const editionDate = clampText(input?.editionDate, 60);
  const targetYear = clampText(input?.targetYear, 10);
  const articleTextRaw = String(input?.articleText || '').trim();
  const articleText = articleTextRaw.length > 5000 ? articleTextRaw.slice(0, 5000) : articleTextRaw;

  const system =
    'You are a meticulous editorial photo art director. Output STRICT JSON only. No markdown fences, no commentary.';
  const user = [
    `Generate a high-fidelity editorial photograph prompt based on this future-dated newspaper story.`,
    `Constraints:`,
    `- Photojournalistic, documentary realism, natural lighting.`,
    `- No text overlays, no captions, no logos, no watermarks.`,
    `- Avoid celebrity likenesses unless explicitly central to the story.`,
    `- Prefer specific objects, materials, places, time-of-day, and action.`,
    ``,
    `Output JSON schema:`,
    `{"schema":1,"visualPrompt":"...","negativePrompt":"...","altText":"...","size":"1792x1024","style":"editorial_photo","objects":["..."],"composition":"..."}`,
    ``,
    `Context:`,
    section ? `Section: ${section}` : null,
    day ? `Baseline day: ${day}` : null,
    editionDate ? `Edition date: ${editionDate}` : null,
    targetYear ? `Target year: ${targetYear}` : null,
    title ? `Headline: ${title}` : null,
    dek ? `Dek: ${dek}` : null,
    ``,
    `Article body (may be truncated):`,
    articleText ? articleText : '(missing article body; infer from headline/dek)'
  ]
    .filter(Boolean)
    .join('\n');

  const { parsed, model } = (await callAnthropicJson({
    modelCandidates: getPromptModelCandidates(),
    system,
    user,
    maxTokens: 900,
    temperature: 0.3,
    timeoutMs: 65000
  })) || { parsed: null, model: null };

  const out = parsed && typeof parsed === 'object' ? parsed : {};
  const visualPrompt = clampText(out.visualPrompt, 1200);
  const negativePrompt = clampText(
    out.negativePrompt ||
      'no text, no logos, no watermarks, no UI overlays, no typographic elements, no charts, no memes, no frames',
    900
  );
  const altText = clampText(out.altText || title || 'Editorial image', 160);
  const size = clampText(out.size || '1792x1024', 20) || '1792x1024';
  const style = clampText(out.style || 'editorial_photo', 40) || 'editorial_photo';
  const objects = Array.isArray(out.objects) ? out.objects.map((x) => clampText(x, 80)).filter(Boolean).slice(0, 12) : [];
  const composition = clampText(out.composition || '', 220);

  return {
    schema: 1,
    providerModel: model || null,
    visualPrompt,
    negativePrompt,
    altText,
    size,
    style,
    objects,
    composition
  };
}

