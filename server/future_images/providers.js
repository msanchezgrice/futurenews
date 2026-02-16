import { getGeminiConfig, getNanoBananaConfig } from './config.js';

function parseSize(size) {
  const raw = String(size || '').trim().toLowerCase();
  const m = raw.match(/^(\d{2,4})x(\d{2,4})$/);
  if (!m) return { width: 1792, height: 1024 };
  const width = Math.max(256, Math.min(4096, Number(m[1])));
  const height = Math.max(256, Math.min(4096, Number(m[2])));
  return { width, height };
}

function uniqueStrings(list) {
  const out = [];
  const seen = new Set();
  for (const item of list || []) {
    const s = String(item || '').trim();
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

function clampText(value, max = 1200) {
  const str = String(value || '').replace(/\s+/g, ' ').trim();
  if (!str) return '';
  if (str.length <= max) return str;
  return str.slice(0, max).replace(/[,\s;:.]+$/g, '').trim();
}

function nearestAspectRatio(width, height) {
  const target = Math.max(0.01, Number(width) / Math.max(1, Number(height)));
  const options = [
    { name: '1:1', value: 1.0 },
    { name: '2:3', value: 2 / 3 },
    { name: '3:2', value: 3 / 2 },
    { name: '3:4', value: 3 / 4 },
    { name: '4:3', value: 4 / 3 },
    { name: '4:5', value: 4 / 5 },
    { name: '5:4', value: 5 / 4 },
    { name: '9:16', value: 9 / 16 },
    { name: '16:9', value: 16 / 9 },
    { name: '21:9', value: 21 / 9 }
  ];
  let best = options[0];
  let bestDelta = Number.POSITIVE_INFINITY;
  for (const opt of options) {
    const delta = Math.abs(Math.log(target) - Math.log(opt.value));
    if (delta < bestDelta) {
      best = opt;
      bestDelta = delta;
    }
  }
  return best.name;
}

function buildGeminiVisualPrompt(promptJson, aspectRatio) {
  const visualPrompt = clampText(promptJson?.visualPrompt || '', 1500);
  if (!visualPrompt) throw new Error('Missing visualPrompt');
  const negativePrompt = clampText(promptJson?.negativePrompt || '', 1200);
  const style = clampText(promptJson?.style || 'editorial_photo', 80);
  const composition = clampText(promptJson?.composition || '', 220);
  const objects = Array.isArray(promptJson?.objects)
    ? promptJson.objects.map((x) => clampText(x, 80)).filter(Boolean).slice(0, 12)
    : [];

  const parts = [
    `Generate a high-fidelity editorial photo image.`,
    `Primary scene: ${visualPrompt}`,
    `Style: ${style}. Photojournalistic realism, natural lighting, high detail.`,
    `Target aspect ratio: ${aspectRatio}.`
  ];
  if (composition) parts.push(`Composition guidance: ${composition}`);
  if (objects.length) parts.push(`Required objects: ${objects.join(', ')}`);
  if (negativePrompt) parts.push(`Avoid: ${negativePrompt}`);
  return parts.join('\n');
}

function extractGeminiImageBuffer(payload) {
  const candidates = Array.isArray(payload?.candidates) ? payload.candidates : [];
  for (const candidate of candidates) {
    const parts = Array.isArray(candidate?.content?.parts) ? candidate.content.parts : [];
    for (const part of parts) {
      const inline = part?.inlineData || part?.inline_data || null;
      const b64 = String(inline?.data || '').trim();
      if (!b64) continue;
      const bytes = decodeBase64ToBuffer(b64);
      if (!bytes || bytes.length < 1000) continue;
      const mimeType = String(inline?.mimeType || inline?.mime_type || '').trim() || 'image/png';
      return { bytes, mimeType };
    }
  }
  return null;
}

function decodeBase64ToBuffer(b64) {
  const raw = String(b64 || '').trim();
  if (!raw) return null;
  const cleaned = raw.includes(',') && raw.startsWith('data:') ? raw.slice(raw.indexOf(',') + 1) : raw;
  try {
    return Buffer.from(cleaned, 'base64');
  } catch {
    return null;
  }
}

async function fetchBytes(url, timeoutMs = 45000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { signal: controller.signal });
    if (!resp.ok) throw new Error(`Fetch ${resp.status} for image url`);
    const arr = await resp.arrayBuffer();
    const contentType = resp.headers.get('content-type') || '';
    return { bytes: Buffer.from(arr), contentType };
  } finally {
    clearTimeout(timeout);
  }
}

export async function generateWithNanoBanana(promptJson) {
  const cfg = getNanoBananaConfig();
  if (!cfg.apiUrl || !cfg.apiKey) {
    const err = new Error('NANOBANANA_API_URL / NANOBANANA_API_KEY not configured');
    err.code = 'nanobanana_not_configured';
    throw err;
  }

  const sizeStr = String(promptJson?.size || '1792x1024');
  const { width, height } = parseSize(sizeStr);
  const model = String(cfg.model || 'nano-banana-3-pro');
  const visualPrompt = String(promptJson?.visualPrompt || '').trim();
  if (!visualPrompt) throw new Error('Missing visualPrompt');
  const negativePrompt = String(promptJson?.negativePrompt || '').trim();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120000);
  try {
    const resp = await fetch(cfg.apiUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${cfg.apiKey}`
      },
      body: JSON.stringify({
        model,
        prompt: visualPrompt,
        negative_prompt: negativePrompt || undefined,
        width,
        height
      }),
      signal: controller.signal
    });

    const text = await resp.text().catch(() => '');
    let parsed = null;
    try { parsed = text ? JSON.parse(text) : null; } catch { parsed = null; }

    if (!resp.ok) {
      throw new Error(`Nano Banana API ${resp.status}: ${String(text || '').slice(0, 240)}`);
    }

    // Attempt to handle a few common response shapes.
    const candidates = [];
    const b64 =
      parsed?.data?.[0]?.b64_json ||
      parsed?.data?.[0]?.b64 ||
      parsed?.image_base64 ||
      parsed?.image ||
      parsed?.output_base64 ||
      null;
    if (b64) candidates.push({ type: 'b64', value: b64 });

    const url =
      parsed?.data?.[0]?.url ||
      parsed?.url ||
      (Array.isArray(parsed?.output) ? parsed.output[0] : null) ||
      null;
    if (url) candidates.push({ type: 'url', value: url });

    for (const c of candidates) {
      if (c.type === 'b64') {
        const buf = decodeBase64ToBuffer(c.value);
        if (buf && buf.length > 1000) {
          return { bytes: buf, mimeType: 'image/png', width, height };
        }
      }
      if (c.type === 'url') {
        const fetched = await fetchBytes(String(c.value), 60000);
        if (fetched.bytes && fetched.bytes.length > 1000) {
          return { bytes: fetched.bytes, mimeType: fetched.contentType || 'image/png', width, height };
        }
      }
    }

    throw new Error('Nano Banana returned no usable image payload');
  } finally {
    clearTimeout(timeout);
  }
}

export async function generateWithGemini(promptJson) {
  const cfg = getGeminiConfig();
  if (!cfg.apiKey) {
    const err = new Error('GEMINI_API_KEY not configured');
    err.code = 'gemini_not_configured';
    throw err;
  }

  const sizeStr = String(promptJson?.size || '1792x1024');
  const { width, height } = parseSize(sizeStr);
  const aspectRatio = nearestAspectRatio(width, height);
  const textPrompt = buildGeminiVisualPrompt(promptJson, aspectRatio);
  const models = uniqueStrings([cfg.model, cfg.fallbackModel, 'gemini-2.5-flash-image']);
  const apiBase = String(cfg.apiBaseUrl || 'https://generativelanguage.googleapis.com/v1beta').replace(/\/+$/g, '');

  let lastError = null;
  for (const model of models) {
    const endpoint = `${apiBase}/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(cfg.apiKey)}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 120000);
    try {
      const resp = await fetch(endpoint, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [{ text: textPrompt }] }],
          generationConfig: {
            responseModalities: ['TEXT', 'IMAGE'],
            imageConfig: { aspectRatio }
          }
        }),
        signal: controller.signal
      });

      const text = await resp.text().catch(() => '');
      let parsed = null;
      try { parsed = text ? JSON.parse(text) : null; } catch { parsed = null; }

      if (!resp.ok) {
        const detail = parsed?.error?.message ? String(parsed.error.message) : String(text || '').slice(0, 300);
        throw new Error(`Gemini API ${resp.status} (${model}): ${detail}`);
      }

      const extracted = extractGeminiImageBuffer(parsed);
      if (extracted) {
        return {
          bytes: extracted.bytes,
          mimeType: extracted.mimeType || 'image/png',
          width,
          height,
          modelUsed: model
        };
      }
      throw new Error(`Gemini API returned no image payload (${model})`);
    } catch (err) {
      lastError = err;
      // Try fallback models when current model is unavailable/unsupported.
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastError || new Error('Gemini image generation failed');
}

export async function generateWithDalle(promptJson) {
  const apiKey = String(process.env.OPENAI_API_KEY || '').trim();
  if (!apiKey) {
    const err = new Error('OPENAI_API_KEY not configured');
    err.code = 'openai_not_configured';
    throw err;
  }

  const sizeStr = String(promptJson?.size || '1792x1024');
  const { width, height } = parseSize(sizeStr);
  const visualPrompt = String(promptJson?.visualPrompt || '').trim();
  if (!visualPrompt) throw new Error('Missing visualPrompt');

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 120000);
  try {
    const resp = await fetch('https://api.openai.com/v1/images/generations', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'dall-e-3',
        prompt: `Newspaper editorial photograph style, photojournalistic. ${visualPrompt}. No text overlays. Modern, high quality.`,
        n: 1,
        size: `${width}x${height}`,
        quality: 'standard'
      }),
      signal: controller.signal
    });

    if (!resp.ok) {
      const errText = await resp.text().catch(() => '');
      throw new Error(`OpenAI image API ${resp.status}: ${errText.slice(0, 240)}`);
    }

    const data = await resp.json();
    const imageUrl = data?.data?.[0]?.url;
    if (!imageUrl) throw new Error('OpenAI image API returned no url');
    const fetched = await fetchBytes(imageUrl, 60000);
    return { bytes: fetched.bytes, mimeType: fetched.contentType || 'image/png', width, height };
  } finally {
    clearTimeout(timeout);
  }
}
