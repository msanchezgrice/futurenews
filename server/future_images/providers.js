import { getNanoBananaConfig } from './config.js';

function parseSize(size) {
  const raw = String(size || '').trim().toLowerCase();
  const m = raw.match(/^(\d{2,4})x(\d{2,4})$/);
  if (!m) return { width: 1792, height: 1024 };
  const width = Math.max(256, Math.min(4096, Number(m[1])));
  const height = Math.max(256, Math.min(4096, Number(m[2])));
  return { width, height };
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

