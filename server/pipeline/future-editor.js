import { getOpusCurationConfigFromEnv } from './curation.js';

const DEFAULT_FUTURE_EDITOR_SYSTEM_PROMPT =
  'You are Opus 4.6 acting as the final standards editor for The Future Times. Return strict JSON only.';

function trimText(value, maxLen = 1200) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.length <= maxLen ? text : text.slice(0, maxLen).trim();
}

function safeParseJson(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    const start = raw.indexOf('{');
    const end = raw.lastIndexOf('}');
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(raw.slice(start, end + 1));
      } catch {
        return null;
      }
    }
    return null;
  }
}

function normalizeEditorDecision(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'reject' || raw === 'rejected' || raw === 'drop') return 'reject';
  if (raw === 'revise' || raw === 'rewrite' || raw === 'fix') return 'revise';
  return 'approve';
}

function resolveAnthropicModelAlias(model) {
  const raw = String(model || '').trim();
  const lower = raw.toLowerCase();
  if (!raw) return 'claude-3-7-sonnet-20250219';
  if (lower.startsWith('claude-opus-') || lower.startsWith('claude-haiku-')) {
    return raw;
  }
  if (lower === 'opus-4.6' || lower === 'opus' || lower.startsWith('opus-')) return 'claude-3-7-sonnet-20250219';
  if (lower === 'haiku' || lower.startsWith('haiku')) return 'claude-haiku-4-5-20251001';
  return 'claude-3-7-sonnet-20250219';
}

async function fetchJsonWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url, { ...options, signal: controller.signal });
    const text = await resp.text();
    let parsedBody = null;
    try {
      parsedBody = JSON.parse(text);
    } catch {
      parsedBody = null;
    }
    if (!resp.ok) {
      const err = new Error(`LLM HTTP ${resp.status}: ${text.slice(0, 220)}`);
      err.status = resp.status;
      err.body = parsedBody;
      throw err;
    }
    const parsed = parsedBody || safeParseJson(text);
    if (!parsed) {
      throw new Error(`LLM returned non-JSON response (first 220 chars): ${text.slice(0, 220)}`);
    }
    return parsed;
  } finally {
    clearTimeout(timeout);
  }
}

async function callAnthropicJson(prompt, config) {
  const url = String(config?.apiUrl || '').trim() || 'https://api.anthropic.com/v1/messages';
  const apiKey = String(config?.apiKey || '').trim();
  if (!apiKey) throw new Error('OPUS_API_KEY is required for future editor review');

  const requested = String(config?.model || '').trim();
  const resolved = resolveAnthropicModelAlias(requested || 'claude-3-7-sonnet-20250219');
  const modelsToTry = [resolved, 'claude-3-7-sonnet-20250219', 'claude-haiku-4-5-20251001'].filter(Boolean);
  const maxTokens = Math.max(2000, Math.min(32000, Number(config?.maxTokens) || 18000));
  const timeoutMs = Math.max(20000, Math.min(300000, Number(config?.timeoutMs) || 180000));
  const systemPrompt = String(config?.systemPrompt || '').trim() || DEFAULT_FUTURE_EDITOR_SYSTEM_PROMPT;

  let lastErr = null;
  for (const model of modelsToTry) {
    try {
      const resp = await fetchJsonWithTimeout(
        url,
        {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-api-key': apiKey,
            'anthropic-version': '2023-06-01'
          },
          body: JSON.stringify({
            model,
            max_tokens: maxTokens,
            temperature: 0.2,
            system: systemPrompt,
            messages: [{ role: 'user', content: String(prompt || '') }]
          })
        },
        timeoutMs
      );

      let text = '';
      if (Array.isArray(resp?.content)) {
        for (const block of resp.content) {
          if (block && block.type === 'text' && block.text) text += String(block.text);
        }
      }
      const stripped = text
        .replace(/^[\s\S]*?```(?:json)?\s*\n?/i, '')
        .replace(/\n?\s*```[\s\S]*$/i, '')
        .trim();
      const parsed = safeParseJson(stripped) || safeParseJson(text) || safeParseJson(resp);
      if (!parsed) {
        throw new Error(`Future editor parse failed (preview=${text.slice(0, 240).replace(/\n/g, '\\n')})`);
      }
      if (parsed && typeof parsed === 'object' && !parsed.model) parsed.model = model;
      return parsed;
    } catch (err) {
      lastErr = err;
      const status = err && typeof err === 'object' ? err.status : null;
      const bodyErr = err && typeof err === 'object' ? err.body : null;
      const modelNotFound =
        status === 404 &&
        bodyErr &&
        bodyErr.error &&
        (bodyErr.error.type === 'not_found_error' || bodyErr.error.type === 'not_found');
      if (modelNotFound) continue;
      throw err;
    }
  }
  throw lastErr || new Error('Anthropic model resolution failed');
}

export function buildFutureEditorPrompt({ day, yearsForward, editionDate, stories }) {
  const storyLines = (Array.isArray(stories) ? stories : [])
    .slice(0, 48)
    .map((story, idx) => {
      const evidence = Array.isArray(story?.evidence) ? story.evidence : [];
      const evidenceLines = evidence
        .slice(0, 2)
        .map((line) => `    - ${trimText(line, 140)}`)
        .join('\n');

      return [
        `${idx + 1}. storyId: ${String(story?.storyId || '').trim()}`,
        `   section: ${String(story?.section || '').trim() || 'World'} rank: ${Number.isFinite(Number(story?.rank)) ? Number(story.rank) : 9999}`,
        `   topic: ${trimText(story?.topicLabel || '', 120) || '(none)'}`,
        `   title: ${trimText(story?.title || '', 220)}`,
        `   dek: ${trimText(story?.dek || '', 260)}`,
        `   body_excerpt: ${trimText(story?.body || '', 900)}`,
        evidenceLines ? `   evidence:\n${evidenceLines}` : ''
      ].filter(Boolean).join('\n');
    })
    .join('\n\n');

  return [
    `You are the final editorial standards gate for The Future Times.`,
    `Edition date: ${editionDate} (yearsForward=${yearsForward}, baseline day=${day}).`,
    ``,
    `Task: For EVERY story below, decide whether it makes sense as a real newspaper story from ${editionDate}.`,
    `Allowed decisions: approve, revise, reject.`,
    ``,
    `Decision policy:`,
    `- approve: coherent, plausible, and properly future-anchored.`,
    `- revise: salvageable but needs corrected headline/dek/body wording to fit ${editionDate}.`,
    `- reject: does not make sense from a ${editionDate} lens, is timeline-incoherent, contradictory, or fundamentally weak.`,
    ``,
    `Reject examples: impossible age/time math, memorialized "legacy" framing with no concrete future development, nonspecific filler, obvious present-day recaps pretending to be future news.`,
    `For revise: provide corrected title and dek. Body is optional, but include a replacement body excerpt if the current copy is materially wrong.`,
    ``,
    `Output STRICT JSON only (no markdown fences).`,
    `Schema:`,
    `{"schema":1,"day":"${day}","yearsForward":${yearsForward},"editionDate":"${editionDate}","stories":[{"storyId":"id","decision":"approve|revise|reject","reason":"string","title":"string","dek":"string","body":"string"}]}`,
    ``,
    `Stories:`,
    storyLines || '- (none)'
  ].join('\n');
}

export async function reviewEditionWithFutureEditor(input) {
  const config = input?.config || getOpusCurationConfigFromEnv();
  const mode = String(config?.mode || '').trim().toLowerCase();
  const stories = Array.isArray(input?.stories) ? input.stories : [];
  const base = {
    schema: 1,
    day: String(input?.day || '').trim(),
    yearsForward: Number(input?.yearsForward) || 5,
    editionDate: String(input?.editionDate || '').trim()
  };

  if (!stories.length) return { ...base, stories: [], skipped: true, reason: 'no_stories' };
  if (mode && mode !== 'anthropic') return { ...base, stories: [], skipped: true, reason: 'mode_not_supported' };
  if (!String(config?.apiKey || '').trim()) return { ...base, stories: [], skipped: true, reason: 'missing_api_key' };

  const prompt = String(input?.prompt || '').trim() || buildFutureEditorPrompt({
    day: base.day,
    yearsForward: base.yearsForward,
    editionDate: base.editionDate,
    stories
  });

  const parsed = await callAnthropicJson(prompt, {
    apiKey: config.apiKey,
    apiUrl: config.apiUrl,
    model: 'claude-3-7-sonnet-20250219',
    maxTokens: Math.min(Number(config.maxTokens) || 20000, 32000),
    timeoutMs: Math.min(Number(config.timeoutMs) || 180000, 240000),
    systemPrompt: DEFAULT_FUTURE_EDITOR_SYSTEM_PROMPT
  });

  const rawStories = Array.isArray(parsed?.stories)
    ? parsed.stories
    : Array.isArray(parsed?.decisions)
      ? parsed.decisions
      : [];

  const out = [];
  const seen = new Set();
  for (const entry of rawStories) {
    const storyId = String(entry?.storyId || '').trim();
    if (!storyId || seen.has(storyId)) continue;
    seen.add(storyId);
    out.push({
      storyId,
      decision: normalizeEditorDecision(entry?.decision || entry?.status || 'approve'),
      reason: trimText(entry?.reason || entry?.why || '', 320),
      title: trimText(entry?.title || entry?.revisedTitle || '', 220),
      dek: trimText(entry?.dek || entry?.revisedDek || '', 320),
      body: trimText(entry?.body || entry?.revisedBody || '', 2400)
    });
  }

  return {
    ...base,
    model: String(parsed?.model || 'claude-3-7-sonnet-20250219'),
    stories: out
  };
}
