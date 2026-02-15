import { isoNow } from './utils.js';
import { readOpusRuntimeConfig } from './runtimeConfig.js';

export const DEFAULT_OPUS_SYSTEM_PROMPT =
  'You are Opus 4.6 acting as a high-quality daily trend curator. Return JSON only. If unsure, pick the most plausible editorial framing.';

function clampInt(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.round(n)));
}

function normalizeMode(value) {
  return String(value || '')
    .trim()
    .toLowerCase();
}

export function getOpusCurationConfigFromEnv() {
  const stored = readOpusRuntimeConfig() || {};
  const modeRaw = String(process.env.OPUS_MODE || stored.mode || '').trim();
  const apiKeyEnv = String(process.env.OPUS_API_KEY || '').trim();
  const apiKeyStored = String(stored.apiKey || '').trim();
  const hasKey = Boolean(apiKeyEnv || apiKeyStored);
  const mode = normalizeMode(modeRaw || (hasKey ? 'anthropic' : 'mock'));

  const modelDefault = mode === 'anthropic' ? 'claude-opus-4-6' : 'opus-4.6';
  const model = String(process.env.OPUS_MODEL || stored.model || modelDefault).trim() || modelDefault;

  const keyStoriesPerEdition = clampInt(process.env.OPUS_KEY_STORIES_PER_EDITION || stored.keyStoriesPerEdition, 1, 0, 7);
  const maxTokens = clampInt(process.env.OPUS_MAX_TOKENS || stored.maxTokens, 16000, 2000, 32000);
  const timeoutMs = clampInt(process.env.OPUS_TIMEOUT_MS || stored.timeoutMs, 300000, 10000, 600000);
  const apiKey = apiKeyEnv || apiKeyStored;
  const apiUrl = String(process.env.OPUS_API_URL || stored.apiUrl || '').trim();
  const systemPrompt =
    String(process.env.OPUS_SYSTEM_PROMPT || stored.systemPrompt || '').trim() || DEFAULT_OPUS_SYSTEM_PROMPT;

  return {
    mode, // 'mock' | 'anthropic' | 'openai' | 'off'
    model,
    keyStoriesPerEdition,
    maxTokens,
    timeoutMs,
    apiKey,
    apiUrl,
    systemPrompt
  };
}

function safeParseJson(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    // Try to recover from "json surrounded by text" responses.
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

export function buildEditionCurationPrompt({ day, yearsForward, editionDate, candidates, keyCount, snapshot }) {
  const sections = snapshot?.topicsBySection || {};
  const topSignals = Array.isArray(snapshot?.topSignals) ? snapshot.topSignals : [];
  const marketSignals = Array.isArray(snapshot?.marketSignals) ? snapshot.marketSignals : [];
  const econSignals = Array.isArray(snapshot?.econSignals) ? snapshot.econSignals : [];

  const topicsBySection = Object.entries(sections)
    .map(([section, topics]) => {
      const list = Array.isArray(topics) ? topics : [];
      const bullets = list
        .slice(0, 6)
        .map((t) => `- ${String(t.label || '').slice(0, 140)} :: ${String(t.brief || '').replace(/\s+/g, ' ').slice(0, 180)}`)
        .join('\n');
      return `## ${section}\n${bullets || '- (no topics)'}\n`;
    })
    .join('\n');

  const signalLines = (arr, max) =>
    (arr || [])
      .slice(0, max)
      .map((s) => `- ${String(s.title || '').slice(0, 160)} (${String(s.source || '').slice(0, 40)})`)
      .join('\n');

  // Keep candidate info concise to reduce prompt tokens
  const candidateLines = (candidates || [])
    .map((c) => {
      const topic = c.topic || {};
      const pack = c.evidencePack || {};
      const citations = Array.isArray(pack.citations) ? pack.citations.slice(0, 2) : [];
      const citeLines = citations
        .map((x) => {
          const title = String(x?.title || '').replace(/\s+/g, ' ').trim().slice(0, 100);
          const source = String(x?.source || '').replace(/\s+/g, ' ').trim().slice(0, 30);
          return `    - ${title} (${source})`;
        })
        .join('\n');
      return [
        `- storyId: ${c.storyId}`,
        `  section: ${c.section} rank: ${c.rank}`,
        `  topic: ${String(topic.label || c.topicLabel || '').slice(0, 130)}`,
        `  brief: ${String(topic.brief || '').replace(/\s+/g, ' ').slice(0, 180)}`,
        citeLines ? `  citations:\n${citeLines}` : null
      ].filter(Boolean).join('\n');
    })
    .join('\n\n');

  return [
    `You are an expert editorial planner for "The Future Times".`,
    `You are curating the edition published on ${editionDate} (yearsForward=${yearsForward}) based on baseline signals from ${day}.`,
    ``,
    `Task: produce a curation plan for the *existing* story candidates list below (do not invent new storyIds).`,
    `- Pick exactly ${keyCount} key stories (the most click-worthy).`,
    `- For every story: propose a sharper headline + dek that describes an ORIGINAL future event, plus concise "sparkDirections" a fast model can use to write the article.`,
    `- For key stories ONLY: also write a draftArticle with body (~4 paragraphs, NYT-style).`,
    ``,
    `CRITICAL:`,
    `- Output STRICT JSON only. No markdown code fences, no commentary before/after the JSON.`,
    `- Write as if this edition is published on ${editionDate}; do not mention forecasts, simulations, projections, or that you are an AI.`,
    `- The curatedTitle/curatedDek must describe an ORIGINAL future event/outcome in the target year. Do NOT restate or lightly rewrite the baseline topic.`,
    `- Prediction markets are inputs: infer the most likely outcome and report that outcome as what happened (do not pose the story as a question).`,
    `- Set exactly ONE hero story by setting hero:true for a single storyId (usually one of the keyStoryIds).`,
    `- Do not output question headlines.`,
    `- topicTitle should be a short stable tag (2-6 words) that captures the underlying topic; it is used as a seed for downstream rendering.`,
    `- futureEventSeed must be a single declarative sentence describing what happened (usable as the lede).`,
    `- Keep sparkDirections short but concrete (who/what happened/what changed/what to cite).`,
    `- For non-key (secondary) stories: it is OK to keep curatedDek very short and avoid prewriting; focus on topicTitle + sparkDirections so Codex Spark can draft on click.`,
    `- For draftArticle.body: narrative paragraphs only (NYT-style), no section headings, and end with a short "Sources" list (4-8 links).`,
    ``,
    `JSON schema:`,
    `{"schema":1,"day":"${day}","yearsForward":${yearsForward},"editionDate":"${editionDate}","keyStoryIds":["id"],"stories":[{"storyId":"id","curatedTitle":"string","curatedDek":"string","sparkDirections":"string","key":false,"hero":false,"futureEventSeed":"string","draftArticle":null}]}`,
    `For key stories, set draftArticle to {"title":"string","dek":"string","body":"string"} with 4+ paragraphs. For non-key stories, set draftArticle to null.`,
    ``,
    ``,
    `Story candidates (must use these storyIds exactly):`,
    candidateLines || '- (none)'
  ].join('\n');
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

function resolveAnthropicModelAlias(model) {
  const raw = String(model || '').trim();
  const lower = raw.toLowerCase();
  if (!raw) return 'claude-opus-4-6';

  // If the user typed the exact API model name, pass it through unchanged.
  if (lower.startsWith('claude-opus-') || lower.startsWith('claude-sonnet-') || lower.startsWith('claude-haiku-')) {
    return raw;
  }

  // User-facing shortcuts → current-gen API model names.
  if (lower === 'opus-4.6' || lower === 'opus' || lower === 'opus-4' || lower.startsWith('opus-')) {
    return 'claude-opus-4-6';
  }
  if (lower === 'sonnet' || lower.startsWith('sonnet')) {
    return 'claude-sonnet-4-5-20250929';
  }
  if (lower === 'haiku' || lower.startsWith('haiku')) {
    return 'claude-haiku-4-5-20251001';
  }
  return raw;
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
  const url = config.apiUrl || 'https://api.anthropic.com/v1/messages';
  if (!config.apiKey) throw new Error('OPUS_API_KEY is required for OPUS_MODE=anthropic');

  const requested = String(config.model || '').trim();
  const resolved = resolveAnthropicModelAlias(requested);
  const modelsToTry = uniqueStrings([
    resolved,
    requested,
    'claude-opus-4-6',
    'claude-sonnet-4-5-20250929',
    'claude-haiku-4-5-20251001'
  ]);

  let lastErr = null;
  for (const model of modelsToTry) {
    const body = {
      model,
      max_tokens: config.maxTokens,
      temperature: 0.4,
      system: String(config.systemPrompt || DEFAULT_OPUS_SYSTEM_PROMPT),
      messages: [{ role: 'user', content: prompt }]
    };

    try {
      const resp = await fetchJsonWithTimeout(
        url,
        {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-api-key': config.apiKey,
            'anthropic-version': '2023-06-01'
          },
          body: JSON.stringify(body)
        },
        config.timeoutMs
      );

      // Anthropic returns { content: [{type:'text', text:'...'}], ... }.
      // Claude may return multiple content blocks; concatenate all text blocks.
      let text = '';
      if (Array.isArray(resp?.content)) {
        for (const block of resp.content) {
          if (block && block.type === 'text' && block.text) {
            text += String(block.text);
          }
        }
      }
      if (!text) text = '';

      // Strip markdown code fences if present
      const stripped = text
        .replace(/^[\s\S]*?```(?:json)?\s*\n?/i, '')
        .replace(/\n?\s*```[\s\S]*$/i, '')
        .trim();

      // Check if response was truncated (stop_reason !== 'end_turn')
      const stopReason = resp?.stop_reason || '';
      const wasTruncated = stopReason === 'max_tokens';

      let parsed = safeParseJson(stripped) || safeParseJson(text);

      // If truncated, try to close unclosed JSON structures
      if (!parsed && wasTruncated) {
        let attempt = stripped || text;
        // Try adding closing brackets/braces
        for (const suffix of [']}', '"}]}', '"}]]}', '"}}]}']) {
          const fixed = attempt + suffix;
          parsed = safeParseJson(fixed);
          if (parsed) break;
        }
      }

      if (!parsed) parsed = safeParseJson(resp);

      if (!parsed) {
        const preview = text.slice(0, 400).replace(/\n/g, '\\n');
        throw new Error(`Anthropic response parse failed (truncated=${wasTruncated}). Preview: ${preview}`);
      }
      // Stash the actual provider model used for downstream visibility.
      if (parsed && typeof parsed === 'object' && !parsed.model) {
        parsed.model = model;
      }
      return parsed;
    } catch (err) {
      lastErr = err;
      const status = err && typeof err === 'object' ? err.status : null;
      const bodyErr = err && typeof err === 'object' ? err.body : null;
      const modelNotFound =
        status === 404 &&
        bodyErr &&
        bodyErr.error &&
        (bodyErr.error.type === 'not_found_error' || bodyErr.error.type === 'not_found') &&
        String(bodyErr.error.message || '').includes('model:');

      if (modelNotFound) {
        // Try the next fallback model.
        continue;
      }
      // Any other error: fail fast (auth, rate limit, etc.)
      throw err;
    }
  }

  throw lastErr || new Error('Anthropic model resolution failed');
}

async function callOpenAiJson(prompt, config) {
  const url = config.apiUrl || 'https://api.openai.com/v1/responses';
  if (!config.apiKey) throw new Error('OPUS_API_KEY is required for OPUS_MODE=openai');

  const body = {
    model: config.model,
    temperature: 0.4,
    max_output_tokens: config.maxTokens,
    input: [
      {
        role: 'system',
        content: [
          {
            type: 'text',
            text: String(config.systemPrompt || DEFAULT_OPUS_SYSTEM_PROMPT)
          }
        ]
      },
      { role: 'user', content: [{ type: 'text', text: prompt }] }
    ]
  };

  const resp = await fetchJsonWithTimeout(
    url,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${config.apiKey}`
      },
      body: JSON.stringify(body)
    },
    config.timeoutMs
  );

  const text = String(resp?.output_text || '');
  const parsed = safeParseJson(text) || safeParseJson(resp);
  if (!parsed) throw new Error('OpenAI response parse failed');
  return parsed;
}

function mockCuration({ day, yearsForward, editionDate, candidates, keyCount }) {
  const list = Array.isArray(candidates) ? candidates : [];
  const hero = list.find((c) => c.rank === 1 && c.section === 'U.S.') || list[0] || null;
  const keyStoryIds = hero && keyCount > 0 ? [hero.storyId] : [];

  const baselineYear = String(day || '').slice(0, 4) || '2026';
  const targetYear = Number(baselineYear) + (Number(yearsForward) || 0);

  const stories = list.map((c) => {
    const key = keyStoryIds.includes(c.storyId);
    const topicLabel = String((c.topic && c.topic.label) || c.topicLabel || c.title || '').replace(/\s+/g, ' ').trim();
    const shortTopic = topicLabel.length > 80 ? topicLabel.slice(0, 80).replace(/\s+\S*$/, '').trim() : topicLabel;
    const futureEventSeed = `By ${editionDate}, the story that began with "${shortTopic}" in ${baselineYear} has reached a decisive turning point.`;
    const outline = [
      `Lead: what happened by ${targetYear}, anchored in ${shortTopic}`,
      `How it traces back to ${baselineYear} baseline signals`,
      'Who won/lost; operational details and constraints',
      'What comes next'
    ];
    const extrapolationTrace = [
      `Baseline (${baselineYear}): ${topicLabel.slice(0, 140)}`,
      `Bridge: institutions adapt + incentives shift over ${Math.max(1, Number(yearsForward) || 0)} years`,
      `Outcome: a specific, reportable event by ${editionDate}`
    ];
    const rationale = [
      'High signal density in baseline evidence',
      'Clear path to a concrete future outcome',
      'Likely to attract reader attention in the section'
    ];
    const sparkDirections = [
      `Write as if published on ${editionDate}.`,
      `The topic is: ${shortTopic}.`,
      `Treat baseline citations as the historical record from ${day}.`,
      `Invent a specific future outcome that resolves the uncertainty around this topic (no hedging).`
    ].join(' ');
    // Build a meaningful mock headline from the topic label
    const mockTitle = `${shortTopic}, ${targetYear}: What Changed`;
    const mockDek = `The signals from ${baselineYear} around ${shortTopic} have matured into policy, markets, and daily life by ${editionDate}.`;
    return {
      storyId: c.storyId,
      curatedTitle: mockTitle,
      curatedDek: mockDek,
      topicTitle: topicLabel,
      sparkDirections,
      key,
      hero: hero ? c.storyId === hero.storyId : false,
      futureEventSeed,
      outline,
      extrapolationTrace,
      rationale,
      draftArticle: null
    };
  });

  return {
    schema: 1,
    day,
    yearsForward,
    editionDate,
    generatedAt: isoNow(),
    model: 'mock-curator',
    editionThesis: `By ${editionDate}, the baseline themes from ${day} have hardened into day-to-day operations and policy.`,
    thinkingTrace: [
      'Prioritized stories with the clearest baseline-to-outcome bridge',
      'Chose a hero story that is broad, narrative, and easy to visualize',
      'Kept secondary stories as directives to minimize prewriting volume'
    ],
    keyStoryIds,
    stories
  };
}

export async function generateEditionCurationPlan(input) {
  const config = input?.config || getOpusCurationConfigFromEnv();
  const mode = normalizeMode(config.mode);
  if (mode === 'off' || mode === 'disabled') {
    return {
      schema: 1,
      day: input?.day || '',
      yearsForward: input?.yearsForward ?? 0,
      editionDate: input?.editionDate || '',
      generatedAt: isoNow(),
      model: 'off',
      keyStoryIds: [],
      stories: []
    };
  }

  const keyCount = clampInt(input?.keyCount ?? config.keyStoriesPerEdition, 1, 0, 7);
  if (mode === 'mock') {
    return mockCuration({ ...input, keyCount });
  }

  const prompt = String(input?.prompt || '').trim() || buildEditionCurationPrompt({ ...input, keyCount });
  if (mode === 'anthropic') {
    const parsed = await callAnthropicJson(prompt, config);
    return parsed || mockCuration({ ...input, keyCount });
  }
  if (mode === 'openai') {
    const parsed = await callOpenAiJson(prompt, config);
    return parsed || mockCuration({ ...input, keyCount });
  }

  // Unknown mode: fall back safely.
  return mockCuration({ ...input, keyCount });
}
