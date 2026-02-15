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
  const maxTokens = clampInt(process.env.OPUS_MAX_TOKENS || stored.maxTokens, 32000, 4000, 64000);
  const timeoutMs = clampInt(process.env.OPUS_TIMEOUT_MS || stored.timeoutMs, 900000, 10000, 1200000);
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

  // Cap candidates to top 20 to keep prompt and output manageable
  const cappedCandidates = (candidates || []).slice(0, 20);
  const candidateLines = cappedCandidates
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

  // Detect AI section candidates with standing topic enrichments
  const aiCandidates = (candidates || []).filter((c) => c.section === 'AI' && c.evidencePack?.standingTopic);
  let aiExtrapolationBlock = '';
  if (aiCandidates.length) {
    const aiLines = aiCandidates.map((c) => {
      const st = c.evidencePack.standingTopic;
      const axes = Array.isArray(st.extrapolationAxes) ? st.extrapolationAxes : [];
      const milestones = Array.isArray(st.milestones) ? st.milestones : [];
      const axesStr = axes.map((a) => `  - ${a.axis}: ${a.description}`).join('\n');
      const msStr = milestones.map((m) => `  - ${m.year}: ${m.event}`).join('\n');
      return [
        `### ${st.label} (${st.category || 'AI'})`,
        st.description ? `${st.description}` : '',
        axes.length ? `Extrapolation axes:\n${axesStr}` : '',
        milestones.length ? `Projected milestones:\n${msStr}` : '',
        `Evidence today: ${c.evidencePack.evidenceCount || 0} signals matched`
      ].filter(Boolean).join('\n');
    }).join('\n\n');

    aiExtrapolationBlock = [
      ``,
      `## AI SECTION — STRUCTURED EXTRAPOLATION CONTEXT`,
      `For AI section stories, use the extrapolation axes and milestones below as scaffolding.`,
      `Your headlines and articles should be SPECIFIC about capabilities, numbers, and outcomes — not vague "AI advances" fluff.`,
      `Example good headline: "Humanoid Robots Now Staff 40% of Amazon Warehouses" (specific, declarative, future-as-fact)`,
      `Example bad headline: "AI Continues to Advance in Robotics" (vague, present-tense-y)`,
      ``,
      aiLines,
      ``
    ].join('\n');
  }

  return [
    `You are an expert editorial planner for "The Future Times".`,
    `You are curating the edition published on ${editionDate} (yearsForward=${yearsForward}) based on baseline signals from ${day}.`,
    ``,
    `Task: produce a curation plan for the *existing* story candidates list below (do not invent new storyIds).`,
    `- Pick exactly ${keyCount} key stories (the most click-worthy). At least one key story should be from the AI section if AI candidates are present.`,
    `- For EVERY story: propose a sharper headline + dek that describes an ORIGINAL future event, plus concise "sparkDirections" (writing directions for the article).`,
    `- For EVERY story: write a full draftArticle with body (~4-6 paragraphs, NYT-style narrative). No story should have draftArticle:null.`,
    `- For EVERY story: assign a "confidence" score (0-100) rating how plausible/likely this prediction is. 90+ = near-certain extrapolation, 70-89 = highly likely, 50-69 = plausible, below 50 = speculative.`,
    ``,
    `CRITICAL — ANALYZE EACH STORY AND EXTRAPOLATE:`,
    `- Output STRICT JSON only. No markdown code fences, no commentary before/after the JSON.`,
    `- You are a journalist in ${editionDate}. Write as if the edition is published on ${editionDate}. Do not mention forecasts, simulations, projections, or that you are an AI.`,
    ``,
    `YOUR #1 JOB: Read each baseline signal carefully. Identify what DOMAIN or TREND it signals. Then write a specific, plausible news story about that domain in ${editionDate}.`,
    ``,
    `HOW TO EXTRAPOLATE FROM A SIGNAL:`,
    `1. READ the actual news story content — look for dates, deadlines, completion targets, funding amounts, election dates, policy timelines, project milestones.`,
    `2. EXTRAPOLATE forward ${yearsForward} years: what plausibly happens by ${editionDate}? A construction project completes. A policy takes effect. A company's trajectory plays out. A technology matures.`,
    `3. WRITE IT AS NEWS: "White House Ballroom Opens After $200M Renovation" not "How Renderings of Trump's Ballroom Reshaped Architecture."`,
    ``,
    `Example of GOOD extrapolation:`,
    `- Baseline: "Renderings show vision for Trump's White House ballroom" (2026) → Story for 2029: "White House Grand Ballroom Opens to Mixed Reviews After Three-Year Renovation"`,
    `- Baseline: "Anthropic raises $30bn at $380bn valuation" (2026) → Story for 2031: "Anthropic Passes $2 Trillion Valuation as Enterprise AI Revenue Crosses $100B"`,
    `- Baseline: "EU leaders agree to Buy European policy" (2026) → Story for 2028: "EU's Buy European Act Reshapes Defense Procurement as Spending Hits Record €400B"`,
    ``,
    `NEVER DO THIS:`,
    `- "How X Reshaped Y" (anniversary framing)`,
    `- "X Years After [headline]" (backward-looking)`,
    `- "The Legacy of Z" (memorial tone)`,
    `- Stories about minor/trivial events that nobody would remember in ${yearsForward} years`,
    `- Generic filler like "The Quiet Shift in [topic]" or "A New Framework for [topic]"`,
    ``,
    `RULES:`,
    `- curatedTitle/curatedDek must describe an ORIGINAL future event/outcome in the target year. Be specific: use numbers, company names, policy names, concrete outcomes.`,
    `- Prediction markets are inputs: infer the most likely outcome and report that outcome as what happened (do not pose the story as a question).`,
    `- Set exactly ONE hero story by setting hero:true for a single storyId (usually one of the keyStoryIds).`,
    `- Do not output question headlines.`,
    `- topicTitle should be a short stable tag (2-6 words) that captures the underlying DOMAIN/TREND (not the specific baseline event).`,
    `- futureEventSeed must be a single declarative sentence describing what happened in ${editionDate} (usable as the lede). It should read like real news from that date.`,
    `- sparkDirections must describe WHAT HAPPENED in ${editionDate} — be specific: who did what, what number changed, what policy passed, what product launched.`,
    `- For AI section stories: use the extrapolation axes to make specific, quantitative predictions. Mention speed/capability numbers, adoption percentages, cost figures where plausible.`,
    `- For draftArticle.body: narrative paragraphs only (NYT-style), no section headings, written as real journalism from ${editionDate}. Do NOT include a Sources section — that is handled separately. Do NOT reference or link to the original baseline news articles.`,
    `- confidence: integer 0-100 rating the plausibility of this prediction.`,
    aiExtrapolationBlock,
    `JSON schema:`,
    `{"schema":1,"day":"${day}","yearsForward":${yearsForward},"editionDate":"${editionDate}","keyStoryIds":["id"],"stories":[{"storyId":"id","curatedTitle":"string","curatedDek":"string","sparkDirections":"string","key":false,"hero":false,"futureEventSeed":"string","confidence":75,"draftArticle":{"title":"string","dek":"string","body":"string"}}]}`,
    `EVERY story must have a draftArticle with title, dek, and body (4-6 paragraphs). No story should have draftArticle:null.`,
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
    // Use extended thinking for capable models to get better curation
    const supportsThinking = model.includes('sonnet') || model.includes('opus');
    const body = {
      model,
      max_tokens: config.maxTokens,
      system: String(config.systemPrompt || DEFAULT_OPUS_SYSTEM_PROMPT),
      messages: [{ role: 'user', content: prompt }]
    };
    if (supportsThinking) {
      // Extended thinking: budget up to 10k tokens for reasoning
      body.thinking = { type: 'enabled', budget_tokens: 10000 };
      // temperature must not be set when thinking is enabled
    } else {
      body.temperature = 0.4;
    }

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

      // Anthropic returns { content: [{type:'thinking', thinking:'...'}, {type:'text', text:'...'}], ... }.
      // Extract text blocks (skip thinking blocks which are reasoning traces).
      let text = '';
      let thinkingTrace = '';
      if (Array.isArray(resp?.content)) {
        for (const block of resp.content) {
          if (block && block.type === 'thinking' && block.thinking) {
            thinkingTrace += String(block.thinking);
          }
          if (block && block.type === 'text' && block.text) {
            text += String(block.text);
          }
        }
      }
      if (!text) text = '';
      // Log thinking trace for debugging (stored separately)
      if (thinkingTrace) {
        console.log(`[curation] Thinking trace (${thinkingTrace.length} chars) for model=${model}`);
      }

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

// No mock curation — all curation must go through LLM

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
    if (!parsed) throw new Error('Anthropic curation returned no parseable JSON — no fallback.');
    return parsed;
  }
  if (mode === 'openai') {
    const parsed = await callOpenAiJson(prompt, config);
    if (!parsed) throw new Error('OpenAI curation returned no parseable JSON — no fallback.');
    return parsed;
  }

  throw new Error(`Unknown OPUS_MODE="${mode}" — no mock fallback available.`);
}
