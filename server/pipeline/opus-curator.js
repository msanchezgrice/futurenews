import { formatEditionDate, isoNow, stableHash, tokenize } from './utils.js';

function safeParseJson(text, fallback = null) {
  const raw = String(text || '').trim();
  if (!raw) return fallback;

  const stripFence = (value) => {
    const trimmed = String(value || '').trim();
    if (trimmed.startsWith('```')) {
      return trimmed.replace(/^```[a-z]*\n?/i, '').replace(/```$/i, '').trim();
    }
    return trimmed;
  };

  const cleaned = stripFence(raw);
  try {
    return JSON.parse(cleaned);
  } catch {
    // Attempt to recover the first JSON object in a blob.
    const start = cleaned.indexOf('{');
    const end = cleaned.lastIndexOf('}');
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(cleaned.slice(start, end + 1));
      } catch {
        return fallback;
      }
    }
    return fallback;
  }
}

function clampOutputText(value, max = 900) {
  const str = String(value || '').replace(/\s+/g, ' ').trim();
  if (!str) return '';
  if (str.length <= max) return str;
  return str.slice(0, max).replace(/[,\s;:.]+$/g, '').trim();
}

function chooseHorizonMix(yearsForward) {
  const y = Number(yearsForward) || 0;
  if (y <= 2) return { near: 0.6, mid: 0.3, long: 0.1 };
  if (y <= 5) return { near: 0.3, mid: 0.5, long: 0.2 };
  return { near: 0.1, mid: 0.4, long: 0.5 };
}

function pickStable(items, seed) {
  if (!Array.isArray(items) || !items.length) return null;
  const idx = stableHash(seed) % items.length;
  return items[idx];
}

function normalizeAngle(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  if (raw === 'impact' || raw === 'markets' || raw === 'policy' || raw === 'tech' || raw === 'society') return raw;
  return '';
}

function normalizeSection(value, sectionOrder) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw.toLowerCase() === 'us') return 'U.S.';
  for (const s of sectionOrder || []) {
    if (String(s).toLowerCase() === raw.toLowerCase()) return s;
  }
  return raw;
}

export function buildMockCuration({ day, sectionOrder, topicsBySection, econSignals, marketSignals }) {
  const sections = Array.isArray(sectionOrder) ? sectionOrder : [];
  const topThemes = [];
  for (const section of sections) {
    const topics = (topicsBySection.get(section) || []).slice(0, 10);
    for (const t of topics) {
      if (t && t.theme && !topThemes.includes(t.theme)) topThemes.push(t.theme);
    }
  }

  const dayBrief = {
    summary: clampOutputText(
      `Top signals cluster around: ${topThemes.slice(0, 10).join(' • ') || 'a mixed set of themes'}. The edition planner uses these to generate future-dated story lines by section.`,
      520
    ),
    sections: Object.fromEntries(
      sections.map((s) => {
        const themes = (topicsBySection.get(s) || []).slice(0, 6).map((t) => t.theme).filter(Boolean);
        const line = themes.length ? `Themes: ${themes.slice(0, 6).join(' • ')}.` : 'No topics available for this section today.';
        return [s, clampOutputText(line, 220)];
      })
    )
  };

  const editions = [];
  for (let yearsForward = 0; yearsForward <= 10; yearsForward++) {
    const editionDate = formatEditionDate(day, yearsForward);
    const mix = chooseHorizonMix(yearsForward);
    const used = new Set();
    const sectionsOut = {};

    for (const section of sections) {
      const topics = (topicsBySection.get(section) || []).slice(0, 40);
      const byBucket = {
        near: topics.filter((t) => t.horizon_bucket === 'near'),
        mid: topics.filter((t) => t.horizon_bucket === 'mid'),
        long: topics.filter((t) => t.horizon_bucket === 'long')
      };
      const want = {
        near: Math.round(5 * mix.near),
        mid: Math.round(5 * mix.mid),
        long: Math.max(0, 5 - Math.round(5 * mix.near) - Math.round(5 * mix.mid))
      };

      const selected = [];
      const tryFill = (bucketName, count) => {
        const pool = byBucket[bucketName] || [];
        const seed = `${day}|${yearsForward}|${section}|${bucketName}|mock`;
        const sorted = [...pool].sort((a, b) => stableHash(`${seed}|${a.topic_slug}`) - stableHash(`${seed}|${b.topic_slug}`));
        for (const t of sorted) {
          if (selected.length >= count) break;
          if (!t || !t.topic_slug) continue;
          if (used.has(t.topic_slug)) continue;
          selected.push(t);
          used.add(t.topic_slug);
        }
      };

      tryFill('near', want.near);
      tryFill('mid', want.mid);
      tryFill('long', want.long);
      if (selected.length < 5) {
        const seed = `${day}|${yearsForward}|${section}|any|mock`;
        const sorted = [...topics].sort((a, b) => stableHash(`${seed}|${a.topic_slug}`) - stableHash(`${seed}|${b.topic_slug}`));
        for (const t of sorted) {
          if (selected.length >= 5) break;
          if (!t || !t.topic_slug) continue;
          if (used.has(t.topic_slug)) continue;
          selected.push(t);
          used.add(t.topic_slug);
        }
      }

      const angleCycle = ['impact', 'markets', 'policy', 'tech', 'society'];
      sectionsOut[section] = selected.slice(0, 5).map((t, idx) => {
        const theme = t.theme || t.label || 'A Major Shift';
        const angle = angleCycle[idx % angleCycle.length];
        const titleTemplates = [
          `The Next Phase of ${theme}`,
          `A New Framework for ${theme}`,
          `Inside the New Politics of ${theme}`,
          `The Quiet Shift in ${theme}`,
          `${theme}: From Flashpoint to Policy`
        ];
        const title = titleTemplates[stableHash(`${day}|${yearsForward}|${section}|${t.topic_slug}|title`) % titleTemplates.length];
        const dek = clampOutputText(
          idx === 0
            ? `A future-dated lead built from today’s signals, written as if published on ${editionDate}.`
            : `A secondary slot with on-demand rendering directions; grounded in today’s evidence but written in the voice of ${editionDate}.`,
          220
        );
        const futureEvent = clampOutputText(
          `A concrete development around ${theme} lands on the docket in ${editionDate}, forcing institutions to adapt in public.`,
          200
        );
        const outline = idx === 0
          ? [
              `Lead with a specific event that happens on ${editionDate} and sets the stakes around ${theme}.`,
              `Use baseline sources only as background (what happened in ${day.slice(0, 4)}).`,
              `Explain who wins/loses, what changes operationally, and what comes next.`
            ]
          : [
              `Describe the future event and why it matters.`,
              `Anchor the backstory to baseline evidence; avoid copying baseline headlines.`
            ];

        return {
          rank: idx + 1,
          topic_slug: t.topic_slug,
          angle,
          title: clampOutputText(title, 120),
          dek,
          future_event: futureEvent,
          lede_seed: idx === 0 ? clampOutputText(`A new turn in ${theme} is reshaping priorities across the system, with effects that show up first in paperwork and timelines.`, 260) : '',
          nut_seed: idx === 0 ? clampOutputText(`The story now is less about rhetoric and more about implementation: budgets, staffing, audit trails, and what happens when edge cases hit scale.`, 280) : '',
          outline
        };
      });
    }

    const heroCandidate = (sectionsOut['U.S.'] || [])[0] || pickStable(Object.values(sectionsOut).flat(), `${day}|${yearsForward}|hero|mock`);
    const hero = heroCandidate
      ? { ...heroCandidate, section: 'U.S.', importance: 'hero', rank: 1 }
      : null;

    editions.push({
      yearsForward,
      editionDate,
      hero,
      sections: sectionsOut
    });
  }

  return {
    schema: 1,
    day,
    generatedAt: isoNow(),
    provider: 'mock',
    model: 'mock-curator',
    dayBrief,
    econ: (econSignals || []).slice(0, 8),
    markets: (marketSignals || []).slice(0, 10),
    editions
  };
}

function sonnetEnabled(mode) {
  const m = String(mode || '').trim().toLowerCase();
  return m && m !== 'off' && m !== 'disabled' && m !== '0' && m !== 'false';
}

function resolveProvider(mode) {
  const m = String(mode || '').trim().toLowerCase();
  if (m === 'openai') return 'openai';
  if (m === 'anthropic') return 'anthropic';
  if (m === 'http') return 'http';
  return 'auto';
}

function envKey(name) {
  return String(process.env[name] || '').trim();
}

async function fetchWithTimeout(url, options = {}) {
  const timeoutMs = Number(options.timeoutMs || 90000);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function callOpenAIJson({ model, system, user, timeoutMs }) {
  const apiKey = envKey('OPENAI_API_KEY') || envKey('SONNET_API_KEY');
  if (!apiKey) throw new Error('Missing OPENAI_API_KEY');
  const baseUrl = envKey('OPENAI_BASE_URL') || 'https://api.openai.com/v1';
  const url = `${baseUrl.replace(/\/+$/g, '')}/chat/completions`;
  const resp = await fetchWithTimeout(url, {
    timeoutMs,
    method: 'POST',
    headers: {
      authorization: `Bearer ${apiKey}`,
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      model,
      temperature: 0.35,
      response_format: { type: 'json_object' },
      messages: [
        { role: 'system', content: system },
        { role: 'user', content: user }
      ]
    })
  });
  if (!resp.ok) {
    const raw = await resp.text().catch(() => '');
    throw new Error(`OpenAI HTTP ${resp.status}: ${raw.slice(0, 240)}`);
  }
  const json = await resp.json();
  const text = json?.choices?.[0]?.message?.content || '';
  const parsed = safeParseJson(text, null);
  if (!parsed) throw new Error('OpenAI response was not JSON');
  return parsed;
}

async function callAnthropicJson({ model, system, user, timeoutMs }) {
  const apiKey = envKey('ANTHROPIC_API_KEY') || envKey('SONNET_API_KEY');
  if (!apiKey) throw new Error('Missing ANTHROPIC_API_KEY');
  const url = envKey('ANTHROPIC_BASE_URL') || 'https://api.anthropic.com/v1/messages';
  const resp = await fetchWithTimeout(url, {
    timeoutMs,
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': envKey('ANTHROPIC_VERSION') || '2023-06-01',
      'content-type': 'application/json'
    },
    body: JSON.stringify({
      model,
      max_tokens: 4096,
      system,
      messages: [{ role: 'user', content: user }]
    })
  });
  if (!resp.ok) {
    const raw = await resp.text().catch(() => '');
    throw new Error(`Anthropic HTTP ${resp.status}: ${raw.slice(0, 240)}`);
  }
  const json = await resp.json();
  const text = (json?.content || []).map((c) => c?.text || '').join('\n');
  const parsed = safeParseJson(text, null);
  if (!parsed) throw new Error('Anthropic response was not JSON');
  return parsed;
}

async function callHttpJson({ user, timeoutMs }) {
  const url = envKey('SONNET_HTTP_URL');
  if (!url) throw new Error('Missing SONNET_HTTP_URL');
  const resp = await fetchWithTimeout(url, {
    timeoutMs,
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ input: user })
  });
  if (!resp.ok) {
    const raw = await resp.text().catch(() => '');
    throw new Error(`HTTP curator ${resp.status}: ${raw.slice(0, 240)}`);
  }
  const json = await resp.json();
  return json;
}

function buildDayBriefPrompt({ day, sectionOrder, topicsBySection, econSignals, marketSignals }) {
  const sections = sectionOrder;
  const input = {
    day,
    sections,
    econ: (econSignals || []).slice(0, 8),
    markets: (marketSignals || []).slice(0, 10),
    topics: Object.fromEntries(
      sections.map((section) => [
        section,
        (topicsBySection.get(section) || []).slice(0, 12).map((t) => ({
          topic_slug: t.topic_slug,
          theme: t.theme || '',
          label: clampOutputText(t.label, 120),
          horizon_bucket: t.horizon_bucket,
          score: t.score ?? null
        }))
      ])
    )
  };

  const system =
    'You are a veteran newspaper editor and futures analyst. You build a daily brief from baseline signals for a speculative future-dated newspaper.\n' +
    'Return ONLY valid JSON. No markdown.';

  const user =
    'Create a concise daily brief that summarizes the dominant themes and trajectories.\n' +
    'Constraints:\n' +
    '- Do not copy source headlines.\n' +
    '- Do not call anything "baseline year"; use explicit years when needed.\n' +
    '- This brief will be used to generate future-dated stories (+0..+10 years).\n' +
    '\n' +
    'Return JSON with this shape:\n' +
    '{ "schema": 1, "dayBrief": { "summary": string, "sections": { "U.S.": string, ... } } }\n' +
    '\n' +
    `INPUT:\n${JSON.stringify(input)}`;

  return { system, user, input };
}

function buildEditionPlanPrompt({ day, yearsForward, editionDate, sectionOrder, topicsBySection, dayBrief }) {
  const sections = sectionOrder;
  const topicIndex = Object.fromEntries(
    sections.map((section) => [
      section,
      (topicsBySection.get(section) || []).slice(0, 18).map((t) => ({
        topic_slug: t.topic_slug,
        theme: t.theme || '',
        label: clampOutputText(t.label, 120),
        horizon_bucket: t.horizon_bucket
      }))
    ])
  );

  const input = {
    day,
    yearsForward,
    editionDate,
    sections,
    dayBrief,
    topics: topicIndex
  };

  const system =
    'You are a veteran newspaper editor and futures analyst. You are planning a full front page for a single future edition.\n' +
    'Return ONLY valid JSON. No markdown.';

  const user =
    `Plan the Future Times edition dated ${editionDate} (yearsForward=${yearsForward}).\n` +
    'You must pick story slots from the provided topic slugs (per section).\n' +
    'Write FUTURE-DATED headlines and deks: original events in the target year. Do not reuse the baseline source headline text.\n' +
    'Treat baseline signals as past context (optional) that can be referenced briefly.\n' +
    'Hero requirements:\n' +
    '- Hero must be the first story in U.S. (rank 1) and also provided as hero.\n' +
    '\n' +
    'Return JSON with this shape:\n' +
    '{\n' +
    '  "schema": 1,\n' +
    '  "yearsForward": number,\n' +
    '  "editionDate": string,\n' +
    '  "hero": { "section": "U.S.", "rank": 1, "topic_slug": string, "angle": "impact|markets|policy|tech|society", "title": string, "dek": string, "future_event": string, "lede_seed": string, "nut_seed": string, "outline": string[] },\n' +
    '  "sections": { "U.S.": StorySlot[5], "World": StorySlot[5], ... }\n' +
    '}\n' +
    'StorySlot fields:\n' +
    '- rank (1..5)\n' +
    '- topic_slug (must exist in that section topics list)\n' +
    '- angle\n' +
    '- title (present-tense in target year; not a question)\n' +
    '- dek (1-2 sentences)\n' +
    '- future_event (1 sentence describing what happened in the target year)\n' +
    '- lede_seed and nut_seed: REQUIRED for rank 1 stories; empty string for rank 2..5\n' +
    '- outline: 5-8 bullets for rank 1; 2-4 bullets for rank 2..5\n' +
    '\n' +
    `INPUT:\n${JSON.stringify(input)}`;

  return { system, user, input };
}

function normalizeSlot(slot, section, idx, sectionOrder) {
  const raw = slot && typeof slot === 'object' ? slot : {};
  const rank = Number(raw.rank) || (idx + 1);
  const topicSlug = String(raw.topic_slug || '').trim();
  const angle = normalizeAngle(raw.angle) || ['impact', 'markets', 'policy', 'tech', 'society'][idx % 5];
  return {
    rank,
    section: normalizeSection(section, sectionOrder),
    topic_slug: topicSlug,
    angle,
    title: clampOutputText(raw.title, 140),
    dek: clampOutputText(raw.dek, 260),
    future_event: clampOutputText(raw.future_event, 240),
    lede_seed: clampOutputText(raw.lede_seed, 520),
    nut_seed: clampOutputText(raw.nut_seed, 520),
    outline: Array.isArray(raw.outline) ? raw.outline.map((x) => clampOutputText(x, 200)).filter(Boolean).slice(0, 10) : []
  };
}

function normalizeEditionPlan(raw, { yearsForward, editionDate, sectionOrder }) {
  const plan = raw && typeof raw === 'object' ? raw : {};
  const out = {
    schema: 1,
    yearsForward,
    editionDate,
    hero: null,
    sections: {}
  };
  const sections = sectionOrder;
  const hero = plan.hero && typeof plan.hero === 'object' ? plan.hero : null;
  if (hero) {
    out.hero = normalizeSlot(hero, hero.section || 'U.S.', 0, sectionOrder);
    out.hero.section = 'U.S.';
    out.hero.rank = 1;
  }
  const sectionsObj = plan.sections && typeof plan.sections === 'object' ? plan.sections : {};
  for (const section of sections) {
    const slots = Array.isArray(sectionsObj[section]) ? sectionsObj[section] : [];
    const normalized = slots.slice(0, 5).map((s, idx) => normalizeSlot(s, section, idx, sectionOrder));
    while (normalized.length < 5) {
      normalized.push(normalizeSlot({}, section, normalized.length, sectionOrder));
    }
    // Ensure rank ordering.
    for (let i = 0; i < normalized.length; i++) normalized[i].rank = i + 1;
    // Ensure lead seeds exist for rank 1.
    if (!normalized[0].lede_seed) normalized[0].lede_seed = '';
    if (!normalized[0].nut_seed) normalized[0].nut_seed = '';
    out.sections[section] = normalized;
  }
  // Ensure hero matches U.S. rank 1.
  if (out.sections['U.S.'] && out.sections['U.S.'][0]) {
    const lead = out.sections['U.S.'][0];
    out.hero = out.hero || { ...lead, section: 'U.S.', rank: 1 };
    out.hero.topic_slug = out.hero.topic_slug || lead.topic_slug;
    out.hero.title = out.hero.title || lead.title;
    out.hero.dek = out.hero.dek || lead.dek;
    out.hero.future_event = out.hero.future_event || lead.future_event;
    out.hero.lede_seed = out.hero.lede_seed || lead.lede_seed;
    out.hero.nut_seed = out.hero.nut_seed || lead.nut_seed;
    out.hero.outline = (out.hero.outline && out.hero.outline.length) ? out.hero.outline : lead.outline;
  }
  return out;
}

export async function generateDailyCuration({ day, sectionOrder, topicsBySection, econSignals, marketSignals }) {
  const mode = String(process.env.SONNET_MODE || 'auto').trim().toLowerCase();
  if (!sonnetEnabled(mode)) {
    return { payload: null, provider: 'disabled', model: 'disabled', promptJson: null, error: null };
  }

  const providerMode = resolveProvider(mode);
  const timeoutMs = Math.max(20000, Math.min(180000, Number(process.env.SONNET_TIMEOUT_MS || 90000)));
  const model = envKey('SONNET_MODEL') || envKey('SONNET_MODEL_NAME') || 'sonnet-4.6';

  const callJson = async ({ system, user }) => {
    if (providerMode === 'openai') return callOpenAIJson({ model, system, user, timeoutMs });
    if (providerMode === 'anthropic') return callAnthropicJson({ model, system, user, timeoutMs });
    if (providerMode === 'http') return callHttpJson({ user, timeoutMs });

    // auto: prefer Anthropic if key present, else OpenAI, else mock.
    if (envKey('ANTHROPIC_API_KEY')) return callAnthropicJson({ model, system, user, timeoutMs });
    if (envKey('OPENAI_API_KEY')) return callOpenAIJson({ model, system, user, timeoutMs });
    return null;
  };

  const promptDayBrief = buildDayBriefPrompt({ day, sectionOrder, topicsBySection, econSignals, marketSignals });
  const dayBriefResp = await callJson({ system: promptDayBrief.system, user: promptDayBrief.user });
  const dayBrief = dayBriefResp && dayBriefResp.dayBrief ? dayBriefResp.dayBrief : null;
  if (!dayBrief || typeof dayBrief !== 'object') {
    throw new Error('Sonnet did not return dayBrief');
  }

  const editions = [];
  const editionPrompts = [];
  for (let yearsForward = 0; yearsForward <= 10; yearsForward++) {
    const editionDate = formatEditionDate(day, yearsForward);
    const promptEdition = buildEditionPlanPrompt({ day, yearsForward, editionDate, sectionOrder, topicsBySection, dayBrief });
    editionPrompts.push({ yearsForward, system: promptEdition.system, user: promptEdition.user });
    const planResp = await callJson({ system: promptEdition.system, user: promptEdition.user });
    const normalized = normalizeEditionPlan(planResp, { yearsForward, editionDate, sectionOrder });
    editions.push(normalized);
  }

  const promptJson = {
    dayBrief: { system: promptDayBrief.system, user: promptDayBrief.user },
    editions: editionPrompts.map((p) => ({ yearsForward: p.yearsForward, system: p.system, user: p.user }))
  };

  const provider =
    providerMode === 'auto'
      ? (envKey('ANTHROPIC_API_KEY') ? 'anthropic' : envKey('OPENAI_API_KEY') ? 'openai' : 'mock')
      : providerMode;

  return {
    provider,
    model,
    promptJson,
    error: null,
    payload: {
      schema: 1,
      day,
      generatedAt: isoNow(),
      provider,
      model,
      dayBrief: {
        summary: clampOutputText(dayBrief.summary, 900),
        sections: dayBrief.sections && typeof dayBrief.sections === 'object' ? dayBrief.sections : {}
      },
      editions
    }
  };
}

export function validateCuratedEditionPlan(plan, { sectionOrder, topicsBySection }) {
  const errors = [];
  const p = plan && typeof plan === 'object' ? plan : {};
  if (!Array.isArray(sectionOrder) || !sectionOrder.length) {
    errors.push('missing section order');
    return errors;
  }
  if (!p.sections || typeof p.sections !== 'object') {
    errors.push('missing sections');
    return errors;
  }
  const used = new Set();
  for (const section of sectionOrder) {
    const slots = Array.isArray(p.sections[section]) ? p.sections[section] : [];
    if (slots.length < 3) {
      errors.push(`section ${section} has too few slots`);
      continue;
    }
    const allowed = new Set((topicsBySection.get(section) || []).map((t) => t.topic_slug).filter(Boolean));
    for (const slot of slots.slice(0, 5)) {
      const slug = String(slot?.topic_slug || '').trim();
      if (!slug) continue;
      if (!allowed.has(slug)) errors.push(`unknown topic_slug ${slug} for section ${section}`);
      if (used.has(slug)) errors.push(`duplicate topic_slug ${slug} across edition`);
      used.add(slug);
    }
  }
  return errors;
}

