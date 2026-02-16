function stripFence(value) {
  const trimmed = String(value || '').trim();
  if (trimmed.startsWith('```')) {
    return trimmed.replace(/^```[a-z]*\n?/i, '').replace(/```$/i, '').trim();
  }
  return trimmed;
}

export function safeParseJson(text, fallback = null) {
  const raw = String(text || '').trim();
  if (!raw) return fallback;
  const cleaned = stripFence(raw);
  try {
    return JSON.parse(cleaned);
  } catch {
    // Attempt to recover the first JSON object/array from a blob.
    const firstObj = cleaned.indexOf('{');
    const lastObj = cleaned.lastIndexOf('}');
    if (firstObj >= 0 && lastObj > firstObj) {
      try {
        return JSON.parse(cleaned.slice(firstObj, lastObj + 1));
      } catch {
        // fallthrough
      }
    }
    const firstArr = cleaned.indexOf('[');
    const lastArr = cleaned.lastIndexOf(']');
    if (firstArr >= 0 && lastArr > firstArr) {
      try {
        return JSON.parse(cleaned.slice(firstArr, lastArr + 1));
      } catch {
        return fallback;
      }
    }
    return fallback;
  }
}

function sortForStableJson(value) {
  if (Array.isArray(value)) return value.map(sortForStableJson);
  if (!value || typeof value !== 'object') return value;
  const out = {};
  for (const key of Object.keys(value).sort()) {
    out[key] = sortForStableJson(value[key]);
  }
  return out;
}

export function stableStringify(value) {
  return JSON.stringify(sortForStableJson(value));
}

