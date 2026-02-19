function readBool(value, fallback = false) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return fallback;
  if (raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on') return true;
  if (raw === '0' || raw === 'false' || raw === 'no' || raw === 'off') return false;
  return fallback;
}

function readInt(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const rounded = Math.round(n);
  return Math.max(min, Math.min(max, rounded));
}

export function getFutureImagesFlags() {
  return {
    imagesEnabled: readBool(process.env.FT_IMAGES_ENABLED, false),
    ideasEnabled: readBool(process.env.FT_IDEAS_ENABLED, false),
    storyHeroEnabled: readBool(process.env.FT_IMAGES_STORY_HERO_ENABLED, false),
    autoTopN: readInt(process.env.FT_IMAGES_AUTO_TOP_N, 5, 0, 50)
  };
}

export function hasPostgresConfig() {
  return Boolean(String(process.env.POSTGRES_URL || process.env.DATABASE_URL || '').trim());
}

export function hasBlobConfig() {
  return Boolean(String(process.env.BLOB_READ_WRITE_TOKEN || '').trim());
}

export function getAnthropicApiKey() {
  // Reuse existing conventions in this repo.
  return (
    String(process.env.ANTHROPIC_API_KEY || '').trim() ||
    String(process.env.SONNET_API_KEY || '').trim()
  );
}

export function getIdeasModelCandidates() {
  const primary = String(process.env.IDEAS_MODEL || 'claude-3-7-sonnet-20250219').trim();
  return [
    primary,
    'claude-3-7-sonnet-20250219',
    'claude-haiku-4-5-20251001'
  ].filter(Boolean);
}

export function getPromptModelCandidates() {
  const primary = String(process.env.IMAGE_PROMPT_MODEL || 'claude-3-7-sonnet-20250219').trim();
  return [
    primary,
    'claude-3-7-sonnet-20250219',
    'claude-haiku-4-5-20251001'
  ].filter(Boolean);
}

export function getNanoBananaConfig() {
  return {
    apiUrl: String(process.env.NANOBANANA_API_URL || '').trim(),
    apiKey: String(process.env.NANOBANANA_API_KEY || '').trim(),
    model: String(process.env.NANOBANANA_MODEL || 'nano-banana-3-pro').trim() || 'nano-banana-3-pro'
  };
}

export function getGeminiConfig() {
  return {
    apiKey: String(process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY || '').trim(),
    apiBaseUrl: String(process.env.GEMINI_API_URL || 'https://generativelanguage.googleapis.com/v1beta').trim(),
    model: String(process.env.GEMINI_IMAGE_MODEL || 'gemini-3-pro-image-preview').trim() || 'gemini-3-pro-image-preview',
    fallbackModel: 'gemini-2.5-flash-image'
  };
}
