import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { isoNow } from './utils.js';

const DEFAULT_DIR = '.futurenews';
const DEFAULT_FILE = 'runtime.json';

function safeParseJson(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function getRuntimeConfigFilePath() {
  const override = String(process.env.FUTURENEWS_RUNTIME_CONFIG_FILE || '').trim();
  if (override) return override;
  const home = typeof os.homedir === 'function' ? os.homedir() : '';
  const base = home || process.cwd();
  return path.resolve(base, DEFAULT_DIR, DEFAULT_FILE);
}

export function getRuntimeConfigInfo() {
  const file = getRuntimeConfigFilePath();
  let exists = false;
  try {
    exists = fs.existsSync(file);
  } catch {
    exists = false;
  }
  return { file, exists };
}

export function readRuntimeConfig() {
  const file = getRuntimeConfigFilePath();
  try {
    if (!fs.existsSync(file)) return null;
    const raw = fs.readFileSync(file, 'utf8');
    const parsed = safeParseJson(raw);
    if (!parsed || typeof parsed !== 'object') return null;
    return parsed;
  } catch {
    return null;
  }
}

function ensureDirSecure(dir) {
  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch {
    // ignore
  }
  // Best-effort permission hardening (POSIX only).
  try {
    fs.chmodSync(dir, 0o700);
  } catch {
    // ignore
  }
}

export function writeRuntimeConfig(payload) {
  const file = getRuntimeConfigFilePath();
  const dir = path.dirname(file);
  ensureDirSecure(dir);

  const tmp = `${file}.tmp`;
  const json = JSON.stringify(payload, null, 2);
  fs.writeFileSync(tmp, json, { encoding: 'utf8', mode: 0o600 });
  fs.renameSync(tmp, file);
  try {
    fs.chmodSync(file, 0o600);
  } catch {
    // ignore
  }
  return { file };
}

export function readOpusRuntimeConfig() {
  const cfg = readRuntimeConfig();
  const opus = cfg && typeof cfg === 'object' ? cfg.opus : null;
  return opus && typeof opus === 'object' ? opus : null;
}

function applyPatch(target, patch) {
  const next = { ...(target && typeof target === 'object' ? target : {}) };
  for (const [k, v] of Object.entries(patch || {})) {
    if (v === undefined) continue;
    if (v === null) {
      delete next[k];
      continue;
    }
    if (typeof v === 'string' && !String(v).trim()) {
      delete next[k];
      continue;
    }
    next[k] = v;
  }
  return next;
}

export function updateOpusRuntimeConfig(patch) {
  const existing = readRuntimeConfig() || { schema: 1 };
  const next = {
    ...existing,
    schema: 1,
    updatedAt: isoNow(),
    opus: applyPatch(existing.opus, patch)
  };
  return { ...getRuntimeConfigInfo(), ...writeRuntimeConfig(next), config: next };
}

