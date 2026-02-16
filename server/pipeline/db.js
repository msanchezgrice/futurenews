import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

function shouldAutoRecoverMalformedDb() {
  return Boolean(process.env.VERCEL) || String(process.env.FT_DB_AUTO_RECOVER || '').toLowerCase() === 'true';
}

function checkDatabaseIntegrity(db) {
  try {
    const rows = db.prepare('PRAGMA quick_check;').all();
    if (!Array.isArray(rows) || rows.length === 0) {
      return { ok: false, reason: 'quick_check_empty' };
    }
    for (const row of rows) {
      const message = String(Object.values(row || {})[0] || '').trim().toLowerCase();
      if (message !== 'ok') {
        return { ok: false, reason: message || 'quick_check_failed' };
      }
    }
    return { ok: true };
  } catch (err) {
    return { ok: false, reason: String(err?.message || err) };
  }
}

export function openDatabase(dbFilePath) {
  let effectivePath = dbFilePath;

  // In Vercel serverless (read-only filesystem), copy DB to /tmp for write access.
  // On every new deployment, the bundled DB has pre-curated data that must be copied.
  if (process.env.VERCEL && dbFilePath && !dbFilePath.startsWith('/tmp')) {
    const tmpPath = path.join('/tmp', path.basename(dbFilePath));
    try {
      // Always copy from the bundled DB — each deploy bundles the latest curated data.
      // Runtime curations from the same function instance (warm start) will be written
      // back to /tmp, but on cold start we always start from the deploy-time snapshot.
      if (fs.existsSync(dbFilePath)) {
        fs.copyFileSync(dbFilePath, tmpPath);
      }
      effectivePath = tmpPath;
    } catch (err) {
      console.warn(`DB copy to /tmp failed: ${err.message}, trying original path`);
    }
  }

  fs.mkdirSync(path.dirname(effectivePath), { recursive: true });
  let db = new DatabaseSync(effectivePath);
  if (shouldAutoRecoverMalformedDb()) {
    const integrity = checkDatabaseIntegrity(db);
    if (!integrity.ok) {
      console.warn(`DB integrity check failed for ${effectivePath}: ${integrity.reason}. Recreating file.`);
      try {
        db.close();
      } catch {
        // ignore
      }
      try {
        fs.rmSync(effectivePath, { force: true });
      } catch {
        // ignore
      }
      db = new DatabaseSync(effectivePath);
    }
  }
  db.exec('PRAGMA journal_mode = WAL;');
  db.exec('PRAGMA synchronous = NORMAL;');
  db.exec('PRAGMA foreign_keys = ON;');
  return db;
}

export function migrate(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS sources (
      source_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      type TEXT NOT NULL,
      section TEXT,
      url TEXT NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1,
      fetch_interval_minutes INTEGER NOT NULL DEFAULT 60,
      last_fetched_at TEXT,
      last_error TEXT,
      last_status INTEGER,
      last_item_count INTEGER,
      meta_json TEXT
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS raw_items (
      raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_id TEXT,
      day TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      published_at TEXT,
      canonical_url TEXT,
      title TEXT NOT NULL,
      summary TEXT,
      payload_json TEXT,
      fingerprint TEXT NOT NULL UNIQUE,
      section_hint TEXT
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS signals (
      signal_id INTEGER PRIMARY KEY AUTOINCREMENT,
      raw_id INTEGER,
      day TEXT NOT NULL,
      section TEXT NOT NULL,
      signal_type TEXT NOT NULL,
      title TEXT NOT NULL,
      published_at TEXT,
      summary TEXT,
      canonical_url TEXT,
      entities_json TEXT,
      keywords_json TEXT,
      horizon_bucket TEXT,
      score REAL,
      citations_json TEXT
    );
  `);

  // Best-effort forward migration for earlier DBs.
  try {
    db.exec('ALTER TABLE signals ADD COLUMN published_at TEXT;');
  } catch {
    // ignore if already present
  }

  db.exec(`CREATE INDEX IF NOT EXISTS idx_signals_day_section ON signals(day, section);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_raw_items_day ON raw_items(day);`);

  db.exec(`
    CREATE TABLE IF NOT EXISTS topics (
      topic_id INTEGER PRIMARY KEY AUTOINCREMENT,
      day TEXT NOT NULL,
      section TEXT NOT NULL,
      label TEXT NOT NULL,
      brief TEXT NOT NULL,
      horizon_bucket TEXT NOT NULL,
      topic_slug TEXT NOT NULL,
      evidence_signal_ids_json TEXT,
      evidence_links_json TEXT,
      score REAL,
      UNIQUE(day, topic_slug)
    );
  `);

  db.exec(`CREATE INDEX IF NOT EXISTS idx_topics_day_section ON topics(day, section);`);

  db.exec(`
    CREATE TABLE IF NOT EXISTS editions (
      edition_id INTEGER PRIMARY KEY AUTOINCREMENT,
      day TEXT NOT NULL,
      years_forward INTEGER NOT NULL,
      generated_at TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      version TEXT NOT NULL,
      UNIQUE(day, years_forward)
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS edition_stories (
      story_id TEXT PRIMARY KEY,
      edition_id INTEGER NOT NULL,
      section TEXT NOT NULL,
      rank INTEGER NOT NULL,
      topic_id INTEGER NOT NULL,
      angle TEXT NOT NULL,
      headline_seed TEXT NOT NULL,
      dek_seed TEXT NOT NULL,
      evidence_pack_json TEXT NOT NULL,
      UNIQUE(edition_id, section, rank)
    );
  `);

  db.exec(`CREATE INDEX IF NOT EXISTS idx_edition_stories_edition ON edition_stories(edition_id);`);

  db.exec(`
    CREATE TABLE IF NOT EXISTS render_cache (
      cache_key TEXT PRIMARY KEY,
      story_id TEXT NOT NULL,
      generated_at TEXT NOT NULL,
      article_json TEXT NOT NULL
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_render_cache_story ON render_cache(story_id);`);

  db.exec(`
    CREATE TABLE IF NOT EXISTS day_signal_snapshots (
      day TEXT PRIMARY KEY,
      generated_at TEXT NOT NULL,
      snapshot_json TEXT NOT NULL
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS day_curations (
      day TEXT PRIMARY KEY,
      generated_at TEXT NOT NULL,
      provider TEXT NOT NULL,
      model TEXT NOT NULL,
      prompt_json TEXT,
      payload_json TEXT NOT NULL,
      error TEXT
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS story_curations (
      story_id TEXT PRIMARY KEY,
      day TEXT NOT NULL,
      years_forward INTEGER NOT NULL,
      section TEXT NOT NULL,
      rank INTEGER NOT NULL,
      generated_at TEXT NOT NULL,
      model TEXT NOT NULL,
      key_story INTEGER NOT NULL DEFAULT 0,
      plan_json TEXT NOT NULL,
      article_json TEXT
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_story_curations_day ON story_curations(day);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_story_curations_day_years ON story_curations(day, years_forward);`);

  db.exec(`
    CREATE TABLE IF NOT EXISTS day_event_traces (
      event_id INTEGER PRIMARY KEY AUTOINCREMENT,
      day TEXT NOT NULL,
      ts TEXT NOT NULL,
      event_type TEXT NOT NULL,
      payload_json TEXT
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_day_event_traces_day ON day_event_traces(day);`);

  // ── Standing Topics Registry (persistent, curated topics for structured forecasting) ──
  db.exec(`
    CREATE TABLE IF NOT EXISTS standing_topics (
      topic_key TEXT PRIMARY KEY,
      section TEXT NOT NULL,
      category TEXT,
      subcategory TEXT,
      label TEXT NOT NULL,
      description TEXT,
      extrapolation_axes JSON,
      keywords JSON,
      milestones JSON,
      enabled INTEGER DEFAULT 1,
      created_at TEXT,
      updated_at TEXT
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_standing_topics_section ON standing_topics(section);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_standing_topics_enabled ON standing_topics(enabled);`);

  // ── Topic Evidence (maps signals to standing topics with relevance scoring) ──
  db.exec(`
    CREATE TABLE IF NOT EXISTS topic_evidence (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      standing_topic_key TEXT NOT NULL,
      signal_id INTEGER NOT NULL,
      day TEXT NOT NULL,
      relevance_score REAL,
      matched_keywords JSON,
      ai_category TEXT,
      created_at TEXT,
      UNIQUE(standing_topic_key, signal_id)
    );
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_topic_evidence_topic ON topic_evidence(standing_topic_key);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_topic_evidence_day ON topic_evidence(day);`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_topic_evidence_topic_day ON topic_evidence(standing_topic_key, day);`);

  const schemaVersion = '2';
  db.prepare('INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)').run('schema_version', schemaVersion);
}
