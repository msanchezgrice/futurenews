import { createPool } from '@vercel/postgres';

let _pool = null;
let _poolInitAttempted = false;
let _schemaPromise = null;
let _poolInitError = null;

export function getPoolInitError() {
  return _poolInitError ? String(_poolInitError?.message || _poolInitError) : '';
}

export function getPostgresPool() {
  if (_poolInitAttempted) return _pool;
  _poolInitAttempted = true;

  const connectionString = String(process.env.POSTGRES_URL || process.env.DATABASE_URL || '').trim();
  if (!connectionString) return null;

  try {
    _pool = createPool({ connectionString });
  } catch (err) {
    _poolInitError = err;
    _pool = null;
  }

  return _pool;
}

export async function ensureFutureImagesSchema() {
  const pool = getPostgresPool();
  if (!pool) return { ok: false, error: 'postgres_not_configured', detail: getPoolInitError() || null };
  if (_schemaPromise) return _schemaPromise;

  _schemaPromise = (async () => {
    // Keep schema in Postgres (not sqlite) so it persists on Vercel.
    await pool.query(`
      CREATE TABLE IF NOT EXISTS ft_future_ideas (
        idea_id TEXT PRIMARY KEY,
        day TEXT NOT NULL,
        years_forward INTEGER NOT NULL,
        rank INTEGER NOT NULL,
        score REAL NOT NULL,
        confidence INTEGER NOT NULL,
        title TEXT NOT NULL,
        object_type TEXT NOT NULL,
        description TEXT NOT NULL,
        scene TEXT NOT NULL,
        prompt_json TEXT NOT NULL,
        sources_json TEXT NOT NULL,
        model TEXT NOT NULL,
        generated_at TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        UNIQUE(day, years_forward, rank)
      );
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_future_ideas_day ON ft_future_ideas(day, years_forward);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_future_ideas_status ON ft_future_ideas(status);`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ft_image_jobs (
        job_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        day TEXT NOT NULL,
        years_forward INTEGER NOT NULL,
        story_id TEXT,
        section TEXT,
        idea_id TEXT,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        prompt_hash TEXT NOT NULL,
        prompt_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'queued',
        attempts INTEGER NOT NULL DEFAULT 0,
        priority INTEGER NOT NULL DEFAULT 100,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT,
        last_error TEXT
      );
    `);

    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_ft_image_jobs_unique
      ON ft_image_jobs(kind, day, years_forward, provider, prompt_hash, COALESCE(story_id, ''), COALESCE(idea_id, ''));
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_image_jobs_status ON ft_image_jobs(status);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_image_jobs_day ON ft_image_jobs(day, years_forward);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_image_jobs_priority ON ft_image_jobs(priority, created_at);`);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS ft_image_assets (
        asset_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        day TEXT NOT NULL,
        years_forward INTEGER NOT NULL,
        kind TEXT NOT NULL,
        story_id TEXT,
        section TEXT,
        idea_id TEXT,
        provider TEXT NOT NULL,
        model TEXT NOT NULL,
        prompt_json TEXT NOT NULL,
        blob_url TEXT NOT NULL,
        mime_type TEXT NOT NULL,
        width INTEGER NOT NULL,
        height INTEGER NOT NULL,
        alt_text TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
    `);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_image_assets_day ON ft_image_assets(day, years_forward, kind);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_image_assets_story ON ft_image_assets(story_id);`);
    await pool.query(`CREATE INDEX IF NOT EXISTS idx_ft_image_assets_idea ON ft_image_assets(idea_id);`);

    return { ok: true };
  })();

  return _schemaPromise;
}

export async function pgQuery(text, params = []) {
  const pool = getPostgresPool();
  if (!pool) {
    const err = new Error('POSTGRES_URL not configured');
    err.code = 'postgres_not_configured';
    throw err;
  }
  return pool.query(text, params);
}
