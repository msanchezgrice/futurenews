import path from 'node:path';

import { FutureTimesPipeline } from './pipeline/pipeline.js';
import { formatDay } from './pipeline/utils.js';

const ROOT_DIR = process.cwd();
const pipeline = new FutureTimesPipeline({
  rootDir: ROOT_DIR,
  dbFile: process.env.PIPELINE_DB_FILE || path.resolve(ROOT_DIR, 'data', 'future-times.sqlite'),
  sourcesFile: process.env.PIPELINE_SOURCES_FILE || path.resolve(ROOT_DIR, 'config', 'sources.json')
});

pipeline.init();

const REFRESH_MS = process.env.PIPELINE_REFRESH_MS ? Number(process.env.PIPELINE_REFRESH_MS) : 0;
const DAILY_HHMM = String(process.env.PIPELINE_DAILY_HHMM || '05:30').trim();
const AUTO_CURATE = process.env.SONNET_AUTO_CURATE !== 'false';

function parseDailyTime(value) {
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{1,2}):(\d{2})$/);
  if (!match) return { hour: 5, minute: 30, label: '05:30' };
  const hour = Math.max(0, Math.min(23, Number(match[1])));
  const minute = Math.max(0, Math.min(59, Number(match[2])));
  const label = `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
  return { hour, minute, label };
}

function msUntilNextLocalTime(hour, minute) {
  const now = new Date();
  const next = new Date(now);
  next.setHours(hour, minute, 0, 0);
  if (next <= now) {
    next.setDate(next.getDate() + 1);
  }
  return next.getTime() - now.getTime();
}

async function tick() {
  try {
    const day = formatDay();
    await pipeline.refresh({ day, force: false });
    const curation = AUTO_CURATE
      ? await pipeline.curateDay(day, { force: false }).catch((err) => ({
          ok: false,
          error: String(err?.message || err)
        }))
      : null;
    const status = pipeline.getStatus();
    const curated = curation && curation.ok ? ` curated=${curation.curatedStories ?? 0} key=${curation.keyStories ?? 0}` : '';
    const curErr = curation && curation.ok === false ? ` curate_error=${curation.error || 'unknown'}` : '';
    process.stdout.write(
      `[worker] ${new Date().toISOString()} day=${status.day} raw=${status.rawItems} signals=${status.signals} topics=${status.topics} editions=${status.editions} curations=${status.curations}${AUTO_CURATE ? '' : ' (auto-curate disabled)'}${curated}${curErr}\n`
    );
  } catch (err) {
    process.stderr.write(`[worker] refresh failed: ${err?.message || err}\n`);
  }
}

await tick();

if (Number.isFinite(REFRESH_MS) && REFRESH_MS > 0) {
  setInterval(tick, REFRESH_MS);
} else {
  const daily = parseDailyTime(DAILY_HHMM);
  const schedule = () => {
    const delay = msUntilNextLocalTime(daily.hour, daily.minute);
    const minutes = Math.max(1, Math.round(delay / 60000));
    process.stdout.write(`[worker] next scheduled refresh in ~${minutes} min (at ${daily.label} local)\n`);
    setTimeout(async () => {
      await tick();
      schedule();
    }, delay);
  };
  schedule();
}
