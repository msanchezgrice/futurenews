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
const day = formatDay();
await pipeline.refresh({ day, force: true });
if (process.env.OPUS_AUTO_CURATE !== 'false') {
  await pipeline.curateDay(day, { force: true });
}
console.log(JSON.stringify(pipeline.getStatus(), null, 2));
