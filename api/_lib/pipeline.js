import fs from 'node:fs';
import path from 'node:path';

import { FutureTimesPipeline } from '../../server/pipeline/pipeline.js';

let pipelineSingleton = null;

function ensureTmpDbPath() {
  const root = process.cwd();
  const bundled = path.resolve(root, 'data', 'future-times.sqlite');
  const tmpDir = path.resolve('/tmp', 'futurenews');
  const tmpDb = path.resolve(tmpDir, 'future-times.sqlite');

  try {
    fs.mkdirSync(tmpDir, { recursive: true });
  } catch {
    // ignore
  }

  if (!fs.existsSync(tmpDb) && fs.existsSync(bundled)) {
    try {
      fs.copyFileSync(bundled, tmpDb);
    } catch {
      // ignore
    }
  }

  // If we cannot copy (or there is no bundled DB), fall back to using /tmp directly.
  return fs.existsSync(tmpDb) ? tmpDb : tmpDb;
}

export function getPipeline() {
  if (pipelineSingleton) return pipelineSingleton;

  const root = process.cwd();
  const dbFile = ensureTmpDbPath();
  const sourcesFile = path.resolve(root, 'config', 'sources.json');

  const pipeline = new FutureTimesPipeline({
    rootDir: root,
    dbFile,
    sourcesFile
  });
  pipeline.init();
  pipelineSingleton = pipeline;
  return pipelineSingleton;
}

