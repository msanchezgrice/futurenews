import { getPipeline } from './_lib/pipeline.js';
import { sendHtml, sendJson } from './_lib/response.js';
import { formatDay, normalizeDay } from '../server/pipeline/utils.js';

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderSnapshotHtml(snapshot) {
  const day = escapeHtml(snapshot?.day || '');
  const generatedAt = escapeHtml(snapshot?.generatedAt || '');
  const pretty = escapeHtml(JSON.stringify(snapshot, null, 2));

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Future Times Signal Pack ${day || 'latest'}</title>
    <style>
      body { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; margin: 24px; line-height: 1.4; }
      header { margin-bottom: 16px; }
      pre { white-space: pre-wrap; background: #0b1020; color: #e6e6e6; padding: 16px; border-radius: 10px; overflow: auto; }
      small { color: #555; }
    </style>
  </head>
  <body>
    <header>
      <h1>Signal Pack: ${day || 'latest'}</h1>
      <small>generatedAt: ${generatedAt || 'unknown'}</small>
    </header>
    <pre>${pretty}</pre>
  </body>
</html>`;
}

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    sendJson(res, { error: 'method_not_allowed' }, 405);
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const format = String(url.searchParams.get('format') || '').trim().toLowerCase();
  const wantsHtml = format === 'html';
  const requestedDay = normalizeDay(url.searchParams.get('day'));

  const pipeline = getPipeline();
  const day = requestedDay || pipeline.getLatestDay() || formatDay();

  let snapshot = pipeline.getDaySignalSnapshot(day);
  if (!snapshot) {
    try {
      snapshot = pipeline.ensureDaySignalSnapshot(day);
    } catch {
      snapshot = null;
    }
  }

  if (!snapshot) {
    sendJson(res, { error: 'signal_snapshot_not_found', day }, 404);
    return;
  }

  if (wantsHtml) {
    sendHtml(res, renderSnapshotHtml(snapshot));
    return;
  }

  sendJson(res, snapshot);
}

