import { getPipeline } from './_lib/pipeline.js';
import { sendHtml, sendJson } from './_lib/response.js';
import { formatDay, normalizeDay } from '../server/pipeline/utils.js';

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderSnapshotHtml(snapshot) {
  const day = escapeHtml(snapshot?.day || '');
  const generatedAt = escapeHtml(snapshot?.generatedAt || '');
  const pretty = escapeHtml(JSON.stringify(snapshot, null, 2));
  return `<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\"/>\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>\n<title>Future Times Signal Pack ${day}</title>\n<style>\nbody{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;margin:24px;line-height:1.4;}\nheader{margin-bottom:16px;}\npre{white-space:pre-wrap;background:#0b1020;color:#e6e6e6;padding:16px;border-radius:10px;overflow:auto;}\nsmall{color:#555}\n</style>\n</head>\n<body>\n<header>\n<h1>Signal Pack: ${day || 'latest'}</h1>\n<small>generatedAt: ${generatedAt || 'unknown'}</small>\n</header>\n<pre>${pretty}</pre>\n</body>\n</html>`;\n}\n\nexport default async function handler(req, res) {\n  if (req.method !== 'GET') {\n    sendJson(res, { error: 'method_not_allowed' }, 405);\n    return;\n  }\n\n  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);\n  const format = String(url.searchParams.get('format') || '').trim().toLowerCase();\n  const wantsHtml = format === 'html';\n  const requestedDay = normalizeDay(url.searchParams.get('day'));\n\n  const pipeline = getPipeline();\n  const day = requestedDay || pipeline.getLatestDay() || formatDay();\n\n  let snapshot = pipeline.getDaySignalSnapshot(day);\n  if (!snapshot) {\n    try {\n      snapshot = pipeline.ensureDaySignalSnapshot(day);\n    } catch {\n      snapshot = null;\n    }\n  }\n\n  if (!snapshot) {\n    sendJson(res, { error: 'signal_snapshot_not_found', day }, 404);\n    return;\n  }\n\n  if (wantsHtml) {\n    sendHtml(res, renderSnapshotHtml(snapshot));\n    return;\n  }\n\n  sendJson(res, snapshot);\n}\n+
