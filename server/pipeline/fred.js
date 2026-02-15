import { canonicalizeUrl } from './utils.js';

function parseCsv(text) {
  const lines = String(text || '').split('\n').map((l) => l.trim());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    const parts = line.split(',');
    if (parts.length < 2) continue;
    const date = parts[0];
    const value = parts[1];
    rows.push({ date, value });
  }
  return rows;
}

function lastNumeric(rows) {
  for (let i = rows.length - 1; i >= 0; i--) {
    const raw = rows[i]?.value;
    const n = Number(raw);
    if (Number.isFinite(n)) return { date: rows[i].date, value: n };
  }
  return null;
}

function approxTrend(rows, stepsBack = 30) {
  const last = lastNumeric(rows);
  if (!last) return null;
  const idx = Math.max(0, rows.length - 1 - stepsBack);
  let past = null;
  for (let i = idx; i >= 0; i--) {
    const n = Number(rows[i]?.value);
    if (Number.isFinite(n)) {
      past = { date: rows[i].date, value: n };
      break;
    }
  }
  if (!past) return null;
  return { delta: last.value - past.value, from: past.date, to: last.date };
}

export async function fetchFredSeriesRawItem(url, fetchedAtIso, seriesIdHint = '') {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);
  const response = await fetch(url, {
    headers: {
      'user-agent': 'FutureTimesBot/1.0',
      accept: 'text/csv'
    },
    signal: controller.signal
  }).finally(() => clearTimeout(timeout));
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`FRED fetch failed ${response.status}: ${text.slice(0, 140)}`);
  }
  const csv = await response.text();
  const rows = parseCsv(csv);
  const last = lastNumeric(rows);
  if (!last) {
    throw new Error('FRED series had no numeric points');
  }
  const trend = approxTrend(rows, 30);
  const seriesId = seriesIdHint || (new URL(url).searchParams.get('id') || '').trim();
  const sourceUrl = seriesId ? `https://fred.stlouisfed.org/series/${seriesId}` : url;
  const canonicalUrl = canonicalizeUrl(sourceUrl);

  const trendText = trend ? `30-step delta: ${trend.delta >= 0 ? '+' : ''}${trend.delta.toFixed(2)}` : '';
  const title = seriesId ? `Economic indicator ${seriesId}` : 'Economic indicator';
  const summary = `${last.date}: ${last.value}${trendText ? ` • ${trendText}` : ''}`;

  return {
    title,
    summary,
    link: canonicalUrl,
    publishedAt: fetchedAtIso,
    payloadJson: { seriesId, last, trend }
  };
}
