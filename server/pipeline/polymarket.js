import { canonicalizeUrl } from './utils.js';

function parseNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function yesProbability(market) {
  const outcomes = Array.isArray(market?.outcomes) ? market.outcomes : null;
  const prices = Array.isArray(market?.outcomePrices) ? market.outcomePrices : null;
  if (!outcomes || !prices || outcomes.length !== prices.length) return null;
  const yesIdx = outcomes.findIndex((o) => String(o || '').toLowerCase() === 'yes');
  if (yesIdx < 0) return null;
  const p = parseNumber(prices[yesIdx]);
  if (p === null) return null;
  if (p >= 0 && p <= 1) return p;
  if (p > 1 && p <= 100) return p / 100;
  return null;
}

export async function fetchPolymarketRawItems(url, fetchedAtIso) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 9000);
  const response = await fetch(url, {
    headers: {
      'user-agent': 'FutureTimesBot/1.0',
      accept: 'application/json'
    },
    signal: controller.signal
  }).finally(() => clearTimeout(timeout));
  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Polymarket fetch failed ${response.status}: ${text.slice(0, 140)}`);
  }
  const payload = await response.json();
  const markets = Array.isArray(payload) ? payload : Array.isArray(payload?.markets) ? payload.markets : [];
  const items = [];

  for (const market of markets) {
    const question = String(market?.question || market?.title || '').trim();
    if (!question) continue;
    const slug = String(market?.slug || '').trim();
    const marketUrl = slug ? `https://polymarket.com/market/${slug}` : '';
    const canonicalUrl = canonicalizeUrl(marketUrl || String(market?.url || market?.link || '').trim());
    const prob = yesProbability(market);
    const endDate = String(market?.endDate || market?.closeTime || market?.end || '').trim();
    const volume = parseNumber(market?.volumeNum ?? market?.volume ?? market?.volumeUsd);
    const summaryBits = [];
    if (prob !== null) summaryBits.push(`Yes: ${(prob * 100).toFixed(0)}%`);
    if (endDate) summaryBits.push(`Closes: ${endDate.slice(0, 10)}`);
    if (volume !== null) summaryBits.push(`Volume: $${Math.round(volume).toLocaleString('en-US')}`);

    items.push({
      title: question,
      summary: summaryBits.join(' • '),
      link: canonicalUrl,
      publishedAt: fetchedAtIso,
      payloadJson: market
    });
  }

  return items;
}
