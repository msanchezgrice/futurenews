import { getPipeline } from './_lib/pipeline.js';
import { sendJson } from './_lib/response.js';
import { clampYears, formatDay, normalizeDay } from '../server/pipeline/utils.js';

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    sendJson(res, { error: 'method_not_allowed' }, 405);
    return;
  }

  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const years = clampYears(url.searchParams.get('years'));
  const requestedDay = normalizeDay(url.searchParams.get('day'));

  const pipeline = getPipeline();

  let day = requestedDay;
  if (day) {
    const exists = pipeline.getEdition(day, years, { applyCuration: false });
    if (!exists) day = '';
  }
  if (!day) {
    day = pipeline.getLatestDay() || requestedDay || formatDay();
  }

  const payload = pipeline.getEdition(day, years);
  if (!payload) {
    sendJson(res, { error: 'edition_not_found', day, years }, 404);
    return;
  }

  sendJson(res, payload);
}

