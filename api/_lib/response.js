export function sendJson(res, payload, status = 200) {
  try {
    res.statusCode = status;
    res.setHeader('content-type', 'application/json; charset=utf-8');
    res.setHeader('cache-control', 'no-store');
    res.end(JSON.stringify(payload));
  } catch {
    // ignore
  }
}

export function sendHtml(res, html, status = 200) {
  try {
    res.statusCode = status;
    res.setHeader('content-type', 'text/html; charset=utf-8');
    res.setHeader('cache-control', 'no-store');
    res.end(String(html || ''));
  } catch {
    // ignore
  }
}

