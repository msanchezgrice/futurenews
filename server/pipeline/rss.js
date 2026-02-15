function decodeHtml(value) {
  return String(value || '')
    .replace(/&lt;/gi, '<')
    .replace(/&gt;/gi, '>')
    .replace(/&amp;/gi, '&')
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/\s+/g, ' ')
    .trim();
}

function stripTags(value) {
  return String(value || '')
    .replace(/<[^>]*>/g, ' ')
    .replace(/<!\[CDATA\[|\]\]>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractTag(block, tagName) {
  const pattern = new RegExp(`<${tagName}[^>]*>([\\s\\S]*?)<\\/${tagName}>`, 'i');
  const match = String(block || '').match(pattern);
  if (!match) return '';
  // Decode HTML entities first, then strip tags, to handle feeds that embed escaped HTML like "&lt;p&gt;".
  return stripTags(decodeHtml(match[1]));
}

function extractAtomLink(entryBlock) {
  const block = String(entryBlock || '');
  const linkTag = block.match(/<link[^>]+>/i);
  if (!linkTag) return extractTag(block, 'link');
  const href = linkTag[0].match(/href=\"([^\"]+)\"/i);
  if (!href) return '';
  return decodeHtml(href[1]);
}

function parseDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const ts = Date.parse(raw);
  if (!Number.isFinite(ts)) return null;
  return new Date(ts).toISOString();
}

function parseRssItems(xml) {
  const items = [];
  const blocks = String(xml || '').match(/<item[\s\S]*?<\/item>/gi) || [];
  for (const block of blocks) {
    const title = extractTag(block, 'title');
    const description = extractTag(block, 'description') || extractTag(block, 'content:encoded');
    const link = extractTag(block, 'link');
    const guid = extractTag(block, 'guid');
    const pub = extractTag(block, 'pubDate') || extractTag(block, 'dc:date') || extractTag(block, 'updated');
    const publishedAt = parseDate(pub) || null;
    const canonical = link || guid || '';
    if (!title) continue;
    items.push({
      title,
      summary: description,
      link: canonical,
      publishedAt
    });
  }
  return items;
}

function parseAtomEntries(xml) {
  const entries = [];
  const blocks = String(xml || '').match(/<entry[\s\S]*?<\/entry>/gi) || [];
  for (const block of blocks) {
    const title = extractTag(block, 'title');
    const summary = extractTag(block, 'summary') || extractTag(block, 'content');
    const link = extractAtomLink(block);
    const pub = extractTag(block, 'published') || extractTag(block, 'updated');
    const publishedAt = parseDate(pub) || null;
    if (!title) continue;
    entries.push({
      title,
      summary,
      link,
      publishedAt
    });
  }
  return entries;
}

export function parseFeed(xml) {
  const text = String(xml || '');
  if (!text) return [];
  const isAtom = /<feed[\s>]/i.test(text) && /<entry[\s>]/i.test(text);
  const items = isAtom ? parseAtomEntries(text) : parseRssItems(text);
  return items
    .map((item) => ({
      title: String(item.title || '').trim(),
      summary: String(item.summary || '').trim(),
      link: String(item.link || '').trim(),
      publishedAt: item.publishedAt
    }))
    .filter((item) => item.title);
}
