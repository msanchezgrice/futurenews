import { put } from '@vercel/blob';

export function putDefaults() {
  return {
    access: 'public',
    addRandomSuffix: false,
    allowOverwrite: true,
    cacheControlMaxAge: 60 * 60 * 24 * 365,
    token: String(process.env.BLOB_READ_WRITE_TOKEN || '').trim()
  };
}

export function blobConfigured() {
  return Boolean(putDefaults().token);
}

export async function putImageBlob(pathname, bytes, options = {}) {
  const defaults = putDefaults();
  if (!defaults.token) {
    const err = new Error('BLOB_READ_WRITE_TOKEN not configured');
    err.code = 'blob_not_configured';
    throw err;
  }
  const contentType = String(options.contentType || 'image/png').trim() || 'image/png';
  const result = await put(String(pathname || '').replace(/^\/+/, ''), bytes, {
    access: defaults.access,
    addRandomSuffix: defaults.addRandomSuffix,
    allowOverwrite: defaults.allowOverwrite,
    cacheControlMaxAge: defaults.cacheControlMaxAge,
    token: defaults.token,
    contentType
  });
  return result;
}

