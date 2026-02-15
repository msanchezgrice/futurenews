// Vercel serverless function entry point
// Wraps the existing Node.js server request handler for Vercel's runtime.

import { requestHandler } from '../server/server.js';

export default async function handler(req, res) {
  return requestHandler(req, res);
}
