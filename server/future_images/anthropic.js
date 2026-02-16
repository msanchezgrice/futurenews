import { safeParseJson } from './json.js';
import { getAnthropicApiKey } from './config.js';

function isModelNotFoundError(status, bodyText) {
  if (status !== 404) return false;
  const msg = String(bodyText || '');
  return msg.includes('not_found') || msg.includes('model') || msg.includes('Model') || msg.includes('model:');
}

export async function callAnthropicJson({
  modelCandidates,
  system,
  user,
  maxTokens = 4096,
  temperature = 0.4,
  timeoutMs = 70000,
  tool = null
}) {
  const apiKey = getAnthropicApiKey();
  if (!apiKey) {
    const err = new Error('ANTHROPIC_API_KEY not set');
    err.code = 'anthropic_missing_key';
    throw err;
  }

  const modelsRaw = Array.isArray(modelCandidates) ? modelCandidates.filter(Boolean) : [];
  const seen = new Set();
  const models = [];
  for (const m of modelsRaw) {
    const trimmed = String(m || '').trim();
    if (!trimmed) continue;
    if (seen.has(trimmed)) continue;
    seen.add(trimmed);
    models.push(trimmed);
  }
  if (!models.length) {
    const err = new Error('No model candidates provided');
    err.code = 'anthropic_no_model';
    throw err;
  }

  let lastErr = null;
  for (const model of models) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const body = {
        model,
        max_tokens: maxTokens,
        temperature,
        system: String(system || '').trim() || undefined,
        messages: [{ role: 'user', content: String(user || '') }]
      };

      const toolCfg = tool && typeof tool === 'object' ? tool : null;
      const toolName = toolCfg ? String(toolCfg.name || '').trim() : '';
      if (toolName) {
        body.tools = [
          {
            name: toolName,
            description: String(toolCfg.description || '').trim() || undefined,
            input_schema: toolCfg.inputSchema || toolCfg.input_schema || toolCfg.schema || { type: 'object' }
          }
        ];
        // Force a single tool call so we can read `tool_use.input` instead of parsing text JSON.
        body.tool_choice = { type: 'tool', name: toolName };
      }

      const resp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': apiKey,
          'anthropic-version': '2023-06-01'
        },
        body: JSON.stringify(body),
        signal: controller.signal
      });

      const bodyText = await resp.text().catch(() => '');
      if (!resp.ok) {
        if (isModelNotFoundError(resp.status, bodyText)) {
          continue;
        }
        const err = new Error(`Anthropic API ${resp.status}: ${bodyText.slice(0, 240)}`);
        err.status = resp.status;
        err.bodyText = bodyText;
        throw err;
      }

      let parsedResp;
      try {
        parsedResp = bodyText ? JSON.parse(bodyText) : null;
      } catch {
        parsedResp = null;
      }

      let text = '';
      let toolInput = null;
      if (parsedResp && Array.isArray(parsedResp.content)) {
        for (const block of parsedResp.content) {
          if (block && block.type === 'text' && block.text) text += String(block.text);
          if (toolName && block && block.type === 'tool_use' && block.name === toolName && block.input) {
            toolInput = block.input;
          }
        }
      }
      if (!text) text = bodyText || '';

      const parsed = toolInput || safeParseJson(text, null);
      if (!parsed) {
        const err = new Error(`Anthropic JSON parse failed. Preview: ${(text || '').slice(0, 300)}`);
        err.code = 'anthropic_parse_failed';
        throw err;
      }

      if (parsed && typeof parsed === 'object' && !parsed.model) {
        parsed.model = model;
      }

      return { ok: true, model, parsed, text, toolUsed: Boolean(toolInput) };
    } catch (err) {
      lastErr = err;
      // If it was an abort, try next model? Better to fail fast.
      if (err && String(err.name || '') === 'AbortError') {
        const e = new Error(`Anthropic request timed out after ${timeoutMs}ms (model=${model})`);
        e.code = 'anthropic_timeout';
        throw e;
      }
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  }

  throw lastErr || new Error('Anthropic model resolution failed');
}
