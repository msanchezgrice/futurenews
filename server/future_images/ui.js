export function renderImagesAdminHtml({ day, yearsForward = 5 } = {}) {
  const dayParam = encodeURIComponent(String(day || ''));
  const yearsParam = encodeURIComponent(String(yearsForward || 5));
  const stateUrl = `/api/admin/images/state?day=${dayParam}&years=${yearsParam}`;
  // Default to a conservative count so the LLM call reliably fits within serverless timeouts.
  const refreshIdeasUrl = `/api/admin/images/ideas/refresh?day=${dayParam}&years=${yearsParam}&count=30`;
  const refreshNewspaperUrl = `/api/admin/images/newspaper/refresh?day=${dayParam}&years=${yearsParam}`;
  const runWorkerUrl = `/api/admin/images/jobs/run?day=${dayParam}&years=${yearsParam}`;

  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin - Images - ${escapeHtml(String(day || ''))}</title>
  <style>
    :root{--bg:#ffffff;--fg:#111;--muted:#555;--border:#e5e5e5;--link:#0b4f8a;--mono: ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;--sans: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;}
    body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 var(--sans);}
    header{border-top:6px solid #111;border-bottom:1px solid #111;padding:16px 18px;}
    main{max-width:1240px;margin:0 auto;padding:18px;}
    h1{margin:0 0 6px;font-size:20px;letter-spacing:.02em;}
    h2{margin:0;font-size:18px;}
    h3{margin:18px 0 8px;font-size:13px;letter-spacing:.06em;text-transform:uppercase;color:#222;}
    a{color:var(--link);text-decoration:none;}
    a:hover{text-decoration:underline;}
    .row{display:flex;gap:10px;align-items:center;flex-wrap:wrap;}
    .card{border:1px solid var(--border);background:#fff;padding:12px;margin-top:14px;}
    .pill{display:inline-block;border:1px solid var(--border);padding:6px 10px;border-radius:999px;font-size:12px;color:inherit;background:#fbfbfb;}
    .badge{display:inline-block;font-family:var(--mono);font-size:12px;border:1px solid var(--border);background:#f3f3f3;padding:2px 8px;border-radius:999px;}
    .muted{color:var(--muted);}
    button{padding:8px 12px;border:1px solid var(--border);background:#111;color:#fff;border-radius:8px;cursor:pointer}
    button.secondary{background:#fff;color:#111}
    button:disabled{opacity:.6;cursor:not-allowed}
    .grid{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:10px}
    @media (max-width: 980px){.grid{grid-template-columns:repeat(2,1fr)}}
    @media (max-width: 620px){.grid{grid-template-columns:1fr}}
    .heroCard{border:1px solid var(--border);border-radius:10px;overflow:hidden;background:#fff}
    .heroImg{width:100%;height:140px;object-fit:cover;background:#f5f5f5;display:block}
    .heroMeta{padding:10px}
    .heroTitle{font-weight:700;margin:0 0 6px;font-size:13px}
    .heroDek{margin:0;color:#333;font-size:12px}
    .mono{font-family:var(--mono);font-size:12px}
    table{width:100%;border-collapse:collapse;margin-top:10px}
    th,td{border-top:1px solid var(--border);padding:8px 6px;text-align:left;vertical-align:top}
    th{color:var(--muted);font-size:11px;letter-spacing:.08em;text-transform:uppercase}
    .thumb{width:84px;height:48px;object-fit:cover;background:#f5f5f5;border:1px solid var(--border);border-radius:6px}
    .kicker{font-size:11px;color:var(--muted);margin-top:2px}
    .danger{color:#b42318}
    code{font-family:var(--mono);font-size:12px}
  </style>
</head>
<body>
  <header>
    <div class="row" style="justify-content:space-between">
      <div>
        <h1>Admin: Images</h1>
        <div class="muted">Day <strong id="dayLabel">${escapeHtml(String(day || ''))}</strong> • +${escapeHtml(String(yearsForward))}y</div>
      </div>
      <div class="row">
        <a class="pill" href="/api/admin/dashboard?day=${escapeHtml(dayParam)}&years=${escapeHtml(yearsParam)}">Dashboard</a>
        <a class="pill" href="/index.html?day=${escapeHtml(dayParam)}&years=${escapeHtml(yearsParam)}" target="_blank" rel="noopener">Front page</a>
      </div>
    </div>
    <div class="row" style="margin-top:10px">
      <button id="refreshIdeasBtn">Refresh ideas</button>
      <button id="refreshNewsBtn">Enqueue section heroes</button>
      <button class="secondary" id="runWorkerBtn">Run worker (x3)</button>
      <button class="secondary" id="runWorkerAllBtn">Run worker until empty</button>
      <span id="status" class="muted"></span>
    </div>
    <div class="row" style="margin-top:10px">
      <span class="badge" id="cfgFlags">flags: —</span>
      <span class="badge" id="cfgPg">postgres: —</span>
      <span class="badge" id="cfgBlob">blob: —</span>
      <span class="badge" id="cfgProvider">provider: —</span>
      <span class="badge" id="queueBadge">queue: —</span>
    </div>
  </header>

  <main>
    <section class="card">
      <h2>Newspaper: +5y section heroes</h2>
      <div class="muted">Rank-1 story per section (plus global hero if different). Images are pulled from Blob when ready.</div>
      <div id="heroGrid" class="grid"></div>
    </section>

    <section class="card">
      <h2>Ranked future objects (+5y)</h2>
      <div class="muted">Daily stack-ranked ideas. Top N auto-enqueued; the rest are on-demand.</div>
      <div class="row" style="margin-top:10px">
        <span class="muted mono" id="ideasMeta"></span>
      </div>
      <div style="overflow:auto">
        <table>
          <thead>
            <tr>
              <th>Image</th>
              <th>Rank</th>
              <th>Title</th>
              <th>Conf</th>
              <th>Status</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="ideasBody"></tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Failures (latest)</h2>
      <div class="muted">Most recent failed jobs for this day.</div>
      <div id="failures"></div>
    </section>
  </main>

  <script>
    const stateUrl = ${JSON.stringify(stateUrl)};
    const refreshIdeasUrl = ${JSON.stringify(refreshIdeasUrl)};
    const refreshNewspaperUrl = ${JSON.stringify(refreshNewspaperUrl)};
    const runWorkerUrl = ${JSON.stringify(runWorkerUrl)};
    const statusEl = document.getElementById('status');
    const heroGrid = document.getElementById('heroGrid');
    const ideasBody = document.getElementById('ideasBody');
    const failuresEl = document.getElementById('failures');
    const cfgFlags = document.getElementById('cfgFlags');
    const cfgPg = document.getElementById('cfgPg');
    const cfgBlob = document.getElementById('cfgBlob');
    const cfgProvider = document.getElementById('cfgProvider');
    const queueBadge = document.getElementById('queueBadge');
    const ideasMeta = document.getElementById('ideasMeta');

    function esc(s){
      return String(s||'')
        .replace(/&/g,'&amp;')
        .replace(/</g,'&lt;')
        .replace(/>/g,'&gt;')
        .replace(/\"/g,'&quot;')
        .replace(/'/g,'&#39;');
    }

    async function postJson(url, body) {
      const resp = await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined
      });
      const text = await resp.text();
      let json = null;
      try { json = text ? JSON.parse(text) : null; } catch { json = null; }
      if (!resp.ok) throw new Error((json && (json.error || json.detail)) || ('HTTP ' + resp.status + ': ' + text.slice(0,200)));
      return json;
    }

    function render(state) {
      const cfg = state && state.config ? state.config : {};
      const flags = cfg.flags || {};
      cfgFlags.textContent = 'flags: images=' + !!flags.imagesEnabled + ' ideas=' + !!flags.ideasEnabled + ' heroes=' + !!flags.storyHeroEnabled + ' topN=' + (flags.autoTopN ?? '—');
      cfgPg.textContent = 'postgres: ' + (cfg.postgresConfigured ? 'configured' : 'missing');
      cfgBlob.textContent = 'blob: ' + (cfg.blobConfigured ? 'configured' : 'missing');
      if (cfg.postgresInitError) cfgPg.textContent += ' (err)';
      const providers = cfg.providers || {};
      const nano = providers.nanoBanana || {};
      const gemini = providers.gemini || {};
      const openai = providers.openai || {};
      const providerName = providers.defaultProvider || '—';
      const providerOk = !!providers.imageProviderConfigured;
      cfgProvider.textContent = 'provider: ' + providerName + (providerOk ? '' : ' (missing keys)') +
        ' nano=' + !!nano.configured + ' gemini=' + !!gemini.configured + ' openai=' + !!openai.configured;

      const q = state && state.queue ? state.queue : {};
      queueBadge.textContent = 'queue: queued=' + (q.queued||0) + ' running=' + (q.running||0) + ' failed=' + (q.failed||0) + ' ok=' + (q.succeeded||0);

      const heroes = Array.isArray(state.sectionHeroes) ? state.sectionHeroes : [];
      heroGrid.innerHTML = heroes.map(h => {
        const img = h.assetUrl || '';
        const status = h.status || 'missing';
        const title = h.title || '(missing story)';
        const dek = h.dek || '';
        const btn = h.storyId ? '<button class=\"secondary\" data-action=\"hero\" data-story=\"' + esc(h.storyId) + '\" data-section=\"' + esc(h.section) + '\">Generate</button>' : '';
        const badgeClass = status === 'failed' ? 'badge danger' : 'badge';
        const badge = '<span class=\"' + badgeClass + '\">' + esc(status) + '</span>';
        return '<div class=\"heroCard\">' +
          (img ? '<img class=\"heroImg\" src=\"' + esc(img) + '\"/>' : '') +
          '<div class=\"heroMeta\">' +
            '<div class=\"row\" style=\"justify-content:space-between\">' +
              '<div class=\"mono\"><strong>' + esc(h.section) + '</strong></div>' +
              badge +
            '</div>' +
            '<div class=\"heroTitle\">' + esc(title) + '</div>' +
            '<div class=\"heroDek\">' + esc(dek) + '</div>' +
            (h.lastError ? '<div class=\"kicker danger\">' + esc(h.lastError) + '</div>' : '') +
            '<div class=\"row\" style=\"margin-top:8px\">' +
              btn +
              (h.storyId ? ('<a class=\"pill\" href=\"/article.html?id=' + esc(h.storyId) + '&years=5&day=' + esc(state.day) + '\" target=\"_blank\">Open story</a>') : '') +
            '</div>' +
          '</div>' +
        '</div>';
      }).join('');

      const ideas = Array.isArray(state.ideas) ? state.ideas : [];
      ideasMeta.textContent = ideas.length ? ('ideas: ' + ideas.length) : 'ideas: —';
      ideasBody.innerHTML = ideas.map(i => {
        const img = i.assetUrl || '';
        const status = i.status || 'missing';
        const actionBtn = '<button class=\"secondary\" data-action=\"idea\" data-idea=\"' + esc(i.ideaId) + '\">' + (img ? 'Regenerate' : 'Generate') + '</button>';
        return '<tr>' +
          '<td>' + (img ? '<img class=\"thumb\" src=\"' + esc(img) + '\"/>' : '<div class=\"thumb\"></div>') + '</td>' +
          '<td class=\"mono\">' + esc(i.rank) + '</td>' +
          '<td><div><strong>' + esc(i.title) + '</strong></div><div class=\"kicker\">' + esc(i.objectType || '') + '</div></td>' +
          '<td class=\"mono\">' + esc(i.confidence) + '</td>' +
          '<td>' + esc(status) + (i.lastError ? '<div class=\"kicker danger\">' + esc(i.lastError) + '</div>' : '') + '</td>' +
          '<td>' + actionBtn + '</td>' +
        '</tr>';
      }).join('');

      const failures = Array.isArray(state.failures) ? state.failures : [];
      failuresEl.innerHTML = failures.length ? ('<ul>' + failures.map(f => {
        return '<li class=\"mono\"><strong>' + esc(f.kind) + '</strong> ' +
          (f.storyId ? ('story=' + esc(f.storyId) + ' ') : '') +
          (f.ideaId ? ('idea=' + esc(f.ideaId) + ' ') : '') +
          'err=' + esc((f.error||'').slice(0,180)) + '</li>';
      }).join('') + '</ul>') : '<div class=\"muted\">No failures.</div>';
    }

    async function load() {
      const resp = await fetch(stateUrl).then(r => r.json());
      render(resp);
    }

    async function runWorker(limit, untilEmpty) {
      statusEl.textContent = 'Worker running...';
      let loops = 0;
      while (true) {
        loops++;
        const sep = runWorkerUrl.includes('?') ? '&' : '?';
        const resp = await postJson(runWorkerUrl + sep + 'limit=' + encodeURIComponent(String(limit||3)), null);
        await load();
        const processed = resp && resp.processed ? resp.processed : 0;
        statusEl.textContent = 'Worker processed ' + processed + ' (loop ' + loops + ')';
        if (!untilEmpty) break;
        if (!processed) break;
        if (loops >= 30) break;
      }
    }

    document.getElementById('refreshIdeasBtn').addEventListener('click', async () => {
      statusEl.textContent = 'Refreshing ideas...';
      try {
        await postJson(refreshIdeasUrl, null);
        await load();
        statusEl.textContent = 'Ideas refreshed.';
      } catch (e) {
        statusEl.textContent = String(e && e.message || e);
      }
    });
    document.getElementById('refreshNewsBtn').addEventListener('click', async () => {
      statusEl.textContent = 'Enqueueing section heroes...';
      try {
        await postJson(refreshNewspaperUrl, null);
        await load();
        statusEl.textContent = 'Section heroes enqueued.';
      } catch (e) {
        statusEl.textContent = String(e && e.message || e);
      }
    });
    document.getElementById('runWorkerBtn').addEventListener('click', async () => {
      try { await runWorker(3, false); } catch(e){ statusEl.textContent = String(e && e.message || e); }
    });
    document.getElementById('runWorkerAllBtn').addEventListener('click', async () => {
      try { await runWorker(3, true); } catch(e){ statusEl.textContent = String(e && e.message || e); }
    });

    document.addEventListener('click', async (e) => {
      const btn = e.target && e.target.closest ? e.target.closest('button[data-action]') : null;
      if (!btn) return;
      const action = btn.getAttribute('data-action');
      if (action === 'hero') {
        const storyId = btn.getAttribute('data-story');
        const section = btn.getAttribute('data-section');
        statusEl.textContent = 'Enqueueing hero...';
        try {
          await postJson('/api/admin/images/jobs/enqueue', { kind: 'story_section_hero', storyId, section, force: true });
          await load();
          statusEl.textContent = 'Hero job enqueued.';
        } catch (e2) {
          statusEl.textContent = String(e2 && e2.message || e2);
        }
      }
      if (action === 'idea') {
        const ideaId = btn.getAttribute('data-idea');
        statusEl.textContent = 'Enqueueing idea...';
        try {
          await postJson('/api/admin/images/jobs/enqueue', { kind: 'idea_image', ideaId, force: true });
          await load();
          statusEl.textContent = 'Idea job enqueued.';
        } catch (e3) {
          statusEl.textContent = String(e3 && e3.message || e3);
        }
      }
    });

    load().catch(err => { statusEl.textContent = String(err && err.message || err); });
    setInterval(() => { load().catch(()=>{}); }, 5000);
  </script>
</body>
</html>`;
}

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
