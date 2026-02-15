(function () {
  'use strict';

  const MAX_YEARS = 10;
  const DEFAULT_YEARS = 5;
  const SECTION_ORDER = ['U.S.', 'World', 'Business', 'Technology', 'AI', 'Arts', 'Lifestyle', 'Opinion'];
  const SECTION_ALL = 'All';
  const EDITION_CACHE_KEY = 'future-times-edition-cache-v16';
  const ARTICLE_CACHE_KEY = 'future-times-article-cache-v16';
  const EDITION_TTL_MS = 1000 * 60 * 60;
  const ARTICLE_TTL_MS = 1000 * 60 * 20;
  const topLoadingBar = document.getElementById('topLoadingBar');
  const topLoadingBarFill = topLoadingBar ? topLoadingBar.querySelector('.top-loading-bar-fill') : null;

  const pageIsIndex = Boolean(document.getElementById('sectionPanels'));
  const pageIsArticle = Boolean(document.getElementById('md'));

  const state = {
    section: SECTION_ALL,
    day: '',
    editions: {},
    articles: {}
  };

  const cacheState = loadCacheState();
  if (cacheState && typeof cacheState === 'object') {
    if (cacheState.editions && typeof cacheState.editions === 'object') {
      state.editions = cacheState.editions;
    }
    if (cacheState.articles && typeof cacheState.articles === 'object') {
      state.articles = cacheState.articles;
    }
  }

  let ws = null;
  let wsReady = null;
  let socketRequestId = 0;
  const inflightArticles = new Map();
  const prefetchedArticles = new Set();
  let topLoadingTicker = null;
  let editionAutoRefreshTimer = null;

  const api = {
    edition: (years, day) => `/api/edition?years=${years}${day ? `&day=${encodeURIComponent(day)}` : ''}`,
    articleStatus: (id, years, day) => `/api/article/${encodeURIComponent(id)}?years=${years}${day ? `&day=${encodeURIComponent(day)}` : ''}`,
    articleRender: (id, years, day) => `/api/article/${encodeURIComponent(id)}?years=${years}${day ? `&day=${encodeURIComponent(day)}` : ''}`,
    ws: () => {
      const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${scheme}//${location.host}/ws`;
    }
  };

  init();

  function init() {
    const years = getYearsFromQuery();
    const section = getSectionFromQuery();
    const day = getDayFromQuery();
    setDaySignalLink(day);
    attachArticleLinkNavigation();
    wireEditionControls(years);
    if (topLoadingBar) {
      topLoadingBar.classList.remove('is-active');
    }
    if (pageIsIndex) {
      state.section = section;
      setupSectionNav();
      state.day = day;
      renderIndex(years, { pushHistory: false, section, day });
      startEditionAutoRefresh();
      window.addEventListener('popstate', () => {
        const poppedYears = getYearsFromQuery();
        const poppedSection = getSectionFromQuery();
        const poppedDay = getDayFromQuery();
        renderIndex(poppedYears, { pushHistory: false, section: poppedSection, day: poppedDay });
      });
    } else if (pageIsArticle) {
      const articleId = getArticleIdFromQuery();
      if (!articleId) {
        handleMissingArticleId();
        return;
      }

      // Ensure the URL is shareable/stable (day-specific) by resolving latest day once.
      if (!day) {
        getEditionPayload(years, '')
          .then((payload) => {
            const resolvedDay = normalizeDay(payload?.day || '');
            if (resolvedDay) {
              location.replace(getArticleUrl(articleId, years, resolvedDay));
            }
          })
          .catch(() => {});
      }

      setHomeLink(years, day);
      const cachedEdition = getCachedEdition(years, day);
      updateEditionTagFromCache(cachedEdition);
      renderArticle(articleId, years, day).catch(() => {});
      window.addEventListener('popstate', () => {
        const poppedYears = getYearsFromQuery();
        const poppedId = getArticleIdFromQuery();
        const poppedDay = getDayFromQuery();
        if (poppedId) {
          renderArticle(poppedId, poppedYears, poppedDay).catch(() => {});
          return;
        }
        const destination = getEditionUrl(poppedYears, poppedDay);
        if (location.pathname.endsWith('index.html') || location.pathname === '/') {
          const poppedSection = getSectionFromQuery();
          renderIndex(poppedYears, { pushHistory: false, section: poppedSection, day: poppedDay });
        } else {
          setHomeLink(poppedYears, poppedDay);
          const cachedEdition = getCachedEdition(poppedYears, poppedDay);
          updateEditionTagFromCache(cachedEdition);
        }
      });
    }
  }

  function startEditionAutoRefresh() {
    if (!pageIsIndex || editionAutoRefreshTimer) return;
    // Poll for newly-curated headlines while the page is open.
    editionAutoRefreshTimer = setInterval(() => {
      const years = clampYears(getYearsFromQuery());
      const section = getSectionFromQuery();
      const day = normalizeDay(getDayFromQuery()) || normalizeDay(state.day) || '';
      const cached = getCachedEdition(years, day);
      const cachedCurationAt = cached ? String(cached.curationGeneratedAt || '') : '';
      getEditionPayload(years, day)
        .then((payload) => {
          const freshCurationAt = payload ? String(payload.curationGeneratedAt || '') : '';
          if (!freshCurationAt || freshCurationAt === cachedCurationAt) {
            return;
          }
          const resolvedDay = normalizeDay(payload?.day || '') || day;
          state.day = resolvedDay || state.day;
          updateEditionHeader(payload);
          const heroId = renderHero(payload, years, section, state.day);
          const shownIds = renderSectionPanels(payload, years, section, state.day, heroId);
          renderSideRailTiles(payload, years, section, state.day, heroId, shownIds);
        })
        .catch(() => {});
    }, 45000);
  }

  function clampYears(raw) {
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) {
      return DEFAULT_YEARS;
    }
    return Math.max(0, Math.min(MAX_YEARS, Math.round(parsed)));
  }

  function getYearsFromQuery() {
    const params = new URLSearchParams(location.search);
    return clampYears(params.get('years'));
  }

  function normalizeDay(day) {
    const raw = String(day || '').trim();
    if (!raw) return '';
    return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : '';
  }

  function getDayFromQuery() {
    const params = new URLSearchParams(location.search);
    return normalizeDay(params.get('day'));
  }

  function normalizeSection(section) {
    if (!section) return SECTION_ALL;
    if (section === SECTION_ALL) return SECTION_ALL;
    const normalized = String(section).trim().toLowerCase();
    for (const option of SECTION_ORDER) {
      if (option.toLowerCase() === normalized) {
        return option;
      }
    }
    return SECTION_ALL;
  }

  function getSectionFromQuery() {
    const params = new URLSearchParams(location.search);
    return normalizeSection(params.get('section'));
  }

  function getArticleIdFromQuery() {
    const params = new URLSearchParams(location.search);
    return params.get('id') || '';
  }

  function getEditionUrl(years, day = '') {
    return getEditionUrlWithSection(years, getSectionFromQuery(), normalizeDay(day) || getDayFromQuery());
  }

  function getEditionUrlWithSection(years, section = SECTION_ALL, day = '') {
    const normalizedYears = clampYears(years);
    const params = new URLSearchParams({ years: String(normalizedYears) });
    const normalizedSection = normalizeSection(section);
    if (normalizedSection !== SECTION_ALL) {
      params.set('section', normalizedSection);
    }
    const normalizedDay = normalizeDay(day);
    if (normalizedDay) {
      params.set('day', normalizedDay);
    }
    return `index.html?${params.toString()}`;
  }

  function getArticleUrl(articleId, years, day = '') {
    const params = new URLSearchParams({ id: String(articleId || ''), years: String(years) });
    const normalizedDay = normalizeDay(day);
    if (normalizedDay) {
      params.set('day', normalizedDay);
    }
    return `article.html?${params.toString()}`;
  }

  function setEditionBadge(years) {
    const offsetYears = document.getElementById('offsetYears');
    if (!offsetYears) {
      return;
    }
    offsetYears.textContent = `+${years} years`;
  }

  function wireEditionControls(years) {
    const select = document.getElementById('editionYears');
    if (!select || select.dataset.futurenewsBound) {
      if (select) {
        select.value = String(years);
        setEditionBadge(years);
      }
      return;
    }
    select.dataset.futurenewsBound = '1';

    for (let i = 0; i <= MAX_YEARS; i++) {
      const option = document.createElement('option');
      option.value = String(i);
      option.textContent = `+${i} years`;
      select.appendChild(option);
    }

    select.value = String(years);
    setEditionBadge(years);

    select.addEventListener('change', () => {
      const newYears = clampYears(select.value);
      const section = getSectionFromQuery();
      const day = getDayFromQuery();
      startTopLoad(15);
      if (pageIsIndex) {
        const next = getEditionUrlWithSection(newYears, section, day);
        if (location.pathname.endsWith('/index.html') || location.pathname === '/' || location.pathname === '') {
          history.pushState({ page: 'index', years: newYears, section, day }, '', next);
        } else {
          location.href = next;
          return;
        }
        state.section = section;
        renderIndex(newYears, { pushHistory: false, section, day });
      } else {
        // Story slots are edition-specific; changing year should update the whole newspaper.
        location.href = getEditionUrlWithSection(newYears, section, day);
      }
    });
  }

  function wireSectionLink(link) {
    const section = link.dataset.section;
    if (!section) return;
    link.addEventListener('click', (event) => {
      event.preventDefault();
      const currentSection = getSectionFromQuery();
      const desiredSection = normalizeSection(section);
      const nextSection = currentSection === desiredSection ? SECTION_ALL : desiredSection;
      const years = getYearsFromQuery();
      const day = getDayFromQuery();

      startTopLoad(10);
      if (location.pathname.endsWith('/index.html') || location.pathname === '/' || location.pathname === '') {
        const next = getEditionUrlWithSection(years, nextSection, day);
        history.pushState({ page: 'index', years, section: nextSection, day }, '', next);
        renderIndex(years, { pushHistory: false, section: nextSection, day });
        return;
      }

      location.href = getEditionUrlWithSection(years, nextSection, day);
    });
  }

  function navSetActiveSection(section) {
    const nav = document.getElementById('sectionNav');
    if (!nav) return;
    const normalized = normalizeSection(section);
    nav.querySelectorAll('a').forEach((link) => {
      const navSection = normalizeSection(link.dataset.section);
      if ((normalized === SECTION_ALL && navSection === SECTION_ALL) || navSection === normalized) {
        link.classList.add('is-active');
      } else {
        link.classList.remove('is-active');
      }
    });
  }

  function setupSectionNav() {
    const nav = document.getElementById('sectionNav');
    if (!nav || nav.dataset.futurenewsBound) return;
    nav.dataset.futurenewsBound = '1';
    nav.querySelectorAll('a[data-section]').forEach((link) => {
      wireSectionLink(link);
    });
  }

  function attachArticleLinkNavigation() {
    document.addEventListener('click', (event) => {
      const anchor = event.target.closest('a[href]');
      if (!anchor) return;
      const anchorHref = anchor.getAttribute('href');
      if (!anchorHref) return;

      let target;
      try {
        target = new URL(anchorHref, location.href);
      } catch {
        return;
      }

      if (!(anchor.classList.contains('article-link') || target.pathname.endsWith('/article.html') || target.pathname.endsWith('article.html'))) {
        return;
      }

      if (
        event.defaultPrevented ||
        event.metaKey ||
        event.ctrlKey ||
        event.shiftKey ||
        event.altKey ||
        event.button !== 0
      ) {
        return;
      }
      if (target.origin !== location.origin) return;
      if (target.protocol === 'mailto:' || target.protocol === 'tel:') {
        return;
      }
      event.preventDefault();

      startTopLoad(12);
      if (pageIsArticle && target.pathname.endsWith('article.html')) {
        const nextYears = getYearsFromQuery();
        target.searchParams.set('years', String(nextYears));
        const nextDay = getDayFromQuery();
        if (nextDay) {
          target.searchParams.set('day', nextDay);
        }
      }
      location.href = target.toString();
    }, { capture: true });
  }

  function getSectionPanelId(section) {
    return `section-${section.toLowerCase().replace(/[^a-z0-9]+/gi, '-')}`;
  }

  function startTopLoad(initialPercent = 0) {
    if (!topLoadingBar || !topLoadingBarFill) return;
    if (topLoadingTicker) {
      clearInterval(topLoadingTicker);
      topLoadingTicker = null;
    }

    topLoadingBar.classList.add('is-active');
    topLoadingBarFill.style.width = `${Math.max(2, Math.min(98, Number(initialPercent) || 0))}%`;
    topLoadingTicker = setInterval(() => {
      const current = Number.parseFloat(topLoadingBarFill.style.width) || 0;
      const next = Math.min(98, current + 4 + Math.random() * 4);
      topLoadingBarFill.style.width = `${next}%`;
    }, 150);
  }

  function setTopLoadPercent(percent) {
    if (!topLoadingBar || !topLoadingBarFill) return;
    if (!topLoadingBar.classList.contains('is-active')) {
      topLoadingBar.classList.add('is-active');
    }
    const safePercent = Math.max(0, Math.min(100, Number(percent) || 0));
    topLoadingBarFill.style.width = `${safePercent}%`;
  }

  function finishTopLoad() {
    if (!topLoadingBar || !topLoadingBarFill) return;
    if (topLoadingTicker) {
      clearInterval(topLoadingTicker);
      topLoadingTicker = null;
    }
    topLoadingBarFill.style.width = '100%';
    window.setTimeout(() => {
      topLoadingBar.classList.remove('is-active');
      topLoadingBarFill.style.width = '0%';
    }, 220);
  }

  function failTopLoad() {
    if (!topLoadingBar || !topLoadingBarFill) return;
    if (topLoadingTicker) {
      clearInterval(topLoadingTicker);
      topLoadingTicker = null;
    }
    topLoadingBarFill.style.width = '100%';
    topLoadingBarFill.style.background = '#c53030';
    window.setTimeout(() => {
      topLoadingBar.classList.remove('is-active');
      topLoadingBarFill.style.width = '0%';
      topLoadingBarFill.style.background = 'linear-gradient(90deg, var(--accent-dark), var(--accent))';
    }, 220);
  }

  function loadCacheState() {
    try {
      const raw = window.localStorage.getItem(EDITION_CACHE_KEY) || '{}';
      const parsed = JSON.parse(raw);
      return parsed;
    } catch {
      return { editions: {}, articles: {} };
    }
  }

  function persistCacheState() {
    try {
      window.localStorage.setItem(
        EDITION_CACHE_KEY,
        JSON.stringify({
          schema: 1,
          editions: state.editions,
          articles: state.articles
        })
      );
    } catch {
      // ignore localStorage write errors in private mode / unavailable storage.
    }
  }

  function loadCacheKey(payloadType, key, ttlMs) {
    const store = payloadType === 'edition' ? state.editions : state.articles;
    const entry = store[key];
    if (!entry || !entry.ts || !entry.payload) return null;
    if (Date.now() - entry.ts > ttlMs) {
      delete store[key];
      persistCacheState();
      return null;
    }
    return entry.payload;
  }

  function setCacheKey(payloadType, key, payload, ttlMs) {
    const store = payloadType === 'edition' ? state.editions : state.articles;
    store[key] = {
      ts: Date.now(),
      payload,
      ttlMs
    };
    persistCacheState();
  }

  function getCachedEdition(years, day = '') {
    const key = `${normalizeDay(day) || 'latest'}|${String(years)}`;
    const payload = loadCacheKey('edition', key, EDITION_TTL_MS);
    if (!payload) return null;
    return payload;
  }

  function getCachedArticle(articleId, years, day = '') {
    const key = `${String(articleId)}|${normalizeDay(day) || 'latest'}|${String(years)}`;
    return loadCacheKey('article', key, ARTICLE_TTL_MS);
  }

  function setCachedArticle(articleId, years, day, payload) {
    const key = `${String(articleId)}|${normalizeDay(day) || 'latest'}|${String(years)}`;
    setCacheKey('article', key, payload, ARTICLE_TTL_MS);
  }

  async function fetchJSON(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) {
      const raw = await response.text().catch(() => '');
      const message = raw ? ` (${raw.slice(0, 140)})` : '';
      throw new Error(`Request failed ${response.status} ${response.statusText}${message}`);
    }
    return response.json();
  }

  async function getEditionPayload(years, day = '') {
    const cacheKey = `${normalizeDay(day) || 'latest'}|${String(years)}`;
    const cached = loadCacheKey('edition', cacheKey, EDITION_TTL_MS);
    try {
      const payload = await fetchJSON(api.edition(years, day));
      setCacheKey('edition', cacheKey, payload, EDITION_TTL_MS);
      return payload;
    } catch (err) {
      if (cached) return cached;
      throw err;
    }
  }

  function setDaySignalLink(day = '') {
    const link = document.getElementById('daySignalLink');
    if (!link) return;
    const normalized = normalizeDay(day) || normalizeDay(state.day) || normalizeDay(getDayFromQuery()) || '';
    link.href = normalized
      ? `/api/day-signal?day=${encodeURIComponent(normalized)}&format=html`
      : '/api/day-signal?format=html';
  }

  function updateEditionHeader(payload) {
    if (!payload) return;
    setText('editionDate', payload.date || '—');
    setText('generatedFrom', payload.generatedFrom || '—');
    // Show current Central Time
    const timeEl = document.getElementById('editionTime');
    if (timeEl) {
      try {
        const ct = new Date().toLocaleString('en-US', { timeZone: 'America/Chicago', hour: 'numeric', minute: '2-digit', hour12: true, timeZoneName: 'short' });
        timeEl.textContent = ct;
      } catch { timeEl.textContent = ''; }
    }
    const notes = document.getElementById('notes');
    if (notes) {
      const signalDay = normalizeDay(payload.day || '');
      notes.textContent = signalDay ? `Signals captured: ${signalDay}` : '';
    }
    setDaySignalLink(payload.day || state.day || getDayFromQuery());
  }

  function updateEditionTagFromCache(cachedEdition) {
    const tag = document.getElementById('editionTag');
    if (!tag) return;
    const edition = cachedEdition || getCachedEdition(getYearsFromQuery(), getDayFromQuery());
    const yearOffset = getYearsFromQuery();
    if (!edition) {
      tag.textContent = `Edition • ${getDateFromYears(yearOffset)}`;
      return;
    }
    tag.textContent = `Edition • ${edition.date || getDateFromYears(yearOffset)}`;
  }

  function setText(id, text) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = text;
  }

  function renderIndex(years, options = {}) {
    const section = normalizeSection(options.section || state.section || SECTION_ALL);
    const day = normalizeDay(options.day) || getDayFromQuery() || state.day || '';
    const clamped = clampYears(years);
    startTopLoad(8);
    if (options.pushHistory) {
      const destination = getEditionUrlWithSection(clamped, section, day);
      if (location.pathname === '/' || location.pathname.endsWith('/index.html')) {
        history.pushState({ page: 'index', years: clamped, section, day }, '', destination);
      }
    }

    state.section = section;
    state.day = day;
    setEditionBadge(clamped);
    wireEditionControls(clamped);
    setupSectionNav();
    navSetActiveSection(section);

    getEditionPayload(clamped, day)
      .then((payload) => {
        setEditionBadge(clamped);
        const resolvedDay = normalizeDay(payload?.day || '') || day;
        if (resolvedDay && resolvedDay !== day) {
          const nextUrl = getEditionUrlWithSection(clamped, section, resolvedDay);
          try {
            history.replaceState({ page: 'index', years: clamped, section, day: resolvedDay }, '', nextUrl);
          } catch {
            // ignore
          }
          state.day = resolvedDay;
        } else {
          state.day = resolvedDay || state.day;
        }
        updateEditionHeader(payload);
        const heroId = renderHero(payload, clamped, section, state.day);
        const shownIds = renderSectionPanels(payload, clamped, section, state.day, heroId);
        renderSideRailTiles(payload, clamped, section, state.day, heroId, shownIds);
        finishTopLoad();
      })
      .catch(() => {
        const fallbackDate = `2031`;
        setText('editionDate', fallbackDate);
        setText('generatedFrom', '');
        renderSideRailTiles(null, clamped, section, state.day, null, new Set());
        failTopLoad();
      });
  }

  function renderHero(payload, years, section = SECTION_ALL, day = '') {
    const hero = document.getElementById('hero');
    if (!hero) return;

    const heroLink = document.getElementById('heroLink');
    const heroImage = hero.querySelector('img');
    const sectionSpan = hero.querySelector('.badge strong');
    const headline = hero.querySelector('.headline');
    const dek = hero.querySelector('.dek');
    const meta = hero.querySelector('.meta');

    if (!payload || !payload.articles || !payload.articles.length) return;

    const normalizedSection = normalizeSection(section);
    const articlesInSection =
      normalizedSection === SECTION_ALL
        ? payload.articles
        : payload.articles.filter((article) => (article.section || SECTION_ORDER[0]) === normalizedSection);

    const heroArticle =
      (normalizedSection !== SECTION_ALL
        ? articlesInSection[0]
        : payload.articles.find((article) => article.id === (payload.heroId || payload.articles[0].id))) ||
      payload.articles[0] ||
      null;

    if (!heroArticle) return;

    if (heroLink) {
      heroLink.href = getArticleUrl(heroArticle.id, years, day);
      heroLink.classList.add('reveal');
      heroLink.style.setProperty('--delay', '0ms');
      if (sectionSpan) {
        sectionSpan.textContent = heroArticle.section || 'Future';
      }
      setTextElement(headline, heroArticle.title);
      setTextElement(dek, heroArticle.dek);
      setTextElement(meta, heroArticle.meta);
      setText('aSection', heroArticle.section || 'Future');
    }

    if (heroImage) {
      heroImage.src = heroArticle.image || heroImage.src;
      heroImage.alt = heroArticle.title || 'Lead story';
    }

    return heroArticle.id;
  }

  function setTextElement(el, text) {
    if (!el) return;
    el.textContent = text || '—';
  }

  function renderSectionPanels(payload, years, section = SECTION_ALL, day = '', heroId = null) {
    const shownIds = new Set();
    const container = document.getElementById('sectionPanels');
    if (!container) return shownIds;
    container.innerHTML = '';

    const selectedSection = normalizeSection(section);
    const articles = Array.isArray(payload.articles) ? payload.articles : [];
    let resolvedHeroId = heroId || null;
    if (!resolvedHeroId) {
      if (selectedSection === SECTION_ALL) {
        resolvedHeroId = payload.heroId || (articles[0] ? articles[0].id : null);
      } else {
        resolvedHeroId = (articles.find((article) => (article.section || SECTION_ORDER[0]) === selectedSection) || {}).id || null;
      }
    }
    const nonHero = articles.filter((article) => article.id !== resolvedHeroId);

    const grouped = new Map();
    for (const article of nonHero) {
      const section = article.section || 'U.S.';
      if (!grouped.has(section)) grouped.set(section, []);
      grouped.get(section).push(article);
    }

    const sectionsToRender = selectedSection === SECTION_ALL ? SECTION_ORDER : [selectedSection];
    const perSectionLimit = selectedSection === SECTION_ALL ? 3 : 50;

    for (const sectionName of sectionsToRender) {
      const sectionArticles = (grouped.get(sectionName) || []).slice(0, perSectionLimit);
      if (!sectionArticles.length) {
        continue;
      }
      const card = document.createElement('section');
      const panelId = getSectionPanelId(sectionName);
      card.id = panelId;
      card.className = 'card panel-section-card';
      card.dataset.section = sectionName;

      const title = document.createElement('h3');
      title.className = 'section-panel-title' + (sectionName === 'AI' ? ' ai-section' : '');
      title.textContent = sectionName;
      card.appendChild(title);

      const list = document.createElement('div');
      list.className = 'panel-list';
      sectionArticles.forEach((article) => {
        const item = document.createElement('a');
        item.className = 'panel-item article-link reveal';
        item.href = getArticleUrl(article.id, years, day);
        item.dataset.articleId = article.id;
        item.dataset.section = sectionName;
        item.style.setProperty('--delay', `${Math.min(420, shownIds.size * 28)}ms`);
        shownIds.add(article.id);

        const badge = document.createElement('div');
        badge.className = 'badge';

        const sparkIcon = document.createElement('span');
        sparkIcon.className = 'spark';
        sparkIcon.textContent = (article.section === 'AI') ? '◆' : '✦';
        if (article.section === 'AI') sparkIcon.style.color = '#6366f1';

        const badgeSection = document.createElement('strong');
        badgeSection.textContent = article.section || sectionName;
        if (article.section === 'AI') badgeSection.style.color = '#4f46e5';

        const badgeEdition = document.createElement('span');
        badgeEdition.style.color = 'var(--muted)';
        badgeEdition.textContent = 'Edition';

        badge.appendChild(sparkIcon);
        badge.appendChild(badgeSection);
        badge.appendChild(badgeEdition);

        const titleEl = document.createElement('h3');
        titleEl.textContent = article.title || 'Untitled';

        const dek = document.createElement('p');
        dek.textContent = article.dek || '';

        const meta = document.createElement('div');
        meta.className = 'small';
        meta.textContent = article.meta || '';

        item.appendChild(badge);
        item.appendChild(titleEl);
        item.appendChild(dek);
        item.appendChild(meta);
        list.appendChild(item);

        item.addEventListener('pointerenter', () => {
          prefetchArticle(article.id, years, day);
        });
        item.addEventListener('focus', () => {
          prefetchArticle(article.id, years, day);
        });
      });
      card.appendChild(list);
      container.appendChild(card);
    }

    const visiblePanels = container.querySelectorAll('.panel-section-card');
    if (!visiblePanels.length) {
      const msg = document.createElement('div');
      msg.className = 'small';
      if (selectedSection === SECTION_ALL) {
        msg.textContent = 'No sections are available for this edition.';
      } else {
        msg.textContent = `No stories available for ${selectedSection} in this edition.`;
      }
      container.appendChild(msg);
    }

    return shownIds;
  }

  function renderSideRailTiles(payload, years, section = SECTION_ALL, day = '', heroId = null, shownIds = new Set()) {
    const container = document.getElementById('sideRailTiles');
    if (!container) return;
    container.innerHTML = '';

    const articles = payload && Array.isArray(payload.articles) ? payload.articles : [];
    if (!articles.length) {
      container.innerHTML = '<div class="small">No stories available.</div>';
      return;
    }

    const normalizedSection = normalizeSection(section);
    const resolvedHeroId = heroId || payload.heroId || (articles[0] ? articles[0].id : null);
    const candidates = articles.filter((article) => article.id !== resolvedHeroId && !shownIds.has(article.id));
    if (!candidates.length) {
      container.innerHTML = '<div class="small">No more stories in this edition.</div>';
      return;
    }

    const maxTiles = normalizedSection === SECTION_ALL ? 16 : 18;
    candidates.slice(0, maxTiles).forEach((article, idx) => {
      const tile = document.createElement('a');
      tile.className = 'rail-tile article-link reveal';
      tile.href = getArticleUrl(article.id, years, day);
      tile.dataset.articleId = article.id;
      tile.dataset.section = article.section || '';
      tile.style.setProperty('--delay', `${idx * 28}ms`);

      const kicker = document.createElement('div');
      kicker.className = 'rail-kicker';
      kicker.textContent = article.section || 'Future';

      const title = document.createElement('div');
      title.className = 'rail-title';
      title.textContent = article.title || 'Untitled';

      tile.appendChild(kicker);
      tile.appendChild(title);

      if (article.dek) {
        const dek = document.createElement('div');
        dek.className = 'rail-dek';
        dek.textContent = String(article.dek || '').slice(0, 140);
        tile.appendChild(dek);
      }

      tile.addEventListener('pointerenter', () => {
        prefetchArticle(article.id, years, day);
      });
      tile.addEventListener('focus', () => {
        prefetchArticle(article.id, years, day);
      });

      container.appendChild(tile);
    });
  }

  async function prefetchArticle(articleId, years, day = '') {
    const cacheKey = `${String(articleId)}|${normalizeDay(day) || 'latest'}|${String(years)}`;
    if (prefetchedArticles.has(cacheKey) || getCachedArticle(articleId, years, day)) {
      return;
    }
    prefetchedArticles.add(cacheKey);
    try {
      const status = await fetchJSON(api.articleStatus(articleId, years, day));
      if (status.status === 'ready') {
        setCachedArticle(articleId, years, day, status.article);
      } else {
        const started = await fetchJSON(api.articleRender(articleId, years, day), { method: 'POST' });
        if (started.status === 'ready' && started.article) {
          setCachedArticle(articleId, years, day, started.article);
        }
      }
    } catch {
      // Prefetch is opportunistic only.
    }
  }

  async function renderArticle(articleId, years, day = '') {
    const clampedYears = clampYears(years);
    if (!articleId) return;

    const normalizedDay = normalizeDay(day) || getDayFromQuery() || state.day || '';
    const pageKey = `${articleId}|${normalizedDay || 'latest'}|${clampedYears}`;
    if (inflightArticles.has(pageKey)) {
      return inflightArticles.get(pageKey);
    }

    setEditionBadge(clampedYears);
    setHomeLink(clampedYears, normalizedDay);
    startTopLoad(8);

    const promise = (async () => {
      const status = document.getElementById('renderStatusText');
      const progressText = document.getElementById('renderProgressText');
      const progressBar = document.getElementById('renderProgress');
      const renderCard = document.getElementById('renderStatus');

      const setProgress = (phase, percent) => {
        if (status) {
          status.textContent = phase || status.textContent;
        }
        if (progressText) {
          progressText.textContent = percent ? `${Math.round(percent)}%` : '';
        }
        if (progressBar) {
          const percentValue = Number.isFinite(percent) ? clampPercent(percent) : 0;
          progressBar.style.width = `${percentValue}%`;
          setTopLoadPercent(percentValue);
        }
      };

      if (renderCard) {
        renderCard.style.display = 'block';
      }
      setProgress('Checking live render cache…', 8);

      let editionPayload = getCachedEdition(clampedYears, normalizedDay);
      if (!editionPayload) {
        try {
          editionPayload = await getEditionPayload(clampedYears, normalizedDay);
        } catch {
          // edition header remains best effort.
        }
      }
      if (editionPayload) {
        updateEditionHeader(editionPayload);
        setEditionTagFromPayload(editionPayload);
        if (!normalizedDay) {
          const resolved = normalizeDay(editionPayload.day || '');
          if (resolved) {
            try {
              history.replaceState(history.state, '', getArticleUrl(articleId, clampedYears, resolved));
            } catch {
              // ignore
            }
            day = resolved;
          }
        }
      }

      const articleSeed = (editionPayload && Array.isArray(editionPayload.articles))
        ? editionPayload.articles.find((item) => item.id === articleId)
        : null;

      const cached = getCachedArticle(articleId, clampedYears, normalizeDay(day) || normalizedDay);
      if (cached) {
        const seedCurationAt = articleSeed && articleSeed.curation ? String(articleSeed.curation.generatedAt || '') : '';
        const cachedCurationAt = String(cached.curationGeneratedAt || (cached.curation && cached.curation.generatedAt) || '');
        const cacheMatches = !seedCurationAt || (seedCurationAt && seedCurationAt === cachedCurationAt);
        if (cacheMatches) {
          renderArticlePayload(cached, { cached: true });
          if (renderCard) {
            renderCard.style.display = 'none';
          }
          setProgress('Loaded from cache', 100);
          finishTopLoad();
          return cached;
        }
      }
      if (articleSeed) {
        renderArticlePayload(articleSeedToArticle(articleSeed, clampedYears), { cached: false, partial: true });
      } else {
        setText('aTitle', 'Loading article…');
      }

      const statusPayload = await fetchJSON(api.articleStatus(articleId, clampedYears, normalizeDay(day) || normalizedDay));
      if (statusPayload.status === 'ready' && statusPayload.article) {
        setCachedArticle(articleId, clampedYears, normalizeDay(day) || normalizedDay, statusPayload.article);
        renderArticlePayload(statusPayload.article, { cached: false });
        if (renderCard) {
          renderCard.style.display = 'none';
        }
        setProgress('Rendered from cache', 100);
        finishTopLoad();
        return statusPayload.article;
      }

      await fetchJSON(api.articleRender(articleId, clampedYears, normalizeDay(day) || normalizedDay), { method: 'POST' });
      setProgress('Connecting to live renderer…', 12);

      let renderedArticle;
      try {
        renderedArticle = await renderArticleOverSocket(articleId, clampedYears, articleSeed, setProgress);
      } catch (error) {
        const fallbackArticle = await pollRenderedArticle(articleId, clampedYears, normalizeDay(day) || normalizedDay, setProgress);
        renderedArticle = fallbackArticle;
      }
      setCachedArticle(articleId, clampedYears, normalizeDay(day) || normalizedDay, renderedArticle);
      renderArticlePayload(renderedArticle, { cached: false });
      if (renderCard) {
        renderCard.style.display = 'none';
      }
      setProgress('Render complete', 100);
      finishTopLoad();
      return renderedArticle;
    })();

    inflightArticles.set(pageKey, promise);
    let article;
    try {
      article = await promise;
      inflightArticles.delete(pageKey);
      return article;
    } catch (error) {
      inflightArticles.delete(pageKey);
      failTopLoad();
      const statusText = document.getElementById('renderStatusText');
      const progressText = document.getElementById('renderProgressText');
      const progress = document.getElementById('renderProgress');
      const section = document.getElementById('aSection');
      const title = document.getElementById('aTitle');
      const dek = document.getElementById('aDek');
      const meta = document.getElementById('aMeta');
      const body = document.getElementById('md');
      const prompt = document.getElementById('prompt');
      const hero = document.getElementById('aHero');
      const status = `${error.message || 'unknown error'}`;

      if (statusText) {
        statusText.textContent = `Render failed: ${status}`;
      }
      if (progressText) {
        progressText.textContent = 'Using cached fallback if available.';
      }
      if (progress) {
        progress.style.width = '0%';
      }
      if (title) title.textContent = 'Failed to load article';
      if (section) section.textContent = 'Article';
      if (dek) dek.textContent = status;
      if (meta) meta.textContent = `Edition: ${clampYears(years)} years`;
      if (hero) hero.src = 'assets/img/humanoids-labor-market.svg';
      if (prompt) prompt.textContent = 'Unavailable until render backend responds.';
      if (body) {
        body.innerHTML = '<p>The selected article could not be loaded right now. Please try another year value or return to the front page.</p>';
      }
      throw error;
    }
  }

  async function pollRenderedArticle(articleId, years, day, setProgress) {
    const maxAttempts = 30;
    const pollingMs = 450;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const status = await fetchJSON(api.articleStatus(articleId, years, day));
      const percent = Math.min(95, 10 + attempt * 2.7);
      if (status.status === 'ready' && status.article) {
        setProgress('Render finished', 100);
        return status.article;
      }
      setProgress('Render queued', percent);
      await delay(pollingMs);
    }
    throw new Error('Render polling timed out');
  }

  function articleSeedToArticle(seed, years) {
    const fallbackMeta = [seed.section, getDateFromYears(years)].filter(Boolean).join(' • ');
    return {
      id: seed.id,
      section: seed.section,
      title: seed.title,
      dek: seed.dek,
      meta: String(seed.meta || seed.baseMeta || fallbackMeta || ''),
      image: seed.image,
      body: '',
      signals: seed.signals || [],
      markets: seed.markets || [],
      prompt: seed.prompt || '',
      editionDate: getDateFromYears(years),
      generatedAt: new Date().toISOString(),
      yearsForward: years
    };
  }

  function getDateFromYears(years) {
    const base = new Date();
    base.setFullYear(base.getFullYear() + Number(years || 0));
    try {
      return new Intl.DateTimeFormat('en-US', { month: 'long', day: 'numeric', year: 'numeric' }).format(base);
    } catch {
      return `${base.getFullYear()}`;
    }
  }

  async function renderArticleOverSocket(articleId, years, articleSeed, onProgress) {
    const socket = await getSocket();
    const requestId = `req-${Date.now()}-${++socketRequestId}`;
    const streamState = articleSeed ? articleSeedToArticle(articleSeed, years) : { id: articleId, body: '' };
    let resolved = false;
    let rejectTimer;

    const livePayload = await new Promise((resolve, reject) => {
      const handleMessage = (event) => {
        let payload;
        try {
          payload = JSON.parse(event.data);
        } catch {
          return;
        }

        if (payload.requestId && payload.requestId !== requestId) {
          return;
        }

        switch (payload.type) {
          case 'render.progress': {
            onProgress(payload.phase || 'Rendering…', payload.percent || 0);
            break;
          }
          case 'render.chunk': {
            streamState.body = `${streamState.body || ''}${String(payload.delta || '')}`;
            renderArticlePayload(streamState, { cached: false, partial: true });
            const current = Number.parseFloat(topLoadingBarFill?.style.width || '0') || 0;
            const next = Math.min(96, current + 1.5);
            setTopLoadPercent(next);
            break;
          }
          case 'render.complete':
          case 'render.article': {
            const article = payload.article || streamState;
            const finalArticle = { ...streamState, ...article };
            finalArticle.body = finalArticle.body || streamState.body || '';
            resolved = true;
            cleanup();
            resolve(finalArticle);
            break;
          }
          case 'render.error': {
            resolved = true;
            cleanup();
            reject(new Error(payload.error || 'Failed to render article'));
            break;
          }
          default:
            break;
        }
      };

      const cleanup = () => {
        socket.removeEventListener('message', handleMessage);
        if (rejectTimer) clearTimeout(rejectTimer);
      };

      socket.addEventListener('message', handleMessage);
      socket.send(
        JSON.stringify({
          type: 'render.article',
          requestId,
          articleId,
          years,
          day: getDayFromQuery()
        })
      );

      rejectTimer = setTimeout(() => {
        if (resolved) return;
        cleanup();
        reject(new Error('Article render timed out'));
      }, 45000);
    });

    return livePayload;
  }

  function getSocket() {
    if (ws && ws.readyState === WebSocket.OPEN) {
      return Promise.resolve(ws);
    }
    if (ws && ws.readyState === WebSocket.CONNECTING && wsReady) {
      return wsReady;
    }

    ws = new WebSocket(api.ws());
    wsReady = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error('WebSocket open timed out'));
      }, 7000);

      ws.addEventListener(
        'open',
        () => {
          clearTimeout(timer);
          resolve(ws);
        },
        { once: true }
      );
      ws.addEventListener(
        'error',
        (event) => {
          clearTimeout(timer);
          reject(new Error(`WebSocket failed`));
        },
        { once: true }
      );
      ws.addEventListener(
        'close',
        () => {
          clearTimeout(timer);
          ws = null;
        }
      );
    });
    return wsReady;
  }

  function setEditionTagFromPayload(payload) {
    const tag = document.getElementById('editionTag');
    if (!tag) return;
    if (!payload) return;
    const date = payload.date || 'future edition';
    tag.textContent = `Edition • ${date}`;
  }

  function renderArticlePayload(article, options = { cached: false, partial: false }) {
    if (!article) return;
    const section = document.getElementById('aSection');
    const title = document.getElementById('aTitle');
    const dek = document.getElementById('aDek');
    const meta = document.getElementById('aMeta');
    const hero = document.getElementById('aHero');
    const prompt = document.getElementById('prompt');
    const md = document.getElementById('md');
    const status = document.getElementById('renderStatusText');
    const progressText = document.getElementById('renderProgressText');
    const progress = document.getElementById('renderProgress');

    setTextElement(section, article.section || '—');
    setTextElement(title, article.title || '—');
    setTextElement(dek, article.dek || '—');
    setTextElement(meta, article.meta || '—');

    if (hero) {
      hero.src = article.image || 'assets/img/humanoids-labor-market.svg';
      hero.alt = article.title || 'Article hero';
    }

    setTextElement(prompt, article.prompt || '—');

    if (options.partial) {
      if (status) status.textContent = 'Streaming live content…';
      if (progressText) progressText.textContent = 'Streaming';
      if (progress) progress.style.width = '40%';
    } else if (options.cached) {
      if (status) status.textContent = 'Loaded from cache';
      if (progressText) progressText.textContent = 'No stream required.';
      if (progress) progress.style.width = '100%';
    } else {
      if (status) status.textContent = 'Rendering complete';
      if (progressText) progressText.textContent = '';
      if (progress) progress.style.width = '100%';
    }

    renderSignalRows('signals', article.signals || []);
    renderMarketRows('marketSnap', article.markets || []);
    if (md) {
      md.innerHTML = markdownToHtml(article.body || '');
    }

    const statusBlock = document.getElementById('renderStatus');
    if (statusBlock && !options.partial) {
      statusBlock.style.display = options.cached ? 'none' : statusBlock.style.display;
      if (statusBlock.style.display === '') {
        statusBlock.style.display = 'block';
      }
    }
  }

  function renderSignalRows(containerId, entries) {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = '';
    entries.forEach((entry) => {
      const row = document.createElement('div');
      row.className = 'kv-row';

      const label = document.createElement('span');
      label.textContent = entry.label || '';
      const value = document.createElement('strong');
      value.textContent = entry.value || '';

      row.appendChild(label);
      row.appendChild(value);
      container.appendChild(row);
    });
    if (!entries.length) {
      const row = document.createElement('div');
      row.className = 'kv-row';
      row.textContent = 'No signals available.';
      container.appendChild(row);
    }
  }

  function renderMarketRows(containerId, entries) {
    const container = document.getElementById(containerId);
    if (!container) return;
    container.innerHTML = '';
    entries.forEach((entry) => {
      const row = document.createElement('div');
      row.className = 'market-row';

      const label = document.createElement('span');
      label.className = 'label';
      label.textContent = entry.label || '';
      const prob = document.createElement('strong');
      prob.className = 'prob';
      prob.textContent = entry.prob || '';

      row.appendChild(label);
      row.appendChild(prob);
      container.appendChild(row);
    });

    if (!entries.length) {
      const row = document.createElement('div');
      row.className = 'market-row';
      row.textContent = 'No market data available.';
      container.appendChild(row);
    }
  }

  function markdownToHtml(raw) {
    const text = String(raw || '').replace(/\r/g, '');
    const lines = text.split('\n');
    const output = [];
    let inList = false;

    for (const rawLine of lines) {
      const line = rawLine.trimEnd();

      if (/^###\s+/.test(line)) {
        if (inList) {
          output.push('</ul>');
          inList = false;
        }
        output.push(`<h3>${escapeHtml(line.replace(/^###\s+/, ''))}</h3>`);
        continue;
      }
      if (/^##\s+/.test(line)) {
        if (inList) {
          output.push('</ul>');
          inList = false;
        }
        output.push(`<h2>${escapeHtml(line.replace(/^##\s+/, ''))}</h2>`);
        continue;
      }
      if (/^#\s+/.test(line)) {
        if (inList) {
          output.push('</ul>');
          inList = false;
        }
        output.push(`<h1>${escapeHtml(line.replace(/^#\s+/, ''))}</h1>`);
        continue;
      }
      if (/^>\s+/.test(line)) {
        if (inList) {
          output.push('</ul>');
          inList = false;
        }
        output.push(`<blockquote>${escapeHtml(line.replace(/^>\s*/, ''))}</blockquote>`);
        continue;
      }
      if (/^-\s+/.test(line)) {
        if (!inList) {
          inList = true;
          output.push('<ul>');
        }
        const itemText = line.replace(/^-\s+/, '');
        output.push(`<li>${linkify(inlineMarkdown(escapeHtml(itemText)))}</li>`);
        continue;
      }
      if (!line) {
        if (inList) {
          output.push('</ul>');
          inList = false;
        }
        output.push('');
        continue;
      }

      if (inList) {
        output.push('</ul>');
        inList = false;
      }
      output.push(`<p>${linkify(inlineMarkdown(escapeHtml(line)))}</p>`);
    }

    if (inList) {
      output.push('</ul>');
    }

    return output.join('\n');
  }

  function inlineMarkdown(text) {
    return String(text)
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.+?)\*/g, '<em>$1</em>');
  }

  function linkify(htmlText) {
    const text = String(htmlText || '');
    if (!text) return '';
    const urlRe = /(https?:\/\/[^\s<]+)/g;
    return text.replace(urlRe, (match) => {
      let url = match;
      let trailing = '';
      while (url.length > 0 && /[)\].,;:!?]$/.test(url)) {
        trailing = `${url.slice(-1)}${trailing}`;
        url = url.slice(0, -1);
      }
      if (!url) return match;
      return `<a href="${url}" target="_blank" rel="noopener">${url}</a>${trailing}`;
    });
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function setHomeLink(years, day = '') {
    const href = getEditionUrl(clampYears(years), day);
    const homeLink = document.getElementById('homeLink');
    if (homeLink) {
      homeLink.href = href;
    }
    const front = document.getElementById('frontPageLink');
    if (front) {
      front.href = href;
    }
  }

  function handleMissingArticleId() {
    const title = document.getElementById('aTitle');
    const md = document.getElementById('md');
    setTextElement(title, 'No article selected');
    if (md) md.textContent = 'Please return to the front page and open an article.';
  }

  function clampPercent(value) {
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return 0;
    return Math.max(0, Math.min(100, Math.round(parsed)));
  }

  function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
})();
