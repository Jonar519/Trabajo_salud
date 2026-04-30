let offset = 0;
    let total = 0;
    let columns = [];
    let mapData = null;
    let mapGeoRows = [];
    let mapGeoByDeptKey = {};
    let mapSelectedDeptKey = null;
    let mapSelectedDeptName = null;
    let homeChatMessages = [];

    function qs(id) { return document.getElementById(id); }

    function setLayoutVars() {
      const root = document.documentElement;
      const header = document.querySelector('.header');
      const nav = document.getElementById('nav');
      const h = header ? header.getBoundingClientRect().height : 0;
      const n = nav ? nav.getBoundingClientRect().height : 0;
      root.style.setProperty('--headerH', Math.round(h) + 'px');
      root.style.setProperty('--navH', Math.round(n) + 'px');
    }

    function toast(msg) {
      const el = qs('toast');
      el.textContent = msg;
      el.style.display = 'block';
      setTimeout(function() { el.style.display = 'none'; }, 4200);
    }

    function setStatus(text, loading) {
      qs('statusText').textContent = text;
      qs('spin').style.display = loading ? 'inline-block' : 'none';
    }

    async function fetchJSON(url) {
      const res = await fetch(url);
      const txt = await res.text();
      try { return JSON.parse(txt); } catch (e) { throw new Error(txt.slice(0, 250)); }
    }

    function setActiveNav(route) {
      const items = [
        { id: 'navHome', route: '/inicio' },
        { id: 'navDiseases', route: '/enfermedades' },
        { id: 'navDash', route: '/dashboard' },
        { id: 'navDashBI', route: '/dashboardbi' },
      ];
      items.forEach(x => {
        const el = qs(x.id);
        if (!el) return;
        if (route === x.route) el.classList.add('active');
        else el.classList.remove('active');
      });
    }

    function showPage(route) {
      const r = route || '/inicio';
      const map = {
        '/inicio': 'page-home',
        '/enfermedades': 'page-diseases',
        '/dashboard': 'page-dashboard',
        '/dashboardbi': 'page-dashboardbi',
      };
      const target = map[r] || 'page-home';
      document.body.setAttribute('data-route', (r || '/inicio').replace('/', ''));
      ['page-home', 'page-diseases', 'page-dashboard', 'page-dashboardbi'].forEach(id => {
        const el = qs(id);
        if (!el) return;
        if (id === target) el.classList.add('active');
        else el.classList.remove('active');
      });
      setActiveNav(r);
      setTimeout(function() {
        try { setHomeChatVisible(); } catch (e) {}
        try { scheduleRefresh(); } catch (e) {}
        try { if (r === '/dashboardbi') loadDashboardBIEmbed(); } catch (e) {}
      }, 80);
    }

    function getRoute() {
      const h = (location.hash || '').replace(/^#/, '');
      if (!h) return '/inicio';
      if (h.startsWith('/')) return h;
      return '/inicio';
    }

    function debounce(fn, wait) {
      let t = null;
      return function() {
        const args = arguments;
        clearTimeout(t);
        t = setTimeout(function() { fn.apply(null, args); }, wait);
      }
    }

    function fmt(n) {
      if (n === null || n === undefined) return '—';
      const x = Number(n);
      if (!isFinite(x)) return '—';
      return x.toLocaleString();
    }

    function escapeHtml(s) {
      return String(s ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }

    function showTooltipAt(html, clientX, clientY) {
      const tip = qs('tooltip');
      if (!tip) return;
      tip.dataset.hideToken = '';
      const wasVisible = tip.style.display === 'block';
      if (tip.innerHTML !== html) tip.innerHTML = html;
      if (!wasVisible) {
        tip.style.display = 'block';
        tip.style.opacity = '0';
        tip.style.transform = 'translateY(4px)';
        requestAnimationFrame(function() {
          tip.style.opacity = '1';
          tip.style.transform = 'translateY(0)';
        });
      }

      tip.style.left = '0px';
      tip.style.top = '0px';
      const rect = tip.getBoundingClientRect();
      const pad = 12;
      let left = clientX + pad;
      let top = clientY + pad;
      if (left + rect.width > window.innerWidth - 8) left = clientX - rect.width - pad;
      if (top + rect.height > window.innerHeight - 8) top = clientY - rect.height - pad;
      left = Math.max(8, Math.min(window.innerWidth - rect.width - 8, left));
      top = Math.max(8, Math.min(window.innerHeight - rect.height - 8, top));
      tip.style.left = left + 'px';
      tip.style.top = top + 'px';
    }

    function hideTooltip() {
      const tip = qs('tooltip');
      if (!tip || tip.style.display !== 'block') return;
      const token = String(Date.now());
      tip.dataset.hideToken = token;
      tip.style.opacity = '0';
      tip.style.transform = 'translateY(4px)';
      setTimeout(function() {
        if (tip.dataset.hideToken === token) tip.style.display = 'none';
      }, 140);
    }

    function normalize(s) {
      return (s || '')
        .normalize('NFKD')
        .replace(/[\\u0300-\\u036f]/g, '')
        .toUpperCase()
        .replace(/[^A-Z0-9]+/g, ' ')
        .trim();
    }

    const MAP_DEPT_ALIAS = {
      'BOGOTA DC': 'BOGOTA',
      'BOGOTA D C': 'BOGOTA',
      'DISTRITO CAPITAL DE BOGOTA': 'BOGOTA',
      'NORTE DE SANTANDER': 'NORTH SANTANDER',
      'SAN ANDRES PROVIDENCIA Y SANTA CATALINA': 'SAN ANDRES Y PROVIDENCIA',
      'ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA': 'SAN ANDRES Y PROVIDENCIA'
    };

    function deptKey(value) {
      let k = normalize(value);
      if (MAP_DEPT_ALIAS[k]) k = MAP_DEPT_ALIAS[k];
      return k;
    }

    function buildGeoIndex(rows) {
      const out = {};
      (rows || []).forEach(r => {
        const k = deptKey(r.departamento);
        out[k] = r;
      });
      return out;
    }

    function deptNameMatches(a, b) {
      const an = normalize(a);
      const bn = normalize(b);
      if (!an || !bn) return false;
      if (an === bn) return true;
      if (an.length > 4 && bn.includes(an)) return true;
      if (bn.length > 4 && an.includes(bn)) return true;
      return false;
    }

    function findGeoRowForDeptName(name) {
      const key = deptKey(name);
      const direct = mapGeoByDeptKey[key];
      if (direct) return direct;
      for (let i = 0; i < (mapGeoRows || []).length; i++) {
        const r = mapGeoRows[i];
        if (!r) continue;
        if (deptNameMatches(name, r.departamento)) return r;
      }
      return null;
    }

    function getSelectedMulti(selectEl) {
      const out = [];
      Array.from(selectEl.options).forEach(o => { if (o.selected) out.push(o.value); });
      return out;
    }

    function getParams() {
      const enfermedades = getSelectedMulti(qs('enfermedades'));
      const p = {
        q: qs('q').value.trim(),
        departamento: (qs('departamento').value || '').trim(),
        enfermedad: enfermedades.join(','),
        ano_min: (qs('anoMin').value || '').trim(),
        ano_max: (qs('anoMax').value || '').trim(),
      };
      const clean = {};
      Object.keys(p).forEach(k => { if (p[k]) clean[k] = p[k]; });
      return clean;
    }

    function updateExportLink() {
      const params = new URLSearchParams(getParams());
      qs('btnExportFiltered').href = '/download_filtered?' + params.toString();
    }

    function setTheme(mode) {
      document.body.setAttribute('data-theme', mode);
      localStorage.setItem('theme', mode);
    }

    function initTheme() {
      const saved = localStorage.getItem('theme');
      if (saved === 'dark' || saved === 'light') setTheme(saved);
    }

    const I18N = {
      es: {
        'app.title': 'Enfermedades en Colombia',
        'app.subtitle': 'Explora la evolución de los casos por año, enfermedad y departamento. Diseñado para entenderse rápido, sin tecnicismos.',
        'status.ready': 'Listo',
        'btn.theme': 'Modo',
        'btn.exportFiltered': 'Exportar (filtrado)',
        'btn.pdf': 'PDF',
        'btn.powerbi': 'Power BI',
        'btn.dashboardBI': 'dashboardBI',
        'btn.csvFull': 'CSV completo',
        'nav.home': 'Inicio',
        'nav.diseases': 'Enfermedades',
        'nav.dashboard': 'Dashboard',
        'nav.dashboardBI': 'dashboardBI',
        'home.title': 'Inicio',
        'home.sub': 'Portal educativo + análisis visual',
        'kpi.totalCases': 'Casos registrados',
        'kpi.totalDataset': 'Total del dataset',
        'kpi.topDisease': 'Enfermedad más común',
        'kpi.byCases': 'Por número de casos',
        'kpi.topRegion': 'Región más afectada',
        'kpi.deptMostCases': 'Departamento con más casos',
        'kpi.changeOverTime': 'Cambio en el tiempo',
        'kpi.overallTrend': 'Tendencia general',
        'home.ctaDash': 'Explorar Dashboard →',
        'home.ctaGuide': 'Guía de enfermedades',
        'home.ctaFaq': 'Preguntas frecuentes',
        'home.tip': 'Consejo: filtra por años y compara enfermedades',
        'home.feature.trends.title': 'Ver tendencias',
        'home.feature.trends.desc': 'Identifica si los casos suben o bajan en el tiempo y compara enfermedades.',
        'home.feature.regions.title': 'Explorar regiones',
        'home.feature.regions.desc': 'Ubica departamentos con mayor incidencia y descubre patrones geográficos.',
        'home.feature.learn.title': 'Aprender y prevenir',
        'home.feature.learn.desc': 'Conoce síntomas comunes, prevención y cuándo consultar para actuar a tiempo.',
        'home.learnQuick.title': 'Aprender y prevenir (guía rápida)',
        'home.learnQuick.more': 'Ver guía completa',
        'home.learnQuick.askIdeas': '¿Qué puedo preguntar?',
        'home.faq1.q': '¿Cómo interpretar este panel?',
        'home.faq1.a': 'Las cifras muestran casos registrados en los datos. Usa el dashboard para filtrar por años, enfermedad y departamento. La tendencia te ayuda a ver si hay aumento o disminución.',
        'home.faq2.q': '¿Qué significa “Cambio en el tiempo”?',
        'home.faq2.a': 'Compara el primer y el último año disponibles en tu selección. Si seleccionas un solo año, no se calcula.',
        'home.faq3.q': '¿De dónde salen los datos?',
        'home.faq3.a': 'Provienen de fuentes públicas (datos.gov.co) integradas en un dataset maestro para análisis y visualización.',
        'home.preview.title': 'Vista previa: tendencia',
        'home.preview.tip': 'Tip: selecciona varias enfermedades para comparar su comportamiento.',
        'home.topMuni.title': 'Top municipios (casos)',
        'home.topMuni.tip': 'Se actualiza con los mismos filtros del dashboard.',
        'powerbi.missing': 'Aún no hay archivo Power BI (.pbix). Cuando lo tengas, colócalo en data/processed/reporte_powerbi.pbix o inicia el servidor con --pbix RUTA.',
        'powerbi.error': 'No se pudo verificar el archivo de Power BI.',
        'dashboardbi.missing': 'Aún no hay archivo brote.pbix en la carpeta raw/. Colócalo como raw/brote.pbix.',
        'dashboardbi.error': 'No se pudo verificar el archivo brote.pbix.',
        'bi.title': 'Dashboard BI',
        'bi.sub': 'Power BI embebido en la app',
        'bi.embedMissing': 'Falta configurar el enlace de Power BI para embeber. Agrega DASHBOARD_BI_EMBED_URL en .env con el enlace de “Publicar en la web” o el enlace de lectura del reporte.',
        'bi.embedError': 'No se pudo cargar el dashboard embebido.',
        'dis.title': 'Enfermedades',
        'dis.sub': 'Información clara para entender y prevenir',
        'dis.search.label': 'Buscador',
        'dis.search.ph': 'Escribe: dengue, zika, chikungunya…',
        'dis.pick.title': 'Selecciona una enfermedad',
        'dis.pick.desc': 'Verás una explicación sencilla y un resumen de datos para tu presentación.',
        'dis.cases': 'Casos',
        'dis.change': 'Cambio',
        'dis.compare.title': 'Comparar en el dashboard',
        'dis.compare.desc': 'Elige 2 o 3 para compararlas en la gráfica de tendencia.',
        'dis.compare.go': 'Comparar ahora',
        'dis.compare.clear': 'Limpiar',
        'dis.detail.title': 'Detalle',
        'dis.detail.sub': 'Educativo, simple y directo',
        'dis.detail.lead': 'Selecciona una enfermedad para ver información.',
        'dis.detail.kpiCases': 'Casos (dataset)',
        'dis.detail.kpiCasesDesc': 'Total disponible',
        'dis.detail.kpiTrend': 'Tendencia',
        'dis.detail.viewDash': 'Ver en el Dashboard',
        'dis.detail.addCompare': 'Añadir a comparación',
        'dis.detail.symptoms': 'Síntomas comunes',
        'dis.detail.prevention': 'Prevención',
        'dis.detail.when': 'Cuándo consultar',
        'dis.detail.noteTitle': 'Nota importante',
        'dis.detail.noteText': 'Este sitio es educativo y no reemplaza una consulta médica. Si tienes síntomas graves o persistentes, busca atención profesional.',
        'dis.faq.title': 'Preguntas frecuentes',
        'dis.faq1.q': '¿Qué significa “brote”?',
        'dis.faq1.a': 'Es una señal de alerta cuando los casos superan lo esperado para ese municipio y enfermedad. Sirve para orientar la atención, no para alarmar.',
        'dis.faq2.q': '¿Por qué algunas enfermedades suben en ciertas épocas?',
        'dis.faq2.a': 'La lluvia y la temperatura influyen en la presencia de mosquitos. También afectan factores como movilidad, prevención y acceso a servicios de salud.',
        'dis.faq3.q': '¿Qué puedo hacer en casa para reducir el riesgo?',
        'dis.faq3.a': 'Elimina agua estancada, lava recipientes, usa repelente, coloca mosquiteros y revisa patios y canaletas semanalmente.',
        'dis.faq4.q': '¿Qué significa “región más afectada” en el dashboard?',
        'dis.faq4.a': 'Es el departamento con más casos dentro de tu selección (filtros). Puedes cambiarlo al filtrar por años o enfermedades.',
      },
      en: {
        'app.title': 'Diseases in Colombia',
        'app.subtitle': 'Explore how cases evolve by year, disease, and department. Designed to be clear and non-technical.',
        'status.ready': 'Ready',
        'btn.theme': 'Theme',
        'btn.exportFiltered': 'Export (filtered)',
        'btn.pdf': 'PDF',
        'btn.powerbi': 'Power BI',
        'btn.dashboardBI': 'dashboardBI',
        'btn.csvFull': 'Full CSV',
        'nav.home': 'Home',
        'nav.diseases': 'Diseases',
        'nav.dashboard': 'Dashboard',
        'nav.dashboardBI': 'dashboardBI',
        'home.title': 'Home',
        'home.sub': 'Educational portal + visual insights',
        'kpi.totalCases': 'Reported cases',
        'kpi.totalDataset': 'Dataset total',
        'kpi.topDisease': 'Most common disease',
        'kpi.byCases': 'By number of cases',
        'kpi.topRegion': 'Most affected region',
        'kpi.deptMostCases': 'Department with most cases',
        'kpi.changeOverTime': 'Change over time',
        'kpi.overallTrend': 'Overall trend',
        'home.ctaDash': 'Explore Dashboard →',
        'home.ctaGuide': 'Disease guide',
        'home.ctaFaq': 'FAQ',
        'home.tip': 'Tip: filter by years and compare diseases',
        'home.feature.trends.title': 'See trends',
        'home.feature.trends.desc': 'Check whether cases go up or down over time and compare diseases.',
        'home.feature.regions.title': 'Explore regions',
        'home.feature.regions.desc': 'Identify departments with higher incidence and discover geographic patterns.',
        'home.feature.learn.title': 'Learn & prevent',
        'home.feature.learn.desc': 'Learn common symptoms, prevention, and when to seek care.',
        'home.learnQuick.title': 'Learn & prevent (quick guide)',
        'home.learnQuick.more': 'Open full guide',
        'home.learnQuick.askIdeas': 'What can I ask?',
        'home.faq1.q': 'How do I read this panel?',
        'home.faq1.a': 'Numbers reflect cases recorded in the dataset. Use the dashboard to filter by years, disease, and department. The trend helps you see increases or decreases.',
        'home.faq2.q': 'What does “Change over time” mean?',
        'home.faq2.a': 'It compares the first and last year in your selection. If you pick a single year, it cannot be computed.',
        'home.faq3.q': 'Where does the data come from?',
        'home.faq3.a': 'It comes from public sources (datos.gov.co) combined into a master dataset for analysis and visualization.',
        'home.preview.title': 'Preview: trend',
        'home.preview.tip': 'Tip: select multiple diseases to compare their behavior.',
        'home.topMuni.title': 'Top municipalities (cases)',
        'home.topMuni.tip': 'Updates with the same dashboard filters.',
        'powerbi.missing': 'Power BI file (.pbix) is not available yet. When you have it, place it in data/processed/reporte_powerbi.pbix or start the server with --pbix PATH.',
        'powerbi.error': 'Could not check the Power BI file.',
        'dashboardbi.missing': 'brote.pbix is not available in the raw/ folder. Place it as raw/brote.pbix.',
        'dashboardbi.error': 'Could not check brote.pbix.',
        'bi.title': 'BI Dashboard',
        'bi.sub': 'Power BI embedded in the app',
        'bi.embedMissing': 'Missing Power BI embed link. Set DASHBOARD_BI_EMBED_URL in .env with a “Publish to web” link or a view link.',
        'bi.embedError': 'Could not load the embedded dashboard.',
        'dis.title': 'Diseases',
        'dis.sub': 'Clear information to understand and prevent',
        'dis.search.label': 'Search',
        'dis.search.ph': 'Type: dengue, zika, chikungunya…',
        'dis.pick.title': 'Pick a disease',
        'dis.pick.desc': 'You will see a simple explanation and a data summary for your presentation.',
        'dis.cases': 'Cases',
        'dis.change': 'Change',
        'dis.compare.title': 'Compare in the dashboard',
        'dis.compare.desc': 'Choose 2–3 to compare them in the trend chart.',
        'dis.compare.go': 'Compare now',
        'dis.compare.clear': 'Clear',
        'dis.detail.title': 'Details',
        'dis.detail.sub': 'Educational, simple, and direct',
        'dis.detail.lead': 'Select a disease to view information.',
        'dis.detail.kpiCases': 'Cases (dataset)',
        'dis.detail.kpiCasesDesc': 'Total available',
        'dis.detail.kpiTrend': 'Trend',
        'dis.detail.viewDash': 'View in Dashboard',
        'dis.detail.addCompare': 'Add to comparison',
        'dis.detail.symptoms': 'Common symptoms',
        'dis.detail.prevention': 'Prevention',
        'dis.detail.when': 'When to seek care',
        'dis.detail.noteTitle': 'Important note',
        'dis.detail.noteText': 'This site is educational and does not replace medical advice. If you have severe or persistent symptoms, seek professional care.',
        'dis.faq.title': 'FAQ',
        'dis.faq1.q': 'What does “outbreak” mean?',
        'dis.faq1.a': 'It is an alert signal when cases exceed what is expected for that municipality and disease. It helps guide attention, not alarm.',
        'dis.faq2.q': 'Why do some diseases increase in certain seasons?',
        'dis.faq2.a': 'Rain and temperature affect mosquito presence. Mobility, prevention, and access to care can also influence trends.',
        'dis.faq3.q': 'What can I do at home to reduce risk?',
        'dis.faq3.a': 'Remove standing water, clean containers, use repellent, use bed nets/screens, and check patios and gutters weekly.',
        'dis.faq4.q': 'What does “most affected region” mean?',
        'dis.faq4.a': 'It is the department with the most cases within your current filters. It will change as you adjust years or diseases.',
      },
    };

    function getLang() {
      const saved = localStorage.getItem('lang');
      if (saved === 'en' || saved === 'es') return saved;
      const nav = (navigator.language || '').toLowerCase();
      return nav.startsWith('en') ? 'en' : 'es';
    }

    function setLang(lang) {
      localStorage.setItem('lang', lang);
      applyLang(lang);
    }

    function t(key) {
      const lang = getLang();
      return (I18N[lang] && I18N[lang][key]) ? I18N[lang][key] : (I18N.es[key] || key);
    }

    function applyLang(lang) {
      const l = (lang === 'en') ? 'en' : 'es';
      document.documentElement.lang = l;
      document.title = I18N[l]['app.title'] || document.title;

      Array.from(document.querySelectorAll('[data-i18n]')).forEach(el => {
        const key = el.getAttribute('data-i18n') || '';
        if (!key) return;
        el.textContent = (I18N[l][key] || I18N.es[key] || el.textContent);
      });
      Array.from(document.querySelectorAll('[data-i18n-placeholder]')).forEach(el => {
        const key = el.getAttribute('data-i18n-placeholder') || '';
        if (!key) return;
        el.setAttribute('placeholder', (I18N[l][key] || I18N.es[key] || el.getAttribute('placeholder') || ''));
      });

      const btn = document.getElementById('btnLang');
      if (btn) btn.textContent = (l === 'en') ? 'EN' : 'ES';

      const guide = document.getElementById('homeGuideList');
      if (guide) {
        const items = (l === 'en')
          ? [
              '<strong>Remove breeding sites:</strong> empty, cover, and clean containers with water.',
              '<strong>Protect your skin:</strong> use repellent and wear clothing that covers arms and legs.',
              '<strong>Protect your home:</strong> bed nets, window screens, and ventilation.',
              '<strong>Warning signs:</strong> bleeding, severe pain, or breathing difficulty → seek care.',
            ]
          : [
              '<strong>Evita criaderos:</strong> vacía, tapa y limpia recipientes con agua.',
              '<strong>Protege tu piel:</strong> usa repelente y ropa que cubra brazos y piernas.',
              '<strong>Protege tu casa:</strong> mosquiteros, mallas y ventilación.',
              '<strong>Señales de alarma:</strong> si hay sangrado, dolor intenso o dificultad respiratoria, busca atención.',
            ];
        guide.innerHTML = items.map(x => '<li>' + x + '</li>').join('');
      }

      const chat = document.getElementById('homeChat');
      if (chat) {
        const header = chat.querySelector('.hd .muted');
        if (header) header.textContent = (l === 'en')
          ? 'Ask me about diseases or what you see in the data'
          : 'Pregúntame sobre las enfermedades o sobre lo que ves en los datos';
      }
    }

    function initLang() {
      applyLang(getLang());
      const btn = document.getElementById('btnLang');
      if (btn) {
        btn.addEventListener('click', function() {
          const cur = getLang();
          setLang(cur === 'es' ? 'en' : 'es');
          try { scheduleRefresh(); } catch (e) {}
        });
      }
    }

    function canvasSize(canvas, height) {
      const dpr = window.devicePixelRatio || 1;
      const w = canvas.clientWidth;
      const h = height;
      canvas.width = Math.floor(w * dpr);
      canvas.height = Math.floor(h * dpr);
      const ctx = canvas.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      return { ctx, w, h };
    }

    function drawAxes(ctx, w, h, pad) {
      ctx.strokeStyle = 'rgba(15,23,42,0.12)';
      if (document.body.getAttribute('data-theme') === 'dark') ctx.strokeStyle = 'rgba(255,255,255,0.14)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad.l, pad.t);
      ctx.lineTo(pad.l, h - pad.b);
      ctx.lineTo(w - pad.r, h - pad.b);
      ctx.stroke();
    }

    function diseaseColor(name, idx) {
      const n = normalize(name);
      if (n.includes('CHIKUNGUNYA')) return '#ef4444';
      if (n.includes('DENGUE')) return '#0284c7';
      if (n.includes('ZIKA')) return '#10b981';
      const fallback = ['#0284c7', '#10b981', '#6366f1', '#f59e0b', '#a855f7'];
      return fallback[idx % fallback.length];
    }

    function drawLineMulti(canvas, labels, series, hoverIndex) {
      labels = labels || [];
      series = series || [];
      const { ctx, w, h } = canvasSize(canvas, 260);
      ctx.clearRect(0, 0, w, h);
      ctx.font = '12px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      const pad = { l: 44, r: 12, t: 10, b: 26 };
      drawAxes(ctx, w, h, pad);
      const iw = w - pad.l - pad.r;
      const ih = h - pad.t - pad.b;
      const n = Math.max(1, (labels || []).length, ...((series || []).map(s => (s.data || []).length)));
      const all = [];
      series.forEach(s => (s.data || []).forEach(v => all.push(Number(v) || 0)));
      const maxV = Math.max(1, ...all);
      const ticks = 4;

      for (let i = 0; i <= ticks; i++) {
        const t = i / ticks;
        const y = pad.t + ih - t * ih;
        ctx.strokeStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.08)' : 'rgba(15,23,42,0.08)';
        ctx.beginPath();
        ctx.moveTo(pad.l, y);
        ctx.lineTo(pad.l + iw, y);
        ctx.stroke();
        ctx.fillStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.55)' : 'rgba(15,23,42,0.55)';
        ctx.fillText(Math.round(t * maxV).toLocaleString(), 6, y + 4);
      }

      series.forEach((s, idx) => {
        const data = s.data || [];
        if (!data.length) return;
        ctx.strokeStyle = diseaseColor(s.name || '', idx);
        ctx.lineWidth = 2.25;
        ctx.beginPath();
        for (let i = 0; i < data.length; i++) {
          const x = pad.l + (i / Math.max(1, n - 1)) * iw;
          const v = Number(data[i]) || 0;
          const y = pad.t + ih - (v / maxV) * ih;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        }
        ctx.stroke();
      });

      if (hoverIndex !== null && hoverIndex !== undefined) {
        const hi = Math.max(0, Math.min(n - 1, hoverIndex));
        const x = pad.l + (hi / Math.max(1, n - 1)) * iw;
        const bg = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(7,16,31,0.92)' : 'rgba(255,255,255,0.92)';
        series.forEach((s, idx) => {
          const data = s.data || [];
          if (!data.length) return;
          if (hi >= data.length) return;
          const v = Number(data[hi]) || 0;
          const y = pad.t + ih - (v / maxV) * ih;
          ctx.beginPath();
          ctx.arc(x, y, 6, 0, Math.PI * 2);
          ctx.fillStyle = bg;
          ctx.fill();
          ctx.lineWidth = 2.5;
          ctx.strokeStyle = diseaseColor(s.name || '', idx);
          ctx.stroke();
          ctx.beginPath();
          ctx.arc(x, y, 2.6, 0, Math.PI * 2);
          ctx.fillStyle = diseaseColor(s.name || '', idx);
          ctx.fill();
        });
      }

      const legendY = h - 8;
      let x = pad.l;
      series.slice(0, 5).forEach((s, idx) => {
        const label = String(s.name || '').slice(0, 18);
        ctx.fillStyle = diseaseColor(s.name || '', idx);
        ctx.fillRect(x, legendY - 9, 10, 10);
        ctx.fillStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.78)' : 'rgba(15,23,42,0.78)';
        ctx.fillText(label, x + 14, legendY);
        x += 14 + ctx.measureText(label).width + 14;
      });

      if (!canvas.__lineMultiState) canvas.__lineMultiState = {};
      canvas.__lineMultiState.labels = labels || [];
      canvas.__lineMultiState.series = series || [];
      canvas.__lineMultiState.pad = pad;
      canvas.__lineMultiState.maxV = maxV;
      canvas.__lineMultiState.n = n;
      canvas.__lineMultiState.ih = ih;
      canvas.__lineMultiState.iw = iw;

      if (!canvas.__lineMultiState.listenersAttached) {
        canvas.__lineMultiState.listenersAttached = true;
        canvas.__lineMultiState.hoverIndex = null;

        canvas.addEventListener('mousemove', function(ev) {
          const state = canvas.__lineMultiState;
          if (!state) return;

          const rect = canvas.getBoundingClientRect();
          const mx = ev.clientX - rect.left;
          const my = ev.clientY - rect.top;
          const pad = state.pad;
          const n = Math.max(1, state.n || 1);
          const iw = rect.width - pad.l - pad.r;
          const ih = rect.height - pad.t - pad.b;
          if (iw <= 0 || ih <= 0) return;

          const t = (mx - pad.l) / iw;
          const idx = Math.max(0, Math.min(n - 1, Math.round(t * Math.max(1, n - 1))));

          const x = pad.l + (idx / Math.max(1, n - 1)) * iw;
          const hitR = 10;
          let minD2 = Infinity;
          (state.series || []).forEach((s) => {
            const data = s.data || [];
            if (!data.length) return;
            if (idx >= data.length) return;
            const v = Number(data[idx]) || 0;
            const y = pad.t + ih - (v / (state.maxV || 1)) * ih;
            const dx = mx - x;
            const dy = my - y;
            const d2 = dx * dx + dy * dy;
            if (d2 < minD2) minD2 = d2;
          });

          if (minD2 <= hitR * hitR) {
            if (state.hoverIndex !== idx) {
              state.hoverIndex = idx;
              drawLineMulti(canvas, state.labels, state.series, idx);
            }

            const year = (state.labels && state.labels.length) ? (state.labels[idx] ?? (idx + 1)) : (idx + 1);
            const lines = (state.series || []).map((s) => {
              const name = escapeHtml(s.name || 'Serie');
              const v = Number((s.data || [])[idx]) || 0;
              return '<div style="display:flex;justify-content:space-between;gap:10px;"><span>' + name + ':</span><strong>' + fmt(v) + ' casos</strong></div>';
            });
            const html =
              '<div style="font-weight:700;margin-bottom:6px;">Año: ' + escapeHtml(year) + '</div>' +
              '<div style="display:grid;gap:4px;">' + lines.join('') + '</div>';
            showTooltipAt(html, ev.clientX, ev.clientY);
          } else {
            if (state.hoverIndex !== null) {
              state.hoverIndex = null;
              drawLineMulti(canvas, state.labels, state.series, null);
            }
            hideTooltip();
          }
        });

        canvas.addEventListener('mouseleave', function() {
          const state = canvas.__lineMultiState;
          if (state && state.hoverIndex !== null) {
            state.hoverIndex = null;
            drawLineMulti(canvas, state.labels, state.series, null);
          }
          hideTooltip();
        });
      }
    }

    function drawBars(canvas, items, color) {
      const { ctx, w, h } = canvasSize(canvas, 260);
      ctx.clearRect(0, 0, w, h);
      ctx.font = '12px ui-sans-serif, system-ui, Segoe UI, Roboto, Arial';
      const pad = { l: 12, r: 12, t: 10, b: 42 };
      const iw = w - pad.l - pad.r;
      const ih = h - pad.t - pad.b;
      const values = items.map(x => Number(x.value) || 0);
      const maxV = Math.max(1, ...values);
      const n = Math.max(1, items.length);
      const gap = 10;
      const barW = Math.max(18, (iw - gap * (n - 1)) / n);
      const baseFill = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.10)' : 'rgba(2,8,23,0.06)';
      for (let i = 0; i < items.length; i++) {
        const x = pad.l + i * (barW + gap);
        const v = Number(items[i].value) || 0;
        const bh = (v / maxV) * ih;
        const y = pad.t + (ih - bh);
        ctx.fillStyle = baseFill;
        ctx.fillRect(x, pad.t, barW, ih);
        ctx.fillStyle = items[i].color || color || '#10b981';
        ctx.fillRect(x, y, barW, bh);
        ctx.save();
        ctx.translate(x + barW / 2, h - 10);
        ctx.rotate(-Math.PI / 6);
        ctx.textAlign = 'center';
        ctx.fillStyle = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.75)' : 'rgba(15,23,42,0.70)';
        ctx.fillText(String(items[i].label).slice(0, 14), 0, 0);
        ctx.restore();
      }
    }

    function buildMap() {
      const box = qs('mapBox');
      box.innerHTML = '';
      if (!mapData || !mapData.map || !mapData.map.locations) {
        box.innerHTML = '<div class="muted" style="padding:10px;">Mapa no disponible. Verifica conexión a internet.</div>';
        return;
      }

      const viewBox = mapData.map.viewBox;
      const locations = mapData.map.locations;
      const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
      svg.setAttribute('viewBox', viewBox);
      svg.setAttribute('aria-label', 'Mapa de Colombia por departamentos');

      locations.forEach(loc => {
        const p = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        p.setAttribute('d', loc.path);
        p.setAttribute('data-name', loc.name);
        p.setAttribute('data-id', loc.id);
        const depVal = findDepartmentOptionValue(loc.name);
        if (depVal) p.setAttribute('data-dept', depVal);
        p.style.fill = '#e0f2fe';
        p.style.stroke = document.body.getAttribute('data-theme') === 'dark' ? 'rgba(255,255,255,0.18)' : 'rgba(15,23,42,0.18)';
        p.style.strokeWidth = '0.8';
        p.style.cursor = 'pointer';
        svg.appendChild(p);

        const hit = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        hit.setAttribute('d', loc.path);
        hit.setAttribute('data-name', loc.name);
        hit.setAttribute('data-id', loc.id);
        hit.setAttribute('data-hit', '1');
        if (depVal) hit.setAttribute('data-dept', depVal);
        hit.style.fill = 'transparent';
        hit.style.stroke = 'transparent';
        hit.style.strokeWidth = '14';
        hit.style.pointerEvents = 'stroke';
        hit.style.cursor = 'pointer';
        hit.addEventListener('mousemove', onMapHover);
        hit.addEventListener('mouseleave', onMapLeave);
        hit.addEventListener('click', function() {
          const name = String(loc.name || '');
          const dep = hit.getAttribute('data-dept') || findDepartmentOptionValue(name) || name;
          mapSelectedDeptName = dep;
          mapSelectedDeptKey = deptKey(mapSelectedDeptName);
          renderMapDetail();
        });
        svg.appendChild(hit);
      });

      box.appendChild(svg);
    }

    function findDepartmentOptionValue(mapName) {
      const depSel = qs('departamento');
      if (!depSel) return null;
      const targetKey = deptKey(mapName);
      const targetNorm = normalize(mapName);
      const opts = Array.from(depSel.options).map(o => ({ val: o.value, key: deptKey(o.value), norm: normalize(o.value) }));
      const foundKey = opts.find(o => o.key && o.key === targetKey);
      if (foundKey) return foundKey.val;
      const found = opts.find(o =>
        o.norm === targetNorm ||
        (targetNorm.includes(o.norm) && o.norm.length > 4) ||
        (o.norm.includes(targetNorm) && targetNorm.length > 4)
      );
      return found ? found.val : null;
    }

    function renderMapDetail() {
      const box = qs('mapDetail');
      if (!box) return;
      if (!mapSelectedDeptKey) {
        box.style.display = 'none';
        return;
      }

      const row = findGeoRowForDeptName(mapSelectedDeptName || mapSelectedDeptKey) || {};
      const dengue = Number(row.dengue) || 0;
      const zika = Number(row.zika) || 0;
      const chik = Number(row.chikungunya) || 0;
      const total = Number(row.casos) || (dengue + zika + chik) || 0;
      const depName = (row.departamento || mapSelectedDeptName || '—');
      mapSelectedDeptName = depName;
      mapSelectedDeptKey = deptKey(depName);

      const html =
        '<div class="rowline" style="align-items:center;">' +
          '<div class="title">Departamento: ' + escapeHtml(depName) + '</div>' +
          '<button class="btn ghost" id="btnMapDetailClose" title="Cerrar">Cerrar</button>' +
        '</div>' +
        '<div class="rowline"><span>Dengue</span><strong>' + fmt(dengue) + ' casos</strong></div>' +
        '<div class="rowline"><span>Zika</span><strong>' + fmt(zika) + ' casos</strong></div>' +
        '<div class="rowline"><span>Chikungunya</span><strong>' + fmt(chik) + ' casos</strong></div>' +
        '<div class="rowline"><span class="muted">Total</span><span class="total">' + fmt(total) + ' casos</span></div>' +
        '<div style="display:flex;gap:10px;flex-wrap:wrap;">' +
          '<button class="btn" id="btnMapApplyDept">Filtrar este departamento</button>' +
        '</div>';

      box.innerHTML = html;
      box.style.display = 'grid';

      const btnClose = qs('btnMapDetailClose');
      if (btnClose) {
        btnClose.addEventListener('click', function() {
          mapSelectedDeptKey = null;
          mapSelectedDeptName = null;
          box.style.display = 'none';
        });
      }

      const btnApply = qs('btnMapApplyDept');
      if (btnApply) {
        btnApply.addEventListener('click', function() {
          const val = findDepartmentOptionValue(depName);
          if (!val) {
            toast('No pude emparejar este departamento con el dataset: ' + depName);
            return;
          }
          qs('departamento').value = val;
          scheduleRefresh();
        });
      }
    }

    function onMapHover(ev) {
      const el = ev.target;
      const name = el.getAttribute('data-dept') || el.getAttribute('data-name') || '';
      const cases = el.getAttribute('data-cases') || '0';
      const html = '<strong>' + escapeHtml(name) + '</strong><div class="muted">Casos: ' + fmt(cases) + '</div><div class="muted">Clic para ver detalle</div>';
      showTooltipAt(html, ev.clientX, ev.clientY);
    }

    function onMapLeave() {
      hideTooltip();
    }

    function updateMapColors(rows) {
      const svg = qs('mapBox').querySelector('svg');
      if (!svg) return;
      const vals = (rows || []).map(r => Number((r || {}).casos) || 0);
      const maxV = Math.max(1, ...vals);

      const paths = svg.querySelectorAll('path:not([data-hit])');
      paths.forEach(p => {
        const name = p.getAttribute('data-name') || '';
        const dep = p.getAttribute('data-dept') || name;
        const row = findGeoRowForDeptName(dep);
        const v = row ? (Number(row.casos) || 0) : 0;
        p.setAttribute('data-cases', String(v));
        const t = v / maxV;
        const c1 = [224, 242, 254];
        const c2 = [2, 132, 199];
        const rr = Math.round(c1[0] + (c2[0] - c1[0]) * t);
        const gg = Math.round(c1[1] + (c2[1] - c1[1]) * t);
        const bb = Math.round(c1[2] + (c2[2] - c1[2]) * t);
        p.style.fill = 'rgb(' + rr + ',' + gg + ',' + bb + ')';
      });

      qs('mapHint').textContent = 'Max: ' + fmt(maxV) + ' casos';
    }

    function renderSummary(sum) {
      qs('kpiCases').textContent = fmt(sum.total_cases);
      qs('kpiTopDis').textContent = sum.top_enfermedad || '—';
      qs('kpiTopDept').textContent = sum.top_departamento || '—';
      qs('pillCount').textContent = 'Filas: ' + fmt(sum.filtered_total_rows);

      const homeCases = document.getElementById('homeKpiCases');
      if (homeCases) homeCases.textContent = fmt(sum.total_cases);
      const homeTopDis = document.getElementById('homeKpiTopDis');
      if (homeTopDis) homeTopDis.textContent = sum.top_enfermedad || '—';
      const homeTopDept = document.getElementById('homeKpiTopDept');
      if (homeTopDept) homeTopDept.textContent = sum.top_departamento || '—';

      const v = sum.variation;
      if (v && v.pct !== null && v.pct !== undefined) {
        const dir = v.direction;
        const badge = dir === 'sube' ? '<span class="badge-up">↑</span>' : (dir === 'baja' ? '<span class="badge-down">↓</span>' : '<span class="badge-up">•</span>');
        const pctText = (dir === 'baja') ? ('-' + Math.abs(v.pct) + '%') : (v.pct + '%');
        qs('kpiVar').innerHTML = badge + ' ' + pctText;
        qs('kpiVarDesc').textContent = v.from_year + ' → ' + v.to_year;
        const hk = document.getElementById('homeKpiVar');
        if (hk) hk.innerHTML = badge + ' ' + pctText;
        const hkDesc = document.getElementById('homeKpiVarDesc');
        if (hkDesc) hkDesc.textContent = v.from_year + ' → ' + v.to_year;
      } else {
        qs('kpiVar').textContent = '—';
        qs('kpiVarDesc').textContent = 'Selecciona más de un año';
        const hk = document.getElementById('homeKpiVar');
        if (hk) hk.textContent = '—';
        const hkDesc = document.getElementById('homeKpiVarDesc');
        if (hkDesc) hkDesc.textContent = 'Selecciona más de un año';
      }

      const insights = qs('insights');
      insights.innerHTML = '';
      (sum.insights || []).slice(0, 4).forEach(t => {
        const div = document.createElement('div');
        div.className = 'insight';
        div.textContent = t;
        insights.appendChild(div);
      });
      if (!(sum.insights || []).length) {
        const div = document.createElement('div');
        div.className = 'insight';
        div.textContent = 'No hay suficientes datos con estos filtros. Prueba ampliar el rango de años o quitar filtros.';
        insights.appendChild(div);
      }

      const trend = sum.trend || { labels: [], series: [] };
      drawLineMulti(qs('chartTrend'), trend.labels || [], trend.series || []);
      const homeTrend = document.getElementById('homeTrend');
      if (homeTrend) drawLineMulti(homeTrend, trend.labels || [], trend.series || []);

      const homeTopMuni = document.getElementById('homeTopMuni');
      if (homeTopMuni) {
        const topHome = (sum.top_municipios || []).slice(0, 8).map(x => ({ label: x.municipio, value: Number(x.casos) || 0 }));
        drawBars(homeTopMuni, topHome, '#0284c7');
      }

      const homeInsights = document.getElementById('homeInsights');
      if (homeInsights) {
        homeInsights.innerHTML = '';
        (sum.insights || []).slice(0, 2).forEach(t => {
          const div = document.createElement('div');
          div.className = 'insight';
          div.textContent = t;
          homeInsights.appendChild(div);
        });
      }

      const dis = (sum.disease_stats || []).slice(0, 6).map((x, idx) => ({ label: x.enfermedad, value: Number(x.casos) || 0, color: diseaseColor(x.enfermedad, idx) }));
      drawBars(qs('chartDisease'), dis, '#10b981');

      const top = (sum.top_municipios || []).slice(0, 8).map(x => ({ label: x.municipio, value: Number(x.casos) || 0 }));
      drawBars(qs('chartTopMuni'), top, '#0284c7');
    }

    async function loadMeta() {
      const meta = await fetchJSON('/api/meta');
      columns = meta.columns || [];
      total = meta.total_rows || 0;
      const thead = qs('thead');
      thead.innerHTML = '';
      columns.forEach(c => {
        const th = document.createElement('th');
        th.textContent = c;
        thead.appendChild(th);
      });
    }

    async function loadValues() {
      const data = await fetchJSON('/api/values');
      const dep = qs('departamento');
      dep.innerHTML = '<option value="">(cualquiera)</option>';
      (data.departamentos || []).forEach(v => {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        dep.appendChild(opt);
      });

      const enf = qs('enfermedades');
      enf.innerHTML = '';
      (data.enfermedades || []).forEach(v => {
        const opt = document.createElement('option');
        opt.value = v;
        opt.textContent = v;
        enf.appendChild(opt);
      });

      const years = (data.anos || []);
      const yMin = qs('anoMin');
      const yMax = qs('anoMax');
      yMin.innerHTML = '<option value="">(cualquiera)</option>';
      yMax.innerHTML = '<option value="">(cualquiera)</option>';
      years.forEach(y => {
        const o1 = document.createElement('option');
        o1.value = String(y);
        o1.textContent = String(y);
        yMin.appendChild(o1);
        const o2 = document.createElement('option');
        o2.value = String(y);
        o2.textContent = String(y);
        yMax.appendChild(o2);
      });
      if (years.length) {
        yMin.value = String(years[0]);
        yMax.value = String(years[years.length - 1]);
      }
    }

    async function loadMap() {
      mapData = await fetchJSON('/api/map');
      buildMap();
    }

    function setPageInfo(limit) {
      const start = Math.min(offset + 1, total);
      const end = Math.min(offset + limit, total);
      qs('pageInfo').textContent = start.toLocaleString() + '–' + end.toLocaleString() + ' de ' + total.toLocaleString();
    }

    async function loadTable() {
      const limit = Math.max(1, Math.min(500, parseInt(qs('limit').value || '50', 10)));
      const p = getParams();
      p.offset = String(offset);
      p.limit = String(limit);
      const payload = await fetchJSON('/api/data?' + new URLSearchParams(p).toString());
      const rows = payload.rows || [];
      const filteredTotal = payload.filtered_total ?? total;
      total = filteredTotal;
      setPageInfo(limit);

      const tbody = qs('tbody');
      tbody.innerHTML = '';
      rows.forEach(r => {
        const tr = document.createElement('tr');
        columns.forEach(c => {
          const td = document.createElement('td');
          const v = r[c];
          td.textContent = (v === null || v === undefined) ? '' : String(v);
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    }

    async function refreshAll() {
      setStatus('Actualizando…', true);
      updateExportLink();
      const p = getParams();
      const qs1 = new URLSearchParams(p).toString();
      const sum = await fetchJSON('/api/summary?' + qs1);
      renderSummary(sum);
      const geo = await fetchJSON('/api/geo?' + qs1);
      mapGeoRows = geo.rows || [];
      mapGeoByDeptKey = buildGeoIndex(mapGeoRows);
      updateMapColors(mapGeoRows);
      renderMapDetail();
      offset = 0;
      await loadTable();
      setStatus('Listo', false);
    }

    const scheduleRefresh = debounce(function() {
      refreshAll().catch(e => { setStatus('Error', false); toast(e.message || String(e)); });
    }, 280);

    async function ask() {
      const q = qs('askInput').value.trim();
      if (!q) return;
      setStatus('Pensando…', true);
      const p = getParams();
      p.question = q;
      const res = await fetchJSON('/api/ask?' + new URLSearchParams(p).toString());
      qs('askAnswer').textContent = res.answer || 'No pude responder.';
      setStatus('Listo', false);
    }

    function resetAll() {
      qs('q').value = '';
      qs('departamento').value = '';
      Array.from(qs('enfermedades').options).forEach(o => { o.selected = false; });
      const minSel = qs('anoMin');
      const maxSel = qs('anoMax');
      if (minSel.options.length) minSel.selectedIndex = 0;
      if (maxSel.options.length) maxSel.selectedIndex = 0;
      qs('askInput').value = '';
      qs('askAnswer').textContent = 'Escribe una pregunta para obtener una respuesta rápida.';
      updateExportLink();
    }

    function setDiseaseSelection(names) {
      const wanted = new Set((names || []).map(x => String(x || '').trim()).filter(Boolean));
      Array.from(qs('enfermedades').options).forEach(o => { o.selected = wanted.has(o.value); });
    }

    function selectDiseaseAndGo(name) {
      setDiseaseSelection([name]);
      location.hash = '#/dashboard';
      showPage('/dashboard');
      offset = 0;
      scheduleRefresh();
    }

    function initDiseasePage() {
      const diseaseInfo = {
        DENGUE: {
          es: {
            title: 'Dengue',
            lead: 'Enfermedad viral transmitida por mosquitos. Puede presentarse como un cuadro leve o, en algunos casos, complicarse.',
            symptoms: ['Fiebre alta', 'Dolor de cabeza', 'Dolor detrás de los ojos', 'Dolor muscular y articular', 'Náuseas o malestar general'],
            prevention: ['Eliminar recipientes con agua estancada', 'Usar repelente', 'Instalar mosquiteros o mallas', 'Usar ropa que cubra la piel (especialmente al amanecer y atardecer)'],
            when: ['Fiebre persistente por más de 2 días', 'Dolor intenso o deshidratación', 'Sangrado, somnolencia o dificultad para respirar (urgencias)'],
          },
          en: {
            title: 'Dengue',
            lead: 'A mosquito-borne viral disease. It can be mild, but in some cases it may become severe.',
            symptoms: ['High fever', 'Headache', 'Pain behind the eyes', 'Muscle and joint pain', 'Nausea or general discomfort'],
            prevention: ['Remove standing water containers', 'Use repellent', 'Use bed nets or window screens', 'Wear clothing that covers skin (especially dawn and dusk)'],
            when: ['Fever lasting more than 2 days', 'Severe pain or dehydration', 'Bleeding, drowsiness, or breathing difficulty (urgent)'],
          },
        },
        ZIKA: {
          es: {
            title: 'Zika',
            lead: 'Virus transmitido por mosquitos. Suele ser leve, pero es importante en embarazo por posibles complicaciones.',
            symptoms: ['Sarpullido', 'Fiebre baja', 'Dolor articular', 'Ojos rojos (conjuntivitis)', 'Cansancio'],
            prevention: ['Evitar picaduras (repelente, ropa larga)', 'Eliminar criaderos de mosquitos', 'Consultar ante síntomas si estás embarazada o planeas estarlo'],
            when: ['Síntomas durante el embarazo', 'Fiebre con sarpullido que empeora', 'Cualquier señal de alarma o malestar intenso'],
          },
          en: {
            title: 'Zika',
            lead: 'A mosquito-borne virus. Symptoms are often mild, but it is especially important during pregnancy.',
            symptoms: ['Rash', 'Low fever', 'Joint pain', 'Red eyes (conjunctivitis)', 'Fatigue'],
            prevention: ['Avoid mosquito bites (repellent, long sleeves)', 'Remove breeding sites', 'Seek care if you are pregnant and have symptoms'],
            when: ['Symptoms during pregnancy', 'Worsening rash with fever', 'Any warning sign or severe discomfort'],
          },
        },
        CHIKUNGUNYA: {
          es: {
            title: 'Chikungunya',
            lead: 'Virus transmitido por mosquitos. Puede causar dolor articular fuerte que limita actividades diarias.',
            symptoms: ['Fiebre', 'Dolor articular intenso', 'Hinchazón articular', 'Dolor muscular', 'Dolor de cabeza'],
            prevention: ['Repelente y mosquiteros', 'Eliminar criaderos', 'Mantener patios y recipientes sin agua acumulada'],
            when: ['Dolor articular severo o prolongado', 'Fiebre alta persistente', 'Deshidratación o síntomas que empeoran'],
          },
          en: {
            title: 'Chikungunya',
            lead: 'A mosquito-borne virus that can cause intense joint pain that limits daily activities.',
            symptoms: ['Fever', 'Severe joint pain', 'Joint swelling', 'Muscle pain', 'Headache'],
            prevention: ['Use repellent and bed nets', 'Remove breeding sites', 'Keep patios and containers free of standing water'],
            when: ['Severe or prolonged joint pain', 'Persistent high fever', 'Dehydration or worsening symptoms'],
          },
        },
      };

      const state = {
        selected: 'DENGUE',
        compare: new Set(),
        cache: {},
      };

      function varText(v) {
        if (!v || v.pct === null || v.pct === undefined) return '—';
        const sign = (v.direction === 'baja') ? '-' : '';
        return sign + String(Math.abs(Number(v.pct) || 0)) + '%';
      }

      function varBadge(v) {
        if (!v || v.pct === null || v.pct === undefined) return '—';
        const dir = v.direction;
        const badge = dir === 'sube' ? '↑' : (dir === 'baja' ? '↓' : '•');
        const pctText = (dir === 'baja') ? ('-' + Math.abs(v.pct) + '%') : (v.pct + '%');
        return badge + ' ' + pctText;
      }

      async function loadDiseaseSummary(d) {
        if (state.cache[d]) return state.cache[d];
        const p = getParams();
        p.enfermedad = d;
        const res = await fetchJSON('/api/summary?' + new URLSearchParams(p).toString());
        state.cache[d] = res;
        return res;
      }

      function setActiveDisease(d) {
        state.selected = d;
        Array.from(document.querySelectorAll('#diseaseList .disease-item')).forEach(el => {
          el.classList.toggle('active', (el.getAttribute('data-disease') || '') === d);
        });
      }

      function fillList(id, items) {
        const el = document.getElementById(id);
        if (!el) return;
        el.innerHTML = '';
        (items || []).slice(0, 6).forEach(t => {
          const li = document.createElement('li');
          li.textContent = t;
          el.appendChild(li);
        });
      }

      function renderCompare() {
        const box = document.getElementById('compareChips');
        if (!box) return;
        box.innerHTML = '';
        const arr = Array.from(state.compare);
        if (!arr.length) {
          const m = document.createElement('div');
          m.className = 'muted';
          m.textContent = (getLang() === 'en') ? 'You have not selected diseases to compare yet.' : 'Aún no seleccionas enfermedades para comparar.';
          box.appendChild(m);
        } else {
          arr.forEach((d, idx) => {
            const chip = document.createElement('span');
            chip.className = 'chip';
            const dot = document.createElement('span');
            dot.style.width = '10px';
            dot.style.height = '10px';
            dot.style.borderRadius = '999px';
            dot.style.background = diseaseColor(d, idx);
            dot.style.display = 'inline-block';
            const s = document.createElement('strong');
            const lang = getLang();
            s.textContent = (diseaseInfo[d] && diseaseInfo[d][lang] ? diseaseInfo[d][lang].title : d);
            chip.appendChild(dot);
            chip.appendChild(s);
            chip.title = (getLang() === 'en') ? 'Click to remove' : 'Clic para quitar';
            chip.style.cursor = 'pointer';
            chip.addEventListener('click', function() {
              state.compare.delete(d);
              renderCompare();
            });
            box.appendChild(chip);
          });
        }
        const btn = document.getElementById('btnCompareGo');
        if (btn) btn.disabled = arr.length < 2;
      }

      async function renderDisease(d) {
        setActiveDisease(d);
        const lang = getLang();
        const info = (diseaseInfo[d] && diseaseInfo[d][lang]) ? diseaseInfo[d][lang] : null;
        const t = document.getElementById('dTitle');
        const lead = document.getElementById('dLead');
        if (t) t.textContent = info ? info.title : d;
        if (lead) lead.textContent = info ? info.lead : '';
        fillList('dSymptoms', info ? info.symptoms : []);
        fillList('dPrevention', info ? info.prevention : []);
        fillList('dWhen', info ? info.when : []);

        const viewBtn = document.getElementById('dBtnView');
        const addBtn = document.getElementById('dBtnAdd');
        if (viewBtn) viewBtn.onclick = function() { selectDiseaseAndGo(d); };
        if (addBtn) addBtn.onclick = function() {
          state.compare.add(d);
          renderCompare();
          toast((getLang() === 'en' ? 'Added: ' : 'Añadido: ') + (info ? info.title : d));
        };

        try {
          const sum = await loadDiseaseSummary(d);
          const cases = document.getElementById('dCases');
          const vEl = document.getElementById('dVar');
          const vDesc = document.getElementById('dVarDesc');
          const top = document.getElementById('dTopDept');
          if (cases) cases.textContent = fmt(sum.total_cases);
          if (vEl) vEl.textContent = varBadge(sum.variation);
          if (vDesc) vDesc.textContent = (sum.variation && sum.variation.from_year) ? (sum.variation.from_year + ' → ' + sum.variation.to_year) : 'Selecciona más de un año';
          if (top) top.textContent = sum.top_departamento || '—';
        } catch (e) {
          const cases = document.getElementById('dCases');
          if (cases) cases.textContent = '—';
        }
      }

      async function hydrateListStats() {
        const mapIds = {
          DENGUE: { c: 'statDengueCases', v: 'statDengueVar' },
          ZIKA: { c: 'statZikaCases', v: 'statZikaVar' },
          CHIKUNGUNYA: { c: 'statChikCases', v: 'statChikVar' },
        };
        for (const d of Object.keys(mapIds)) {
          try {
            const sum = await loadDiseaseSummary(d);
            const cases = document.getElementById(mapIds[d].c);
            const v = document.getElementById(mapIds[d].v);
            if (cases) cases.textContent = fmt(sum.total_cases);
            if (v) v.textContent = varText(sum.variation);
          } catch (e) {}
        }
      }

      Array.from(document.querySelectorAll('#diseaseList .disease-item')).forEach(el => {
        el.addEventListener('click', function() {
          const d = el.getAttribute('data-disease') || '';
          if (d) renderDisease(d);
        });
      });

      const search = document.getElementById('diseaseSearch');
      if (search) {
        search.addEventListener('input', function() {
          const q = normalize(search.value || '');
          Array.from(document.querySelectorAll('#diseaseList .disease-item')).forEach(el => {
            const d = normalize(el.getAttribute('data-disease') || '');
            const t = normalize(el.textContent || '');
            const ok = !q || d.includes(q) || t.includes(q);
            el.style.display = ok ? '' : 'none';
          });
        });
      }

      const btnGo = document.getElementById('btnCompareGo');
      if (btnGo) btnGo.addEventListener('click', function() {
        const arr = Array.from(state.compare);
        if (arr.length < 2) return;
        setDiseaseSelection(arr);
        location.hash = '#/dashboard';
        showPage('/dashboard');
        offset = 0;
        scheduleRefresh();
      });

      const btnClear = document.getElementById('btnCompareClear');
      if (btnClear) btnClear.addEventListener('click', function() { state.compare.clear(); renderCompare(); });

      renderCompare();
      hydrateListStats();
      renderDisease(state.selected);
    }

    function initRouting() {
      if (!location.hash) location.hash = '#/inicio';
      showPage(getRoute());
      window.addEventListener('hashchange', function() { showPage(getRoute()); });
      const go = document.getElementById('btnGoDash');
      if (go) go.addEventListener('click', function() { location.hash = '#/dashboard'; showPage('/dashboard'); });
      const goDis = document.getElementById('btnGoDiseases');
      if (goDis) goDis.addEventListener('click', function() { location.hash = '#/enfermedades'; showPage('/enfermedades'); });
    }

    function isHomeActive() {
      const el = document.getElementById('page-home');
      return !!(el && el.classList.contains('active'));
    }

    function setHomeChatVisible() {
      const chat = document.getElementById('homeChat');
      const mini = document.getElementById('homeChatMini');
      if (!chat || !mini) return;
      const open = localStorage.getItem('home_chat_open') === '1';
      if (!isHomeActive()) {
        chat.classList.remove('active');
        mini.classList.remove('active');
        return;
      }
      if (open) {
        chat.classList.add('active');
        mini.classList.remove('active');
      } else {
        chat.classList.remove('active');
        mini.classList.add('active');
      }
    }

    function addHomeMsg(kind, text) {
      const box = document.getElementById('homeChatBody');
      if (!box) return;
      const div = document.createElement('div');
      div.className = 'msg ' + (kind === 'user' ? 'user' : 'bot');
      div.textContent = text;
      box.appendChild(div);
      box.scrollTop = box.scrollHeight;
      return div;
    }

    function resetHomeChat() {
      homeChatMessages = [];
      const box = document.getElementById('homeChatBody');
      if (!box) return;
      box.innerHTML = '';
      const m = document.createElement('div');
      m.className = 'msg bot';
      m.textContent = (getLang() === 'en')
        ? 'Hi. I am your assistant. You can ask about health (dengue, zika, chikungunya) or about what you see in the data. What would you like to know?'
        : 'Hola. Soy tu asistente. Puedes preguntarme sobre salud (dengue, zika, chikungunya) o sobre lo que ves en los datos. ¿Qué te gustaría saber?';
      box.appendChild(m);
      box.scrollTop = box.scrollHeight;
    }

    async function askHome(question) {
      const q = String(question || '').trim();
      if (!q) return;
      addHomeMsg('user', q);
      homeChatMessages.push({ role: 'user', content: q });
      setStatus('Pensando…', true);
      const typing = addHomeMsg('bot', (getLang() === 'en') ? 'Typing…' : 'Escribiendo…');
      try {
        const body = {
          messages: homeChatMessages.slice(-16),
          params: getParams(),
        };
        const res = await fetch('/api/chat', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        });
        const txt = await res.text();
        let payload = null;
        try { payload = JSON.parse(txt); } catch (e) { payload = { answer: txt }; }
        const a = (payload && payload.answer) ? String(payload.answer) : 'No pude responder.';
        if (typing) typing.textContent = a;
        else addHomeMsg('bot', a);
        homeChatMessages.push({ role: 'assistant', content: a });
      } catch (e) {
        if (typing) typing.textContent = 'Ocurrió un error al responder. Intenta de nuevo.';
        else addHomeMsg('bot', 'Ocurrió un error al responder. Intenta de nuevo.');
      } finally {
        setStatus('Listo', false);
      }
    }

    function initHomeChat() {
      const openBtn = document.getElementById('homeChatOpen');
      const closeBtn = document.getElementById('homeChatClose');
      const resetBtn = document.getElementById('homeChatReset');
      const sendBtn = document.getElementById('homeAskSend');
      const input = document.getElementById('homeAskInput');
      if (openBtn) openBtn.addEventListener('click', function() { localStorage.setItem('home_chat_open', '1'); setHomeChatVisible(); });
      if (closeBtn) closeBtn.addEventListener('click', function() { localStorage.setItem('home_chat_open', '0'); setHomeChatVisible(); });
      if (resetBtn) resetBtn.addEventListener('click', function() { resetHomeChat(); });
      if (sendBtn) sendBtn.addEventListener('click', function() { askHome(input ? input.value : ''); if (input) input.value = ''; });
      if (input) input.addEventListener('keydown', function(e) { if (e.key === 'Enter') { e.preventDefault(); if (sendBtn) sendBtn.click(); } });
      Array.from(document.querySelectorAll('.qbtn')).forEach(b => {
        b.addEventListener('click', function() {
          const q = b.getAttribute('data-q') || '';
          askHome(q);
        });
      });
      resetHomeChat();
      setHomeChatVisible();
    }

    function initHomeFAQ() {
      const box = document.getElementById('homeFAQ');
      if (!box) return;
      const items = Array.from(box.querySelectorAll('details.faq'));
      items.forEach(det => {
        const summary = det.querySelector('summary');
        if (!summary) return;
        summary.addEventListener('click', function(e) {
          e.preventDefault();
          const wasOpen = det.hasAttribute('open');
          items.forEach(x => { if (x !== det) x.removeAttribute('open'); });
          if (wasOpen) det.removeAttribute('open');
          else det.setAttribute('open', '');
          det.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
      });
    }

    function initHomeActions() {
      Array.from(document.querySelectorAll('.feature[data-go]')).forEach(el => {
        const go = el.getAttribute('data-go') || '';
        const run = function() {
          if (go === 'dash-trend') {
            location.hash = '#/dashboard';
            showPage('/dashboard');
            setTimeout(function() {
              const t = document.getElementById('dashTrend');
              if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }, 240);
          } else if (go === 'dash-map') {
            location.hash = '#/dashboard';
            showPage('/dashboard');
            setTimeout(function() {
              const t = document.getElementById('dashMap');
              if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }, 240);
          } else if (go === 'learn') {
            location.hash = '#/enfermedades';
            showPage('/enfermedades');
          }
        };
        el.addEventListener('click', run);
        el.addEventListener('keydown', function(e) { if (e.key === 'Enter') run(); });
      });

      const more = document.getElementById('btnLearnMore');
      if (more) more.addEventListener('click', function() { location.hash = '#/enfermedades'; showPage('/enfermedades'); });
      const ideas = document.getElementById('btnAskIdeas');
      if (ideas) ideas.addEventListener('click', function() {
        localStorage.setItem('home_chat_open', '1');
        setHomeChatVisible();
        const chat = document.getElementById('homeChat');
        if (chat) chat.scrollIntoView({ behavior: 'smooth', block: 'start' });
        askHome((getLang() === 'en') ? 'help' : 'ayuda');
      });
    }

    function initPowerBIButton() {
      const btn = document.getElementById('btnPowerBI');
      if (!btn) return;
      btn.addEventListener('click', async function(e) {
        e.preventDefault();
        try {
          const opened = await fetchJSON('/api/open_powerbi');
          if (opened && opened.ok) {
            const msg = (getLang() === 'en')
              ? ('Opening: ' + (opened.opened || 'file'))
              : ('Abriendo: ' + (opened.opened || 'archivo'));
            toast(msg);
            return;
          }
          const st = await fetchJSON('/api/powerbi');
          if (st && st.available) {
            window.location.href = '/download_powerbi';
            return;
          }
          const msg = (getLang() === 'en')
            ? (I18N.en['powerbi.missing'] || 'Power BI file not available yet.')
            : (I18N.es['powerbi.missing'] || 'Aún no hay archivo de Power BI disponible.');
          toast(msg);
        } catch (err) {
          const msg = (getLang() === 'en')
            ? (I18N.en['powerbi.error'] || 'Could not check Power BI file.')
            : (I18N.es['powerbi.error'] || 'No se pudo verificar el archivo de Power BI.');
          toast(msg);
        }
      });
    }

    function initDashboardBIButton() {
      const btn = document.getElementById('btnDashboardBI');
      if (!btn) return;
      btn.addEventListener('click', function(e) {
        e.preventDefault();
        location.hash = '#/dashboardbi';
        showPage('/dashboardbi');
      });
    }

    let dashboardBIEmbedUrl = null;
    async function loadDashboardBIEmbed() {
      const frame = document.getElementById('dashboardBIFrame');
      const msg = document.getElementById('dashboardBIMessage');
      if (!frame || !msg) return;

      if (dashboardBIEmbedUrl) {
        msg.style.display = 'none';
        frame.style.display = 'block';
        if (frame.src !== dashboardBIEmbedUrl) frame.src = dashboardBIEmbedUrl;
        return;
      }

      try {
        const data = await fetchJSON('/api/dashboardbi_embed');
        const url = (data && data.embed_url) ? String(data.embed_url).trim() : '';
        if (!url) {
          frame.style.display = 'none';
          msg.style.display = 'block';
          msg.textContent = t('bi.embedMissing');
          return;
        }
        dashboardBIEmbedUrl = url;
        msg.style.display = 'none';
        frame.style.display = 'block';
        frame.src = dashboardBIEmbedUrl;
      } catch (e) {
        frame.style.display = 'none';
        msg.style.display = 'block';
        msg.textContent = t('bi.embedError');
      }
    }

    qs('btnApply').addEventListener('click', function() { scheduleRefresh(); });
    qs('btnReset').addEventListener('click', function() { resetAll(); scheduleRefresh(); });
    qs('btnPDF').addEventListener('click', function() { window.print(); });
    qs('btnAsk').addEventListener('click', function() { ask().catch(e => toast(e.message || String(e))); });
    qs('askInput').addEventListener('keydown', function(e) { if (e.key === 'Enter') qs('btnAsk').click(); });

    ['q','anoMin','anoMax','departamento','enfermedades'].forEach(id => {
      qs(id).addEventListener('change', scheduleRefresh);
      if (id === 'q') qs(id).addEventListener('input', scheduleRefresh);
    });

    qs('btnPrev').addEventListener('click', async function() {
      const limit = Math.max(1, Math.min(500, parseInt(qs('limit').value || '50', 10)));
      offset = Math.max(0, offset - limit);
      await loadTable();
    });
    qs('btnNext').addEventListener('click', async function() {
      const limit = Math.max(1, Math.min(500, parseInt(qs('limit').value || '50', 10)));
      offset = offset + limit;
      await loadTable();
    });
    qs('limit').addEventListener('change', function() { offset = 0; scheduleRefresh(); });

    qs('btnTheme').addEventListener('click', function() {
      const cur = document.body.getAttribute('data-theme') || 'light';
      setTheme(cur === 'dark' ? 'light' : 'dark');
      buildMap();
      scheduleRefresh();
    });

    window.addEventListener('resize', debounce(function() { setLayoutVars(); scheduleRefresh(); }, 220));

    (async function() {
      try {
        initTheme();
        initLang();
        initPowerBIButton();
        initDashboardBIButton();
        setLayoutVars();
        initRouting();
        initHomeFAQ();
        initDiseasePage();
        initHomeActions();
        initHomeChat();
        setStatus('Cargando…', true);
        await loadMeta();
        await loadValues();
        await loadMap();
        updateExportLink();
        await refreshAll();
      } catch (e) {
        setStatus('Error', false);
        toast(e.message || String(e));
      }
    })();
