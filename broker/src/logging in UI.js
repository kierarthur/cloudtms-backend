(() => {
  if (window.__CLOUDTMS_TS_TRACE__?.stop) {
    try { window.__CLOUDTMS_TS_TRACE__.stop(); } catch {}
  }

  const TRACE_ID = `ts-trace-${new Date().toISOString()}`;
  const startedAt = Date.now();
  const maxBodyChars = 12000;
  const maxEvents = 2500;
  const uuidRe = /[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/ig;

  const state = {
    trace_id: TRACE_ID,
    started_at: new Date(startedAt).toISOString(),
    page_url: location.href,
    user_agent: navigator.userAgent,
    paused: false,
    events: [],
    ids: {
      timesheet_ids: new Set(),
      contract_week_ids: new Set(),
      candidate_ids: new Set(),
      session_ids: new Set(),
      batch_ids: new Set(),
      other_uuids: new Set()
    },
    counters: {}
  };

  const original = {
    fetch: window.fetch,
    xhrOpen: XMLHttpRequest.prototype.open,
    xhrSend: XMLHttpRequest.prototype.send,
    consoleLog: console.log,
    consoleInfo: console.info,
    consoleWarn: console.warn,
    consoleError: console.error,
    consoleDebug: console.debug
  };

  const nowIso = () => new Date().toISOString();
  const elapsedMs = () => Math.max(0, Date.now() - startedAt);
  const inc = (key) => { state.counters[key] = (state.counters[key] || 0) + 1; };

  const safeClone = (value, depth = 0, seen = new WeakSet()) => {
    try {
      if (value == null) return value;
      if (typeof value === 'string') return value.length > maxBodyChars ? value.slice(0, maxBodyChars) + '[truncated]' : value;
      if (typeof value === 'number' || typeof value === 'boolean') return value;
      if (typeof value === 'bigint') return String(value);
      if (typeof value === 'function') return `[function ${value.name || 'anonymous'}]`;
      if (value instanceof Error) return { name: value.name, message: value.message, stack: value.stack };
      if (value instanceof Event) return { type: value.type, target: describeNode(value.target) };
      if (value instanceof Element) return describeNode(value);
      if (depth >= 4) return '[depth-limit]';
      if (typeof value === 'object') {
        if (seen.has(value)) return '[circular]';
        seen.add(value);
      }
      if (Array.isArray(value)) return value.slice(0, 50).map(v => safeClone(v, depth + 1, seen));
      const out = {};
      for (const [k, v] of Object.entries(value).slice(0, 80)) out[k] = safeClone(v, depth + 1, seen);
      return out;
    } catch (e) {
      return `[unserialisable: ${e?.message || e}]`;
    }
  };

  const redact = (value) => {
    let text = typeof value === 'string' ? value : JSON.stringify(safeClone(value));
    text = text || '';
    text = text.replace(/(authorization["']?\s*[:=]\s*["']?bearer\s+)[^"',\s}]+/ig, '$1[redacted]');
    text = text.replace(/(apikey["']?\s*[:=]\s*["']?)[^"',\s}]+/ig, '$1[redacted]');
    text = text.replace(/(access_token["']?\s*[:=]\s*["']?)[^"',\s}]+/ig, '$1[redacted]');
    text = text.replace(/(refresh_token["']?\s*[:=]\s*["']?)[^"',\s}]+/ig, '$1[redacted]');
    text = text.replace(/(password["']?\s*[:=]\s*["']?)[^"',\s}]+/ig, '$1[redacted]');
    text = text.replace(/(account_number["']?\s*[:=]\s*["']?)[0-9 -]{4,}/ig, '$1[redacted]');
    text = text.replace(/(sort_code["']?\s*[:=]\s*["']?)[0-9 -]{4,}/ig, '$1[redacted]');
    if (text.length > maxBodyChars) text = text.slice(0, maxBodyChars) + '[truncated]';
    try { return JSON.parse(text); } catch { return text; }
  };

  const describeNode = (node) => {
    try {
      if (!node) return null;
      if (node.nodeType !== 1) return { nodeType: node.nodeType, text: String(node.textContent || '').slice(0, 200) };
      const el = node;
      const attrs = {};
      for (const name of ['id', 'class', 'role', 'type', 'name', 'aria-label', 'title', 'data-testid', 'data-test', 'data-action', 'href']) {
        const v = el.getAttribute?.(name);
        if (v) attrs[name] = v;
      }
      return {
        tag: el.tagName,
        attrs,
        text: String(el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim().slice(0, 300)
      };
    } catch (e) {
      return { error: String(e?.message || e) };
    }
  };

  const collectIds = (source) => {
    try {
      const text = typeof source === 'string' ? source : JSON.stringify(safeClone(source));
      const ids = String(text || '').match(uuidRe) || [];
      for (const idRaw of ids) {
        const id = idRaw.toLowerCase();
        state.ids.other_uuids.add(id);
        const around = String(text).slice(Math.max(0, String(text).toLowerCase().indexOf(id) - 80), String(text).toLowerCase().indexOf(id) + 120).toLowerCase();
        if (/timesheet/.test(around)) state.ids.timesheet_ids.add(id);
        if (/contract[_ -]?week/.test(around)) state.ids.contract_week_ids.add(id);
        if (/candidate/.test(around)) state.ids.candidate_ids.add(id);
        if (/session|workbench/.test(around)) state.ids.session_ids.add(id);
        if (/batch/.test(around)) state.ids.batch_ids.add(id);
      }
    } catch {}
  };

  const addEvent = (type, detail = {}, severity = 'info') => {
    if (state.paused) return;
    try {
      const ev = {
        n: state.events.length + 1,
        t: nowIso(),
        ms: elapsedMs(),
        type,
        severity,
        url: location.href,
        detail: redact(detail)
      };
      state.events.push(ev);
      if (state.events.length > maxEvents) state.events.shift();
      inc(type);
      collectIds(ev);
      render();
    } catch (e) {
      try { original.consoleWarn('[TRACE_ADD_EVENT_FAILED]', e); } catch {}
    }
  };

  const headersToObject = (headers) => {
    const out = {};
    try {
      if (!headers) return out;
      if (headers instanceof Headers) {
        headers.forEach((v, k) => out[k] = /authorization|apikey|cookie/i.test(k) ? '[redacted]' : v);
      } else if (Array.isArray(headers)) {
        for (const [k, v] of headers) out[k] = /authorization|apikey|cookie/i.test(k) ? '[redacted]' : v;
      } else if (typeof headers === 'object') {
        for (const [k, v] of Object.entries(headers)) out[k] = /authorization|apikey|cookie/i.test(k) ? '[redacted]' : v;
      }
    } catch {}
    return out;
  };

  const bodyPreview = async (body) => {
    try {
      if (body == null) return null;
      if (typeof body === 'string') return redact(body);
      if (body instanceof FormData) {
        const out = {};
        for (const [k, v] of body.entries()) out[k] = v instanceof File ? `[File ${v.name} ${v.size} bytes]` : String(v).slice(0, 2000);
        return redact(out);
      }
      if (body instanceof URLSearchParams) return redact(String(body));
      if (body instanceof Blob) return `[Blob ${body.type || 'unknown'} ${body.size} bytes]`;
      return redact(body);
    } catch (e) {
      return `[body-preview-failed: ${e?.message || e}]`;
    }
  };

  window.fetch = async function tracedFetch(input, init = {}) {
    const start = performance.now();
    const method = String(init?.method || (input && input.method) || 'GET').toUpperCase();
    const url = String(input?.url || input || '');
    const requestHeaders = headersToObject(init?.headers || input?.headers);
    const requestBody = await bodyPreview(init?.body);
    const requestId = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

    addEvent('fetch:start', { requestId, method, url, requestHeaders, requestBody });

    try {
      const res = await original.fetch.apply(this, arguments);
      const ms = Math.round(performance.now() - start);
      let responseBody = null;
      const contentType = res.headers?.get?.('content-type') || '';
      try {
        const clone = res.clone();
        if (/json|text|javascript|xml|html/i.test(contentType)) {
          responseBody = await clone.text();
          responseBody = redact(responseBody);
        } else {
          responseBody = `[${contentType || 'non-text'} response not captured]`;
        }
      } catch (e) {
        responseBody = `[response capture failed: ${e?.message || e}]`;
      }
      addEvent('fetch:end', {
        requestId,
        method,
        url,
        status: res.status,
        ok: res.ok,
        ms,
        responseHeaders: headersToObject(res.headers),
        responseBody
      }, res.ok ? 'info' : 'warn');
      return res;
    } catch (e) {
      addEvent('fetch:error', { requestId, method, url, ms: Math.round(performance.now() - start), error: safeClone(e) }, 'error');
      throw e;
    }
  };

  XMLHttpRequest.prototype.open = function tracedOpen(method, url) {
    this.__trace = { method: String(method || 'GET').toUpperCase(), url: String(url || ''), start: null, requestId: `${Date.now()}-${Math.random().toString(16).slice(2)}` };
    return original.xhrOpen.apply(this, arguments);
  };

  XMLHttpRequest.prototype.send = function tracedSend(body) {
    const trace = this.__trace || { method: 'GET', url: '[unknown]', requestId: `${Date.now()}-${Math.random().toString(16).slice(2)}` };
    trace.start = performance.now();
    bodyPreview(body).then((requestBody) => addEvent('xhr:start', { ...trace, requestBody }));
    this.addEventListener('loadend', () => {
      let responseBody = null;
      try {
        const ct = this.getResponseHeader('content-type') || '';
        responseBody = /json|text|javascript|xml|html/i.test(ct) ? redact(this.responseText) : `[${ct || 'non-text'} response not captured]`;
      } catch (e) {
        responseBody = `[xhr response capture failed: ${e?.message || e}]`;
      }
      addEvent('xhr:end', {
        requestId: trace.requestId,
        method: trace.method,
        url: trace.url,
        status: this.status,
        ok: this.status >= 200 && this.status < 300,
        ms: Math.round(performance.now() - trace.start),
        responseBody
      }, this.status >= 200 && this.status < 300 ? 'info' : 'warn');
    });
    return original.xhrSend.apply(this, arguments);
  };

  for (const level of ['log', 'info', 'warn', 'error', 'debug']) {
    console[level] = function tracedConsole(...args) {
      try {
        addEvent(`console:${level}`, { args: args.map(a => safeClone(a)) }, level === 'error' ? 'error' : level === 'warn' ? 'warn' : 'info');
      } catch {}
      return original[`console${level[0].toUpperCase()}${level.slice(1)}`].apply(console, args);
    };
  }

  const clickHandler = (ev) => {
    const path = ev.composedPath?.() || [];
    const target = path.find(n => n && n.nodeType === 1 && /^(BUTTON|A|INPUT|TEXTAREA|SELECT|DIV|SPAN)$/i.test(n.tagName)) || ev.target;
    const desc = describeNode(target);
    const text = String(desc?.text || '').toLowerCase();
    const interesting = /authorise|authorize|unauthorise|unauthorize|save|edit|timesheet|banking|pay|refresh|close|ok|confirm|cancel/.test(text + ' ' + JSON.stringify(desc?.attrs || {}));
    addEvent(interesting ? 'ui:click:important' : 'ui:click', { target: desc, x: ev.clientX, y: ev.clientY }, interesting ? 'warn' : 'info');
  };
  document.addEventListener('click', clickHandler, true);

  const inputHandler = (ev) => {
    const el = ev.target;
    if (!el || el.nodeType !== 1) return;
    const desc = describeNode(el);
    const value = el.type === 'password' ? '[redacted]' : String(el.value ?? '').slice(0, 500);
    addEvent(`ui:${ev.type}`, { target: desc, value });
  };
  document.addEventListener('change', inputHandler, true);
  document.addEventListener('input', inputHandler, true);
  document.addEventListener('submit', (ev) => addEvent('ui:submit', { target: describeNode(ev.target) }, 'warn'), true);

  const errorHandler = (ev) => addEvent('window:error', { message: ev.message, filename: ev.filename, lineno: ev.lineno, colno: ev.colno, error: safeClone(ev.error) }, 'error');
  const rejectionHandler = (ev) => addEvent('window:unhandledrejection', { reason: safeClone(ev.reason) }, 'error');
  window.addEventListener('error', errorHandler);
  window.addEventListener('unhandledrejection', rejectionHandler);

  const mo = new MutationObserver((mutations) => {
    try {
      const interesting = [];
      for (const m of mutations.slice(0, 40)) {
        for (const n of Array.from(m.addedNodes || []).slice(0, 10)) {
          if (n.nodeType !== 1) continue;
          const txt = String(n.innerText || n.textContent || '').replace(/\s+/g, ' ').trim();
          if (/authorise|authorize|unauthorise|unauthorize|saved|error|failed|warning|timesheet|banking|pay|refresh|stale|signature|modal|loading/i.test(txt)) {
            interesting.push(describeNode(n));
          }
        }
      }
      if (interesting.length) addEvent('dom:interesting-added', { nodes: interesting.slice(0, 10) }, 'warn');
    } catch {}
  });
  mo.observe(document.documentElement, { childList: true, subtree: true });

  const popup = (() => {
    try {
      const w = window.open('', `CloudTMS_${TRACE_ID}`, 'width=980,height=720,left=60,top=60,resizable=yes,scrollbars=yes');
      if (!w) return null;
      w.document.open();
      w.document.write(`<!doctype html><html><head><title>CloudTMS Trace</title><style>
        body{font-family:Arial,sans-serif;margin:12px;background:#101214;color:#e8e8e8}
        button{margin:4px;padding:8px 10px;border-radius:6px;border:1px solid #555;background:#222;color:#fff;cursor:pointer}
        button:hover{background:#333}
        .row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
        .stat{font-size:12px;color:#ccc;margin:4px 0}
        pre{white-space:pre-wrap;word-break:break-word;background:#050607;border:1px solid #333;padding:10px;border-radius:8px;max-height:560px;overflow:auto}
        .warn{color:#ffd166}.error{color:#ff6b6b}.info{color:#9be7ff}
      </style></head><body>
        <h2>CloudTMS frontend tracer</h2>
        <div class="row">
          <button id="dump">Dump to clipboard</button>
          <button id="pause">Pause</button>
          <button id="clear">Clear</button>
          <button id="close">Stop tracer</button>
        </div>
        <div id="status" class="stat"></div>
        <pre id="out"></pre>
      </body></html>`);
      w.document.close();
      return w;
    } catch { return null; }
  })();

  const makeDump = () => {
    const ids = {};
    for (const [k, set] of Object.entries(state.ids)) ids[k] = Array.from(set).sort();
    const performanceEntries = performance.getEntriesByType?.('resource')
      ?.filter(e => /supabase|rest\/v1|rpc|timesheet|tsfin|banking|pay|workbench/i.test(e.name))
      ?.slice(-100)
      ?.map(e => ({
        name: e.name,
        initiatorType: e.initiatorType,
        startTime: Math.round(e.startTime),
        duration: Math.round(e.duration),
        transferSize: e.transferSize,
        encodedBodySize: e.encodedBodySize
      })) || [];
    return {
      trace_id: state.trace_id,
      started_at: state.started_at,
      dumped_at: nowIso(),
      elapsed_ms: elapsedMs(),
      page_url: location.href,
      ids,
      counters: state.counters,
      event_count: state.events.length,
      performance_entries: performanceEntries,
      events: state.events
    };
  };

  const dumpText = () => JSON.stringify(makeDump(), null, 2);

  const render = () => {
    if (!popup || popup.closed) return;
    try {
      const ids = {};
      for (const [k, set] of Object.entries(state.ids)) ids[k] = set.size;
      popup.document.getElementById('status').textContent =
        `${state.events.length} events | ${elapsedMs()}ms | paused=${state.paused} | ids=${JSON.stringify(ids)} | url=${location.href}`;
      const tail = state.events.slice(-80).map(e => `[${e.n}] ${e.t} ${e.type} ${e.severity}\n${JSON.stringify(e.detail, null, 2)}`).join('\n\n');
      popup.document.getElementById('out').textContent = tail || 'Tracing... perform Authorise / Unauthorise / Edit / Save, then click Dump to clipboard.';
    } catch {}
  };

  if (popup && !popup.closed) {
    popup.document.getElementById('dump').onclick = async () => {
      const text = dumpText();
      try {
        await navigator.clipboard.writeText(text);
        popup.document.getElementById('status').textContent = `Copied ${text.length} chars to clipboard at ${nowIso()}`;
      } catch (e) {
        popup.document.getElementById('out').textContent = text;
        popup.document.getElementById('status').textContent = `Clipboard failed; select/copy text below. ${e?.message || e}`;
      }
    };
    popup.document.getElementById('pause').onclick = () => {
      state.paused = !state.paused;
      popup.document.getElementById('pause').textContent = state.paused ? 'Resume' : 'Pause';
      render();
    };
    popup.document.getElementById('clear').onclick = () => {
      state.events = [];
      state.counters = {};
      for (const set of Object.values(state.ids)) set.clear();
      addEvent('trace:cleared', {});
      render();
    };
    popup.document.getElementById('close').onclick = () => {
      window.__CLOUDTMS_TS_TRACE__.stop();
      try { popup.close(); } catch {}
    };
  }

  const stop = () => {
    try { window.fetch = original.fetch; } catch {}
    try { XMLHttpRequest.prototype.open = original.xhrOpen; } catch {}
    try { XMLHttpRequest.prototype.send = original.xhrSend; } catch {}
    try { console.log = original.consoleLog; console.info = original.consoleInfo; console.warn = original.consoleWarn; console.error = original.consoleError; console.debug = original.consoleDebug; } catch {}
    try { document.removeEventListener('click', clickHandler, true); } catch {}
    try { document.removeEventListener('change', inputHandler, true); } catch {}
    try { document.removeEventListener('input', inputHandler, true); } catch {}
    try { window.removeEventListener('error', errorHandler); } catch {}
    try { window.removeEventListener('unhandledrejection', rejectionHandler); } catch {}
    try { mo.disconnect(); } catch {}
    addEvent('trace:stopped', {});
  };

  window.__CLOUDTMS_TS_TRACE__ = {
    state,
    dump: makeDump,
    dumpText,
    stop,
    addEvent,
    copy: async () => {
      const text = dumpText();
      await navigator.clipboard.writeText(text);
      return { copied_chars: text.length, event_count: state.events.length };
    }
  };

  addEvent('trace:started', { trace_id: TRACE_ID, page_url: location.href });
  render();

  console.info('[CloudTMS tracer installed]', {
    trace_id: TRACE_ID,
    instructions: 'Perform Authorise / Unauthorise / Edit / Save. Then click Dump to clipboard in the floating window, or run: await __CLOUDTMS_TS_TRACE__.copy()'
  });
})();