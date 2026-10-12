// Minimal stand-in for the dashboard runtime so the same charts run as a
// static page on GitHub Pages. Data comes from data.js (the gold tables).
(function () {
  const D = window.AR_DATA || {};
  const calcs = {};
  const listeners = [];
  window.dash = {
    colors: ['var(--ar-c1)', 'var(--ar-c2)', 'var(--ar-c3)', 'var(--ar-c4)'],
    data(id) {
      if (id in calcs) return { status: 'ok', data: calcs[id] };
      const rows = D[id];
      if (!rows) return { status: 'missing', data: [] };
      return { status: 'ok', data: rows, columns: Object.keys(rows[0] || {}) };
    },
    calc(id, o) { calcs[id] = o.fn(...o.inputs.map((i) => D[i] || [])); },
    loader() {},
    onData(fn) { listeners.push(fn); fn(); },
    refresh() {}
  };
  window.__redraw = () => listeners.forEach((f) => f());
  let t;
  window.addEventListener('resize', () => { clearTimeout(t); t = setTimeout(window.__redraw, 120); });
})();
