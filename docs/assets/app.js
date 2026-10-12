(function () {
  const fmt = d3.format(',');
  const pct = d3.format('.1%');
  const pct0 = d3.format('.0%');
  const one = d3.format('.1f');
  const C = dash.colors;
  const enc = (s) => String(s).replace(/%/g, '%25').replace(/&/g, '%26').replace(/=/g, '%3D');
  const drawn = {};
  // A chart animates on its first draws only: redraws within the opening window
  // (e.g. once the font loads) replay the entrance instead of cutting it short.
  const fresh = (id) => {
    const now = performance.now();
    if (!(id in drawn)) drawn[id] = now;
    return now - drawn[id] < 1500;
  };
  const calm = () => window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  const counted = new WeakSet();
  // Counts a number up to its value on first load; the final text is always the formatted value.
  function countUp(el, value, format) {
    const finalText = format(value);
    if (calm() || counted.has(el) || !isFinite(value)) { el.textContent = finalText; counted.add(el); return; }
    counted.add(el);
    const dur = 1100, t0 = performance.now(), ease = d3.easeCubicOut;
    const step = (t) => {
      const p = Math.min(1, (t - t0) / dur);
      el.textContent = p < 1 ? format(Math.round(value * ease(p) * 1000) / 1000) : finalText;
      if (p < 1) requestAnimationFrame(step);
    };
    requestAnimationFrame(step);
  }

  dash.calc('categorized_share', {
    inputs: ['kpis'],
    title: 'Share matched to an issue',
    description: 'Posts that hit at least one issue keyword, divided by all posts analysed.',
    fn: (kpis) => [{ share: Number(kpis[0].categorized_posts) / Number(kpis[0].posts) }]
  });

  // ---------- shared helpers ----------
  function prep(el, d) {
    el.querySelectorAll(':scope > svg, :scope > .ar-fail, :scope > .ar-tip').forEach((n) => n.remove());
    if (d.status === 'ok') { el.classList.remove('dash-skeleton'); return true; }
    if (d.status === 'loading') { el.classList.add('dash-skeleton'); return false; }
    el.classList.remove('dash-skeleton');
    const f = document.createElement('div');
    f.className = 'ar-fail';
    f.style.minHeight = el.style.minHeight;
    f.append(document.createElement('i'), document.createTextNode('Failed to load'));
    el.append(f);
    return false;
  }

  function makeTip(el) {
    const tip = document.createElement('div');
    tip.className = 'ar-tip';
    el.append(tip);
    return {
      show(event, title, lines) {
        tip.replaceChildren();
        const t = document.createElement('div');
        t.className = 't';
        t.textContent = title;
        tip.append(t);
        lines.forEach(([color, text]) => {
          const row = document.createElement('div');
          if (color) {
            const sw = document.createElement('span');
            sw.className = 'sw';
            sw.style.background = color;
            row.append(sw);
          }
          row.append(document.createTextNode(text));
          tip.append(row);
        });
        tip.style.display = 'block';
        const [px, py] = d3.pointer(event, el);
        const w = tip.offsetWidth, h = tip.offsetHeight, W = el.clientWidth;
        let x = px + 12;
        if (x + w > W) x = Math.max(0, px - w - 12);
        let y = py - h - 8;
        if (y < 0) y = py + 14;
        tip.style.left = x + 'px';
        tip.style.top = y + 'px';
      },
      hide() { tip.style.display = 'none'; }
    };
  }

  function svgIn(el, height) {
    const w = Math.max(240, el.clientWidth);
    const svg = d3.select(el).append('svg')
      .attr('width', '100%').attr('height', height)
      .attr('viewBox', `0 0 ${w} ${height}`)
      .attr('preserveAspectRatio', 'xMinYMin meet');
    return { svg, w };
  }

  function textWidth(svg, strings) {
    const g = svg.append('g').attr('visibility', 'hidden');
    let m = 0;
    strings.forEach((s) => {
      const t = g.append('text').style('font-size', 'var(--cds-font-size-caption)').text(s);
      m = Math.max(m, t.node().getComputedTextLength());
    });
    g.remove();
    return m;
  }

  // Horizontal (optionally stacked) bar chart.
  function hbar(el, id, rows, o) {
    const rowH = 28, barH = 18;
    const height = rows.length * rowH + 4;
    const { svg, w } = svgIn(el, height);
    const ml = textWidth(svg, rows.map(o.label)) + 10;
    const mr = textWidth(svg, rows.map((r) => o.endLabel(r))) + 10;
    const total = (r) => o.segs.reduce((a, s) => a + s.value(r), 0);
    const x = d3.scaleLinear([0, d3.max(rows, total) || 1], [0, Math.max(20, w - ml - mr)]);
    const tip = makeTip(el);
    const animate = fresh(id);

    const g = svg.selectAll('g.row').data(rows).join('g').attr('class', 'row')
      .attr('transform', (_, i) => `translate(0,${i * rowH + 2})`);
    g.append('text').attr('x', ml - 10).attr('y', rowH / 2 - 2).attr('dy', '0.35em')
      .attr('text-anchor', 'end').style('font-size', 'var(--cds-font-size-caption)')
      .style('fill', 'var(--color-fg)').text(o.label);
    o.segs.forEach((s, si) => {
      g.append('rect')
        .attr('class', animate ? 'ar-grow' : null)
        .style('transform-box', 'fill-box')
        .style('animation-delay', (_, i) => `${i * 40 + si * 140}ms`)
        .attr('x', (r) => ml + x(o.segs.slice(0, si).reduce((a, p) => a + p.value(r), 0)))
        .attr('y', (rowH - barH) / 2 - 2)
        .attr('height', barH)
        .attr('width', (r) => Math.max(0, x(s.value(r)) - (si < o.segs.length - 1 && s.value(r) > 0 ? 1 : 0)))
        .attr('rx', 4)
        .style('fill', (r) => (s.color ? s.color(r) : C[si]));
    });
    g.append('text').attr('x', (r) => ml + x(total(r)) + 6).attr('y', rowH / 2 - 2).attr('dy', '0.35em')
      .style('font-size', 'var(--cds-font-size-caption)').style('font-variant-numeric', 'tabular-nums')
      .style('fill', 'var(--color-fg-muted)').text((r) => o.endLabel(r))
      .attr('class', animate ? 'ar-fadein' : null)
      .style('animation-delay', (_, i) => `${i * 40 + 420}ms`);
    g.append('rect').attr('x', 0).attr('y', 0).attr('width', w).attr('height', rowH).style('fill', 'transparent')
      .on('mousemove', (ev, r) => { g.style('opacity', (q) => (q === r ? 1 : 0.4)); tip.show(ev, o.label(r), o.tip(r)); })
      .on('mouseleave', () => { g.style('opacity', 1); tip.hide(); });
  }

  // ---------- takeaway ----------
  function mark(tag, source, field, where, text) {
    const b = document.createElement(tag);
    b.setAttribute('data-source', source);
    b.setAttribute('data-field', field);
    b.setAttribute('data-where', where);
    b.textContent = text;
    return b;
  }
  function drawTakeaway(issues, models) {
    const p = document.getElementById('takeaway');
    if (issues.status !== 'ok' || models.status !== 'ok') {
      p.replaceChildren();
      const s = document.createElement('span');
      s.className = issues.status === 'loading' || models.status === 'loading' ? 'dash-skeleton' : '';
      s.textContent = issues.status === 'loading' || models.status === 'loading' ? ' ' : '—';
      p.append(s);
      return;
    }
    const rows = issues.data;
    const top = rows.reduce((a, r) => (Number(r.mentions) > Number(a.mentions) ? r : a));
    const eng = rows.filter((r) => Number(r.mentions) >= 30)
      .reduce((a, r) => (Number(r.avg_comments) > Number(a.avg_comments) ? r : a));
    const m = models.data.reduce((a, r) => (Number(r.mentions) > Number(a.mentions) ? r : a));
    const wI = 'issue=' + enc(top.issue), wE = 'issue=' + enc(eng.issue), wM = 'car_model=' + enc(m.car_model);
    p.replaceChildren(
      mark('b', 'top_issues', 'issue', wI, top.issue),
      document.createTextNode(' problems come up most, in '),
      mark('b', 'top_issues', 'mentions', wI, fmt(Number(top.mentions))),
      document.createTextNode(' posts. '),
      mark('b', 'top_issues', 'issue', wE, eng.issue),
      document.createTextNode(' threads pull the most replies, '),
      mark('b', 'top_issues', 'avg_comments', wE, one(Number(eng.avg_comments))),
      document.createTextNode(' comments per post on average. The '),
      mark('b', 'top_models', 'car_model', wM, m.car_model),
      document.createTextNode(' is the most discussed model ('),
      mark('span', 'top_models', 'mentions', wM, fmt(Number(m.mentions))),
      document.createTextNode(' posts).')
    );
  }

  // ---------- KPI tiles ----------
  function drawKpis(k, share) {
    document.querySelectorAll('#kpis [data-source="kpis"]').forEach((el) => {
      if (k.status === 'ok') {
        el.classList.remove('dash-skeleton');
        if (el.classList.contains('num')) countUp(el, Number(k.data[0][el.dataset.field]), (v) => fmt(Math.round(v)));
        else el.textContent = fmt(Number(k.data[0][el.dataset.field]));
      } else {
        el.textContent = '—';
        el.classList.toggle('dash-skeleton', k.status === 'loading' && el.classList.contains('num'));
      }
    });
    const s = document.querySelector('#kpis [data-source="categorized_share"]');
    if (share.status === 'ok' && share.data.length) {
      s.classList.remove('dash-skeleton');
      countUp(s, share.data[0].share, pct);
    } else {
      s.textContent = '—';
      s.classList.toggle('dash-skeleton', share.status === 'loading');
    }
    document.querySelectorAll('#method [data-source="kpis"]').forEach((el) => {
      el.textContent = k.status === 'ok' ? String(k.data[0][el.dataset.field]) : '—';
    });
  }

  // ---------- issue charts ----------
  function drawIssues(d) {
    const el = document.getElementById('c-issues');
    if (!prep(el, d)) return;
    const rows = d.data.slice().sort((a, b) => Number(b.mentions) - Number(a.mentions));
    hbar(el, 'issues', rows, {
      label: (r) => r.issue,
      endLabel: (r) => fmt(Number(r.mentions)),
      segs: [{ value: (r) => Number(r.mentions), color: () => C[0] }],
      tip: (r) => [[null, `${fmt(Number(r.mentions))} posts`], [null, `${fmt(Number(r.total_comments))} comments in total`]]
    });
  }

  function drawEngage(d) {
    const el = document.getElementById('c-engage');
    if (!prep(el, d)) return;
    const rows = d.data.slice().sort((a, b) => Number(b.avg_comments) - Number(a.avg_comments));
    const best = rows[0];
    hbar(el, 'engage', rows, {
      label: (r) => r.issue,
      endLabel: (r) => one(Number(r.avg_comments)),
      segs: [{ value: (r) => Number(r.avg_comments), color: (r) => (r === best ? C[0] : 'var(--cds-chart-muted)') }],
      tip: (r) => [
        [null, `${one(Number(r.avg_comments))} comments per post on average`],
        [null, `Median ${one(Number(r.median_comments))}, across ${fmt(Number(r.mentions))} posts`],
        [null, `Average score ${one(Number(r.avg_score))}`]
      ]
    });
  }

  // ---------- models ----------
  function drawModels(d) {
    const el = document.getElementById('c-models');
    const lg = document.getElementById('l-models');
    lg.replaceChildren();
    if (!prep(el, d)) return;
    [['Named in the post', C[0]], ['From a model subreddit', C[1]]].forEach(([t, c]) => {
      const s = document.createElement('span');
      const sw = document.createElement('i');
      sw.className = 'sw';
      sw.style.background = c;
      s.append(sw, document.createTextNode(t));
      lg.append(s);
    });
    const rows = d.data.slice().sort((a, b) => Number(b.mentions) - Number(a.mentions)).slice(0, 15);
    hbar(el, 'models', rows, {
      label: (r) => r.car_model,
      endLabel: (r) => fmt(Number(r.mentions)),
      segs: [
        { value: (r) => Number(r.named_in_post), color: () => C[0] },
        { value: (r) => Number(r.from_subreddit), color: () => C[1] }
      ],
      tip: (r) => [
        [C[0], `Named in the post: ${fmt(Number(r.named_in_post))}`],
        [C[1], `From a model subreddit: ${fmt(Number(r.from_subreddit))}`],
        [null, `${one(Number(r.avg_comments))} comments per post on average`]
      ]
    });
  }

  // ---------- issue mix heatmap ----------
  function drawMix(d) {
    const el = document.getElementById('c-mix');
    if (!prep(el, d)) return;
    const rows = d.data.map((r) => ({ ...r, share: Number(r.share_of_posts), mentions: Number(r.mentions), posts: Number(r.posts) }));
    const weeks = Array.from(new Set(rows.map((r) => r.week))).sort();
    const tot = d3.rollup(rows, (v) => d3.sum(v, (r) => r.mentions), (r) => r.issue);
    const issues = Array.from(tot.keys()).sort((a, b) => tot.get(b) - tot.get(a));
    const byKey = new Map(rows.map((r) => [r.week + '|' + r.issue, r]));
    const cellH = 26, top = 34;
    const height = top + issues.length * cellH + 2;
    const { svg, w } = svgIn(el, height);
    const ml = textWidth(svg, issues) + 10;
    const x = d3.scaleBand(weeks, [ml, w]).paddingInner(0.06);
    const maxS = d3.max(rows, (r) => r.share) || 1;
    const op = (s) => 0.08 + 0.92 * (s / maxS);
    const wf = d3.utcFormat('%b %-d');
    const tip = makeTip(el);

    const step = textWidth(svg, ['Sep 15']) + 8 > x.bandwidth() ? 2 : 1;
    svg.selectAll('text.wk').data(weeks).join('text').attr('class', 'wk')
      .attr('x', (wk) => x(wk) + x.bandwidth() / 2).attr('y', 14).attr('text-anchor', 'middle')
      .style('font-size', 'var(--cds-font-size-caption)').style('fill', 'var(--color-fg-muted)')
      .text((wk, i) => (i % step === 0 ? wf(new Date(wk + 'T00:00:00Z')) : ''));
    svg.append('text').attr('x', ml - 10).attr('y', 14).attr('text-anchor', 'end')
      .style('font-size', 'var(--cds-font-size-caption)').style('fill', 'var(--color-fg-muted)').text('Week of');

    const g = svg.selectAll('g.iss').data(issues).join('g').attr('class', 'iss')
      .attr('transform', (_, i) => `translate(0,${top + i * cellH})`);
    g.append('text').attr('x', ml - 10).attr('y', cellH / 2).attr('dy', '0.35em').attr('text-anchor', 'end')
      .style('font-size', 'var(--cds-font-size-caption)').style('fill', 'var(--color-fg)').text((s) => s);
    const showPct = x.bandwidth() >= 40;
    const animate = fresh('mix');
    const cells = [];
    g.each(function (issue, ri) {
      const row = d3.select(this);
      weeks.forEach((wk, wi) => {
        const delay = `${wi * 70 + ri * 22}ms`;
        const r = byKey.get(wk + '|' + issue);
        const s = r ? r.share : 0;
        row.append('rect').attr('x', x(wk)).attr('y', 1).attr('width', x.bandwidth()).attr('height', cellH - 2)
          .attr('rx', 3)
          .style('fill', r ? C[0] : 'transparent')
          .attr('fill-opacity', r ? op(s) : 1)
          .style('stroke', r ? 'none' : 'var(--color-border-line)')
          .attr('class', animate ? 'ar-pop' : null)
          .on('animationend', function () { this.classList.remove('ar-pop'); })
          .style('transform-box', 'fill-box')
          .style('animation-delay', delay)
          .each(function () { cells.push({ node: this, issue, wk }); })
          .on('mouseenter', () => cells.forEach((c) => { c.node.style.opacity = c.issue === issue || c.wk === wk ? 1 : 0.35; }))
          .on('mousemove', (ev) => {
            const posts = r ? r.posts : (rows.find((q) => q.week === wk) || {}).posts;
            tip.show(ev, `${issue} · week of ${wf(new Date(wk + 'T00:00:00Z'))}`, [
              [null, `${pct(s)} of the week's posts`],
              [null, `${fmt(r ? r.mentions : 0)} of ${fmt(posts || 0)} posts`]
            ]);
          })
          .on('mouseleave', () => { cells.forEach((c) => { c.node.style.opacity = ''; }); tip.hide(); });
        if (showPct && r) {
          row.append('text').attr('x', x(wk) + x.bandwidth() / 2).attr('y', cellH / 2).attr('dy', '0.35em')
            .attr('text-anchor', 'middle').attr('pointer-events', 'none')
            .style('font-size', 'var(--cds-font-size-caption)').style('font-variant-numeric', 'tabular-nums')
            .style('fill', op(s) > 0.55 ? 'var(--color-bg)' : 'var(--color-fg)')
            .text(s > 0 && s < 0.005 ? '<1%' : pct0(s))
            .attr('class', animate ? 'ar-fadein' : null)
            .style('animation-delay', `${wi * 70 + ri * 22 + 200}ms`);
        }
      });
    });
  }

  // ---------- daily volume ----------
  function drawDaily(d) {
    const el = document.getElementById('c-daily');
    if (!prep(el, d)) return;
    const rows = d.data.map((r) => ({ day: r.day, date: new Date(r.day + 'T00:00:00Z'), posts: Number(r.posts), cat: Number(r.categorized_posts) }));
    const height = 220;
    const { svg, w } = svgIn(el, height);
    const y = d3.scaleLinear([0, d3.max(rows, (r) => r.posts) || 1], [height - 22, 8]).nice();
    const ticks = y.ticks(4);
    const ml = textWidth(svg, ticks.map(fmt)) + 8;
    const x = d3.scaleBand(rows.map((r) => r.day), [ml, w - 2]).paddingInner(0.2);
    const tip = makeTip(el);
    const df = d3.utcFormat('%b %-d');
    svg.append('g').selectAll('line').data(ticks).join('line')
      .attr('x1', ml).attr('x2', w).attr('y1', (t) => y(t)).attr('y2', (t) => y(t))
      .style('stroke', 'var(--cds-chart-grid)');
    svg.append('g').selectAll('text').data(ticks).join('text')
      .attr('x', ml - 6).attr('y', (t) => y(t)).attr('dy', '0.35em').attr('text-anchor', 'end')
      .style('font-size', 'var(--cds-font-size-caption)').style('fill', 'var(--color-fg-muted)').text(fmt);
    const animDaily = fresh('daily');
    const bars = svg.append('g').selectAll('rect').data(rows).join('rect')
      .style('transition', 'opacity 180ms ease')
      .attr('x', (r) => x(r.day)).attr('width', x.bandwidth())
      .attr('y', (r) => y(r.posts)).attr('height', (r) => y(0) - y(r.posts))
      .attr('rx', Math.min(4, x.bandwidth() / 2)).style('fill', C[0])
      .attr('class', animDaily ? 'ar-rise' : null)
      .style('transform-box', 'fill-box')
      .style('animation-delay', (_, i) => `${i * 22}ms`)
      .on('mousemove', function (ev, r) { bars.style('opacity', (q) => (q === r ? 1 : 0.4)); tip.show(ev, df(r.date), [[null, `${fmt(r.posts)} posts`], [null, `${fmt(r.cat)} matched to an issue`]]); })
      .on('mouseleave', () => { bars.style('opacity', 1); tip.hide(); });

    const lw = textWidth(svg, ['Aug 31']) + 12;
    const every = Math.max(1, Math.ceil(lw / x.step()));
    svg.append('g').selectAll('text').data(rows.filter((_, i) => i % every === 0)).join('text')
      .attr('x', (r) => x(r.day) + x.bandwidth() / 2).attr('y', height - 4).attr('text-anchor', 'middle')
      .style('font-size', 'var(--cds-font-size-caption)').style('fill', 'var(--color-fg-muted)').text((r) => df(r.date));
    svg.append('line').attr('x1', ml).attr('x2', w).attr('y1', y(0)).attr('y2', y(0)).style('stroke', 'var(--cds-chart-axis)');
  }

  // ---------- subreddits ----------
  function drawSubs(d) {
    const el = document.getElementById('c-subs');
    if (!prep(el, d)) return;
    const rows = d.data.slice().sort((a, b) => Number(b.posts) - Number(a.posts));
    hbar(el, 'subs', rows, {
      label: (r) => r.subreddit,
      endLabel: (r) => fmt(Number(r.posts)),
      segs: [{ value: (r) => Number(r.posts), color: () => C[0] }],
      tip: (r) => [[null, `${fmt(Number(r.posts))} posts`], [null, `${fmt(Number(r.comments))} comments`]]
    });
  }

  // ---------- posts table ----------
  const sortState = { k: 'comments', dir: -1 };
  const numCols = new Set(['comments', 'score']);
  const table = document.getElementById('t-posts');
  const search = document.getElementById('q-posts');
  let lastPosts = null;

  function renderTable() {
    const tb = table.querySelector('tbody');
    tb.replaceChildren();
    const d = lastPosts;
    if (!d || d.status !== 'ok') {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 7;
      if (d && d.status !== 'loading') {
        const f = document.createElement('div');
        f.className = 'ar-fail';
        f.append(document.createElement('i'), document.createTextNode('Failed to load'));
        td.append(f);
      } else {
        td.className = 'dash-skeleton';
        td.textContent = ' ';
      }
      tr.append(td);
      tb.append(tr);
      return;
    }
    const q = search.value.trim().toLowerCase();
    let rows = d.data.filter((r) => !q || [r.title, r.subreddit, r.car_model, r.issue_category].join(' ').toLowerCase().includes(q));
    const k = sortState.k;
    rows = rows.slice().sort((a, b) => {
      const va = numCols.has(k) ? Number(a[k]) : String(a[k] || '');
      const vb = numCols.has(k) ? Number(b[k]) : String(b[k] || '');
      return (va > vb ? 1 : va < vb ? -1 : 0) * sortState.dir;
    });
    const animRows = fresh('table');
    rows.forEach((r, i) => {
      const tr = document.createElement('tr');
      tr.dataset.key = r.id;
      if (animRows && i < 25) { tr.className = 'ar-enter'; tr.style.animationDelay = `${300 + i * 30}ms`; }
      const tdT = document.createElement('td');
      tdT.className = 'title';
      const url = String(r.url || '');
      if (/^https:\/\/(www\.)?reddit\.com\//.test(url)) {
        const a = document.createElement('a');
        a.href = url;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.textContent = r.title;
        tdT.append(a);
      } else tdT.textContent = r.title;
      const cell = (txt, cls) => { const td = document.createElement('td'); if (cls) td.className = cls; td.textContent = txt; return td; };
      tr.append(
        tdT,
        cell(r.subreddit, 'hide-n'),
        cell(r.car_model || '', ''),
        cell(r.issue_category, 'hide-n cat'),
        cell(fmt(Number(r.comments)), 'n'),
        cell(fmt(Number(r.score)), 'n hide-n'),
        cell(r.posted, 'hide-n nowrap')
      );
      tb.append(tr);
    });
    table.querySelectorAll('th').forEach((th) => {
      const b = th.querySelector('button');
      th.setAttribute('aria-sort', b.dataset.k === k ? (sortState.dir > 0 ? 'ascending' : 'descending') : 'none');
    });
  }
  table.querySelectorAll('th button').forEach((b) => b.addEventListener('click', () => {
    const k = b.dataset.k;
    if (sortState.k === k) sortState.dir *= -1;
    else { sortState.k = k; sortState.dir = numCols.has(k) ? -1 : 1; }
    renderTable();
  }));
  search.addEventListener('input', renderTable);

  // ---------- car on the road ----------
  const road = document.getElementById('road');
  const drive = () => { road.classList.remove('go'); void road.offsetWidth; road.classList.add('go'); };
  road.addEventListener('click', drive);
  road.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); drive(); } });

  // ---------- wire up ----------
  const redraw = () => {
    const k = dash.data('kpis'), share = dash.data('categorized_share');
    const issues = dash.data('top_issues'), models = dash.data('top_models');
    drawKpis(k, share);
    drawTakeaway(issues, models);
    drawIssues(issues);
    drawEngage(issues);
    drawModels(models);
    drawMix(dash.data('issue_mix_by_week'));
    drawDaily(dash.data('posts_by_day'));
    drawSubs(dash.data('subreddits'));
    lastPosts = dash.data('top_posts');
    renderTable();
  };
  dash.onData(redraw);
  // Labels are measured for layout, so redraw once League Spartan has loaded.
  if (document.fonts && document.fonts.load) {
    document.fonts.load('600 16px "League Spartan"').then(() => redraw()).catch(() => {});
  }
})();
