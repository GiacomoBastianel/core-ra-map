// Core Remedial Action Map — browser downloader for the JAO Publication Tool (Core CCR).
// Run this whole file with the browser's JavaScript tool in a tab open on https://publicationtool.jao.eu/core/
// (the API only answers same-origin requests). Then:
//   jaoDump.run('2026-10-25', '2026-11-01')   -> starts the download in the background (business days, Brussels time, inclusive)
//   jaoDump.state()                            -> poll until running is false; gives chunks, sha and counts
//   jaoDump.download()                         -> in a normal browser: saves jao-dump.json.gz, then run
//                                                 python3 pipeline/update.py --site . --raw jao-dump.json.gz
//   jaoDump.chunk(0), jaoDump.chunk(1), ...    -> for Claude's browser tools: each result is padded so the tool saves it to a
//                                                 file; pass those files to update.py --raw-chunks
window.jaoDump = (() => {
  const st = { running: false, done: 0, total: 0, errors: [], n429: 0, b64: null, sha: null, info: null, gz: null };
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const F = window.__origFetch || window.fetch.bind(window);
  const fmt = new Intl.DateTimeFormat('en-CA', { timeZone: 'Europe/Brussels', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hourCycle: 'h23' });
  function localMidnightUTC(day){
    const [y, m, d] = day.split('-').map(Number);
    for (const off of [1, 2]){
      const t = Date.UTC(y, m - 1, d) - off * 3600e3; const o = {};
      for (const x of fmt.formatToParts(new Date(t))) o[x.type] = x.value;
      if (`${o.year}-${o.month}-${o.day}` === day && +o.hour % 24 === 0) return t;
    }
    throw new Error('could not find local midnight for ' + day);
  }
  function nextDay(day){ const [y, m, d] = day.split('-').map(Number); return new Date(Date.UTC(y, m - 1, d + 1)).toISOString().slice(0, 10); }
  async function get(ep, f, t){
    const url = `/core/api/data/${ep}?FromUtc=${new Date(f).toISOString()}&ToUtc=${new Date(t).toISOString()}`;
    for (let k = 0; k < 10; k++){
      let r;
      try { r = await F(url); } catch (e){ await sleep(3000); continue; }
      if (r.ok) return r.json();
      if (r.status === 429){ st.n429++; await sleep((parseInt(r.headers.get('retry-after') || '60', 10) + 2) * 1000); continue; }
      if (r.status === 400) return { data: [] };
      await sleep(3000 * (k + 1));
    }
    throw new Error('failed ' + url);
  }
  function run(fromDay, toDay){
    if (st.running) return 'already running';
    Object.assign(st, { running: true, done: 0, total: 0, errors: [], n429: 0, b64: null, sha: null, info: null });
    const start = localMidnightUTC(fromDay), end = localMidnightUTC(nextDay(toDay));
    const wins = []; for (let t = start; t < end; t += 48 * 3600e3) wins.push([t, Math.min(t + 48 * 3600e3, end)]);
    st.total = wins.length;
    const D = { ra: [], raTso: {}, cnec: [], reasons: [], pra: [], cra: [], from: fromDay, to: toDay };
    const maps = { ra: new Map(), cnec: new Map(), reasons: new Map() };
    const ix = (kind, v) => { let i = maps[kind].get(v); if (i === undefined){ i = D[kind].length; D[kind].push(v); maps[kind].set(v, i); } return i; };
    const P = new Map(), C = new Map();
    (async () => {
      for (const [f, t] of wins){
        try {
          const p = await get('pra', f, t); await sleep(400);
          const c = await get('cra', f, t); await sleep(400);
          for (const x of p.data || []){
            const h = Math.round(Date.parse(x.dateTimeUtc) / 3600e3);
            let e = P.get(h); if (!e){ e = { r: -1, a: [] }; P.set(h, e); }
            if (x.reason) e.r = ix('reasons', x.reason);
            if (x.name){ const i = ix('ra', x.name); if (x.tso) (D.raTso[i] ||= {})[x.tso] = 1; e.a.push([i, x.baseline, x.afterNrao]); }
          }
          for (const x of c.data || []){
            const h = Math.round(Date.parse(x.dateTimeUtc) / 3600e3);
            let e = C.get(h); if (!e){ e = { r: -1, c: [] }; C.set(h, e); }
            if (x.reason) e.r = ix('reasons', x.reason);
            if (x.cnecName){
              const row = [ix('cnec', (x.cnecTso || '') + '|' + x.cnecName)];
              for (let i = 1; i <= 10; i++){ const n = x['cra' + i + 'Name']; if (n) row.push(ix('ra', n), x['cra' + i + 'Baseline'], x['cra' + i + 'AfterNrao']); }
              e.c.push(row);
            }
          }
        } catch (e){ st.errors.push(String(e)); }
        st.done++;
      }
      D.pra = [...P].sort((a, b) => a[0] - b[0]).map(([h, e]) => [h, e.r, e.a]);
      D.cra = [...C].sort((a, b) => a[0] - b[0]).map(([h, e]) => [h, e.r, e.c]);
      D.exportedAt = new Date().toISOString();
      const cs = new CompressionStream('gzip'); const w = cs.writable.getWriter(); w.write(new TextEncoder().encode(JSON.stringify(D))); w.close();
      const buf = new Uint8Array(await new Response(cs.readable).arrayBuffer());
      st.gz = buf;
      let bin = ''; for (let i = 0; i < buf.length; i += 0x8000) bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000));
      st.b64 = btoa(bin);
      st.sha = [...new Uint8Array(await crypto.subtle.digest('SHA-256', buf))].map(x => x.toString(16).padStart(2, '0')).join('');
      st.info = { hoursPRA: D.pra.length, hoursCRA: D.cra.length, raNames: D.ra.length, cnecs: D.cnec.length, first: D.pra.length ? new Date(D.pra[0][0] * 3600e3).toISOString() : null, last: D.pra.length ? new Date(D.pra[D.pra.length - 1][0] * 3600e3).toISOString() : null };
      st.running = false;
    })().catch(e => { st.errors.push(String(e)); st.running = false; });
    return `started: ${wins.length} two-day windows from ${fromDay} to ${toDay}`;
  }
  const CH = 350000;
  function state(){ return JSON.stringify({ running: st.running, done: st.done, total: st.total, errors: st.errors, n429: st.n429, sha: st.sha, info: st.info, chunks: st.b64 ? Math.ceil(st.b64.length / CH) : 0 }); }
  let PAD = null;
  function pad(){ if (!PAD){ const a = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/'; let x = 12345, s = []; for (let i = 0; i < 130000; i++){ x = (x * 1103515245 + 12345) & 0x7fffffff; s.push(a[(x >> 8) & 63]); } PAD = s.join(''); } return PAD; }
  function chunk(i){
    if (!st.b64) return 'no dump yet';
    const n = Math.ceil(st.b64.length / CH);
    return ['JAODUMP', i, n, st.sha, pad(), st.b64.slice(i * CH, (i + 1) * CH)].join('|');
  }
  function download(name){
    if (!st.gz) return 'no dump yet';
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([st.gz], { type: 'application/gzip' }));
    a.download = name || 'jao-dump.json.gz';
    document.body.append(a); a.click(); a.remove();
    return 'saved ' + a.download;
  }
  return { run, state, chunk, download };
})();
'jaoDump ready';
