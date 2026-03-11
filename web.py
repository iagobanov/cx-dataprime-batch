#!/usr/bin/env python3
"""
cx-dataprime-batch — Web UI
A minimal Flask app that wraps dataprime_chunk.py with a browser interface.

Usage:
    python web.py                  # http://localhost:5001
    python web.py --port 8080      # http://localhost:8080
"""

import json
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from queue import Queue

from flask import Flask, request, jsonify, Response, stream_with_context

from dataprime_chunk import (
    CORALOGIX_ENDPOINTS,
    run_query,
    detect_result_type,
    merge_results,
    generate_chunks,
    format_duration,
    extract_aggregate_fields,
)

app = Flask(__name__)

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>cx-dataprime-batch</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace;
         background: #0d1117; color: #c9d1d9; min-height: 100vh; padding: 2rem; }
  .container { max-width: 900px; margin: 0 auto; }
  h1 { font-size: 1.4rem; color: #58a6ff; margin-bottom: 0.3rem; }
  .subtitle { color: #8b949e; font-size: 0.85rem; margin-bottom: 1.5rem; }
  .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-bottom: 1rem; }
  .form-full { grid-column: 1 / -1; }
  label { display: block; font-size: 0.75rem; color: #8b949e; margin-bottom: 0.3rem; text-transform: uppercase; letter-spacing: 0.05em; }
  input, select, textarea { width: 100%; padding: 0.5rem 0.7rem; background: #161b22; border: 1px solid #30363d;
    border-radius: 6px; color: #c9d1d9; font-family: inherit; font-size: 0.9rem; outline: none; }
  input:focus, select:focus, textarea:focus { border-color: #58a6ff; }
  textarea { resize: vertical; min-height: 160px; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 0.8rem; line-height: 1.5; }
  .row { display: flex; gap: 1rem; align-items: end; }
  .row > div { flex: 1; }
  button { padding: 0.6rem 1.5rem; background: #238636; color: #fff; border: none; border-radius: 6px;
    font-size: 0.9rem; cursor: pointer; font-weight: 600; width: 100%; margin-top: 1rem; }
  button:hover { background: #2ea043; }
  button:disabled { background: #21262d; color: #484f58; cursor: not-allowed; }
  .btn-csv { background: #1f6feb; margin-top: 0.7rem; display: none; font-size: 0.85rem; width: auto; }
  .btn-csv:hover { background: #388bfd; }
  .btn-csv.visible { display: inline-block; }
  .log-box { background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 1rem;
    margin-top: 1.5rem; max-height: 400px; overflow-y: auto; font-family: 'SF Mono', monospace;
    font-size: 0.78rem; line-height: 1.6; white-space: pre-wrap; display: none; }
  .log-box.visible { display: block; }
  .result-box { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 1rem;
    margin-top: 1rem; display: none; }
  .result-box.visible { display: block; }
  .result-box h3 { font-size: 0.9rem; color: #58a6ff; margin-bottom: 0.7rem; }
  table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
  th { text-align: left; padding: 0.4rem 0.6rem; border-bottom: 2px solid #30363d; color: #8b949e; }
  td { padding: 0.4rem 0.6rem; border-bottom: 1px solid #21262d; }
  tr:hover td { background: #161b22; }
  .ok { color: #3fb950; } .err { color: #f85149; } .warn { color: #d29922; } .dim { color: #484f58; }
  .info { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 0.8rem 1rem;
    margin-bottom: 1.5rem; font-size: 0.8rem; color: #8b949e; line-height: 1.6; }
  .info code { background: #21262d; padding: 0.15rem 0.4rem; border-radius: 3px; color: #c9d1d9; font-size: 0.78rem; }
</style>
</head>
<body>
<div class="container">
  <h1>cx-dataprime-batch</h1>
  <p class="subtitle">Chunked DataPrime queries with automatic scan-limit bypass and parallel execution</p>

  <div class="info">
    Paste a DataPrime query below, pick a date range, and hit <code>Run</code>.
    Queries are split into time chunks and run in parallel. If a chunk hits the scan limit, it auto-splits into smaller windows and retries.
    Use <code>$l.subsystemname</code>, <code>$d.body</code>, etc. &mdash; standard DataPrime syntax.
  </div>

  <div class="form-full" style="margin-bottom:1rem">
    <label>DataPrime Query</label>
    <textarea id="query" placeholder="source logs&#10;| filter $l.subsystemname == 'MySubsystem'&#10;| aggregate count() as Count"></textarea>
  </div>

  <div class="form-grid">
    <div><label>Start Date</label><input type="date" id="start"></div>
    <div><label>End Date</label><input type="date" id="end"></div>
    <div><label>Region</label>
      <select id="region">REGION_OPTIONS</select>
    </div>
    <div><label>API Key</label><input type="password" id="apikey" placeholder="cxup_... or CORALOGIX_API_KEY"></div>
    <div><label>Chunk Size (days)</label><input type="number" id="chunk" value="7" min="1" max="90"></div>
    <div><label>Workers</label><input type="number" id="workers" value="4" min="1" max="16"></div>
    <div><label>Tier</label>
      <select id="tier">
        <option value="TIER_ARCHIVE" selected>Archive</option>
        <option value="TIER_FREQUENT_SEARCH">Frequent Search</option>
      </select>
    </div>
    <div><label>Min Chunk (days)</label><input type="number" id="minchunk" value="1" min="1" max="30"></div>
  </div>

  <button id="run" onclick="runQuery()">Run Query</button>

  <div id="logbox" class="log-box"></div>
  <div id="resultbox" class="result-box">
    <h3 id="result-title">Results</h3>
    <div id="result-table"></div>
    <button id="csv-btn" class="btn-csv" onclick="downloadCSV()">Download CSV</button>
  </div>
</div>

<script>
// Default dates: 90 days ago → today
const today = new Date();
const d90 = new Date(today); d90.setDate(d90.getDate() - 90);
document.getElementById('end').value = today.toISOString().slice(0,10);
document.getElementById('start').value = d90.toISOString().slice(0,10);

let lastResultRows = [];

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

function downloadCSV() {
  if (!lastResultRows.length) return;
  const keys = Object.keys(lastResultRows[0]);
  const header = keys.map(k => '"' + k.replace(/"/g, '""') + '"').join(',');
  const lines = lastResultRows.map(row =>
    keys.map(k => { const v = String(row[k] ?? ''); return '"' + v.replace(/"/g, '""') + '"'; }).join(',')
  );
  const csv = header + '\\n' + lines.join('\\n');
  const blob = new Blob([csv], {type: 'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'dataprime-results.csv';
  a.click();
}

function runQuery() {
  const btn = document.getElementById('run');
  const logbox = document.getElementById('logbox');
  const resultbox = document.getElementById('resultbox');
  btn.disabled = true; btn.textContent = 'Running...';
  logbox.className = 'log-box visible'; logbox.innerHTML = '';
  resultbox.className = 'result-box'; document.getElementById('result-table').innerHTML = '';

  const params = new URLSearchParams({
    query: document.getElementById('query').value,
    start: document.getElementById('start').value,
    end: document.getElementById('end').value,
    region: document.getElementById('region').value,
    api_key: document.getElementById('apikey').value,
    chunk_size: document.getElementById('chunk').value,
    min_chunk_size: document.getElementById('minchunk').value,
    workers: document.getElementById('workers').value,
    tier: document.getElementById('tier').value,
  });

  const evtSource = new EventSource('/run?' + params.toString());

  evtSource.addEventListener('log', e => {
    logbox.innerHTML += e.data + '\\n';
    logbox.scrollTop = logbox.scrollHeight;
  });

  evtSource.addEventListener('result', e => {
    const data = JSON.parse(e.data);
    lastResultRows = data.rows;
    resultbox.className = 'result-box visible';
    document.getElementById('result-title').textContent =
      'Results (' + data.rows.length + ' rows, type=' + data.type + ', ' + data.elapsed + ')';
    if (data.rows.length === 0) {
      document.getElementById('result-table').innerHTML = '<p class="dim">No data returned.</p>';
      document.getElementById('csv-btn').className = 'btn-csv';
    } else {
      let html = '<table><tr>';
      Object.keys(data.rows[0]).forEach(k => html += '<th>' + esc(k) + '</th>');
      html += '</tr>';
      const aggFields = new Set(data.agg_fields || []);
      data.rows.forEach(row => {
        html += '<tr>';
        Object.keys(data.rows[0]).forEach(k => {
          const v = row[k]; const num = parseFloat(v);
          html += '<td>' + (aggFields.has(k) && !isNaN(num) ? num.toLocaleString() : esc(String(v))) + '</td>';
        });
        html += '</tr>';
      });
      html += '</table>';
      document.getElementById('result-table').innerHTML = html;
      document.getElementById('csv-btn').className = 'btn-csv visible';
    }
  });

  evtSource.addEventListener('done', e => {
    evtSource.close();
    btn.disabled = false; btn.textContent = 'Run Query';
  });

  evtSource.onerror = () => {
    evtSource.close();
    btn.disabled = false; btn.textContent = 'Run Query';
    logbox.innerHTML += '<span class="err">Connection lost.</span>\\n';
  };
}
</script>
</body>
</html>"""


REGION_LABELS = {
    "us2": "us2 | cx498",
    "cx498": None,  # hide duplicate
}

def build_html():
    opts = ''
    for r in CORALOGIX_ENDPOINTS:
        label = REGION_LABELS.get(r, r)
        if label is None:
            continue
        opts += f'<option value="{r}">{label}</option>'
    return HTML.replace('REGION_OPTIONS', opts)


@app.route('/')
def index():
    return build_html()


@app.route('/run')
def run():
    q = request.args.get('query', '').strip()
    start_s = request.args.get('start', '')
    end_s = request.args.get('end', '')
    region = request.args.get('region', 'eu1')
    api_key = request.args.get('api_key', '')
    chunk_size = int(request.args.get('chunk_size', 7))
    min_chunk_size = int(request.args.get('min_chunk_size', 1))
    workers = int(request.args.get('workers', 4))
    tier = request.args.get('tier', 'TIER_ARCHIVE')

    def generate():
        def send_log(msg):
            yield f"event: log\ndata: {msg}\n\n"

        if not q:
            yield from send_log('<span class="err">Query is empty.</span>')
            yield "event: done\ndata: ok\n\n"
            return
        if not api_key:
            yield from send_log('<span class="err">API key is required.</span>')
            yield "event: done\ndata: ok\n\n"
            return

        try:
            start_dt = datetime.fromisoformat(start_s).replace(tzinfo=timezone.utc)
            end_dt = datetime.fromisoformat(end_s).replace(tzinfo=timezone.utc)
        except Exception as e:
            yield from send_log(f'<span class="err">Date error: {e}</span>')
            yield "event: done\ndata: ok\n\n"
            return

        from dataprime_chunk import get_base_url, is_scan_limit_error
        try:
            base = get_base_url(region)
        except Exception as e:
            yield from send_log(f'<span class="err">{e}</span>')
            yield "event: done\ndata: ok\n\n"
            return

        total_days = (end_dt - start_dt).days
        initial_chunks = list(generate_chunks(start_dt, end_dt, chunk_size))

        yield from send_log(f'<span class="dim">Range: {start_dt.date()} &rarr; {end_dt.date()} ({total_days} days)</span>')
        yield from send_log(f'<span class="dim">Chunks: {len(initial_chunks)} &times; {chunk_size}d | Workers: {workers} | Tier: {tier}</span>')
        yield from send_log('')

        wall_start = time.monotonic()
        all_rows = []
        result_type = None
        failed = []
        pending = list(initial_chunks)
        round_num = 0

        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=workers) as pool:
            while pending:
                round_num += 1
                if round_num > 1:
                    yield from send_log(f'<span class="warn">Retry round {round_num}: {len(pending)} sub-chunks</span>')

                futs = {}
                for cs, ce in pending:
                    fut = pool.submit(run_query, base, api_key, q, cs, ce, 2000, tier, 3)
                    futs[fut] = (cs, ce)

                next_pending = []
                for fut in as_completed(futs):
                    cs, ce = futs[fut]
                    days = (ce - cs).days
                    label = f"{cs.date()} &rarr; {ce.date()} ({days}d)"
                    elapsed = format_duration(time.monotonic() - wall_start)

                    try:
                        result = fut.result()
                    except Exception as e:
                        yield from send_log(f'<span class="err">&#10007; {label} &mdash; {e}</span>')
                        failed.append(label)
                        continue

                    if result["scan_limit"]:
                        half = max(days // 2, 1)
                        if days <= 1 or half < min_chunk_size:
                            yield from send_log(f'<span class="err">&#10007; SCAN LIMIT {label} &mdash; can\'t split further</span>')
                            failed.append(label)
                        else:
                            yield from send_log(f'<span class="warn">&#9888; SCAN LIMIT {label} &mdash; splitting to {half}d</span>')
                            next_pending.extend(generate_chunks(cs, ce, half))
                    elif result["error"]:
                        yield from send_log(f'<span class="err">&#10007; {label} &mdash; {str(result["error"])[:500]}</span>')
                        failed.append(label)
                    else:
                        rows = result["rows"]
                        if result_type is None and rows:
                            result_type = detect_result_type(rows)
                        all_rows.extend(rows)
                        yield from send_log(f'<span class="ok">&#10003;</span> {label} &rarr; {len(rows)} rows <span class="dim">({elapsed})</span>')

                next_pending.sort(key=lambda x: x[0])
                pending = next_pending

        wall_elapsed = format_duration(time.monotonic() - wall_start)

        if failed:
            yield from send_log(f'\n<span class="warn">&#9888; {len(failed)} window(s) failed</span>')

        rt = result_type or detect_result_type(all_rows)
        agg_fields = extract_aggregate_fields(q)
        final = merge_results(all_rows, rt, aggregate_fields=agg_fields or None) if all_rows else []

        yield from send_log(f'\n<span class="dim">Done in {wall_elapsed}</span>')

        payload = json.dumps({"rows": final, "type": rt, "elapsed": wall_elapsed, "failed": len(failed), "agg_fields": list(agg_fields)})
        yield f"event: result\ndata: {payload}\n\n"
        yield "event: done\ndata: ok\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')


if __name__ == '__main__':
    port = 5001
    if '--port' in sys.argv:
        port = int(sys.argv[sys.argv.index('--port') + 1])
    print(f"cx-dataprime-batch UI: http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
