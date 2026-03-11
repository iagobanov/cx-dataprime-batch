#!/usr/bin/env python3
"""
DataPrime Query Chunker
Splits a DataPrime query across time chunks to bypass scan limits.
Auto-detects scan limit errors and halves the chunk size automatically.
Handles count aggregations, grouped results, and raw log rows.
Supports parallel chunk execution for faster queries.
"""

import click
import re
import requests
import json
import csv
import sys
import time
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse as parse_date
from rich.console import Console
from rich.table import Table
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

console = Console()
print_lock = threading.Lock()

CORALOGIX_ENDPOINTS = {
    "us1": "https://api.coralogix.us",
    "us2": "https://api.cx498.coralogix.com",
    "eu1": "https://api.coralogix.com",
    "eu2": "https://api.eu2.coralogix.com",
    "ap1": "https://api.coralogix.in",
    "ap2": "https://api.coralogixsg.com",
    "ap3": "https://api.ap3.coralogix.com",
    "cx498": "https://api.cx498.coralogix.com",
}

SCAN_LIMIT_SIGNALS = [
    "scan limit",
    "scanlimit",
    "quota exceeded",
    "scan quota",
    "bytes scanned",
    "limit exceeded",
    "bytesscannedlimit",
]

def is_scan_limit_error(text: str) -> bool:
    return any(sig in text.lower() for sig in SCAN_LIMIT_SIGNALS)


def get_base_url(region: str, custom_url: str = None) -> str:
    if custom_url:
        return custom_url.rstrip("/")
    region = region.lower()
    if region not in CORALOGIX_ENDPOINTS:
        raise click.BadParameter(f"Unknown region '{region}'. Valid: {', '.join(CORALOGIX_ENDPOINTS.keys())}")
    return CORALOGIX_ENDPOINTS[region]


def flatten_dict(d: dict, parent_key: str = "", sep: str = ".") -> dict:
    """Flatten nested dicts: {'a': {'b': 'c'}} -> {'a.b': 'c'}"""
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep))
        else:
            items[new_key] = v
    return items


def _run_query_once(base_url: str, api_key: str, query: str, start: datetime, end: datetime, limit: int = 2000, tier: str = "TIER_ARCHIVE", timeout: int = 300) -> dict:
    url = f"{base_url}/api/v1/dataprime/query"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "query": query,
        "metadata": {
            "startDate": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endDate": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "defaultSource": "logs",
            "tier": tier.upper(),
            "syntax": "QUERY_SYNTAX_DATAPRIME",
            "limit": limit,
        },
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout, stream=True)
        if resp.status_code in (429, 503):
            body = resp.text
            return {"rows": [], "warnings": [], "error": body, "scan_limit": is_scan_limit_error(body), "retryable": True}
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        body = getattr(e.response, "text", str(e))
        err_msg = f"{e} | {body[:300]}" if body else str(e)
        return {"rows": [], "warnings": [], "error": err_msg, "scan_limit": is_scan_limit_error(body), "retryable": False}
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        return {"rows": [], "warnings": [], "error": str(e), "scan_limit": False, "retryable": True}
    except requests.exceptions.RequestException as e:
        return {"rows": [], "warnings": [], "error": str(e), "scan_limit": False, "retryable": True}

    rows = []
    warnings = []
    error = None
    scan_limit = False

    for line in resp.iter_lines():
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue

        if "error" in obj:
            err_msg = str(obj["error"])
            error = err_msg
            if is_scan_limit_error(err_msg):
                scan_limit = True

        elif "warning" in obj:
            w = str(obj["warning"])
            if w.strip():
                warnings.append(w)
            if is_scan_limit_error(w):
                scan_limit = True

        elif "result" in obj:
            for item in obj["result"].get("results", []):
                user_data = item.get("userData", item)
                if isinstance(user_data, str):
                    try:
                        user_data = json.loads(user_data)
                    except json.JSONDecodeError:
                        user_data = {"_raw": user_data}
                if isinstance(user_data, dict):
                    rows.append(flatten_dict(user_data))

    retryable = bool(error) and not scan_limit  # server-side errors are retryable
    return {"rows": rows, "warnings": warnings, "error": error, "scan_limit": scan_limit, "retryable": retryable}


def run_query(base_url: str, api_key: str, query: str, start: datetime, end: datetime, limit: int = 2000, tier: str = "TIER_ARCHIVE", retries: int = 3) -> dict:
    """Run query with automatic retries on timeouts and transient errors."""
    label = f"[{start.date()} → {end.date()}]"
    for attempt in range(1, retries + 1):
        result = _run_query_once(base_url, api_key, query, start, end, limit, tier, timeout=300)
        if not result.get("retryable") or attempt == retries:
            result.pop("retryable", None)
            return result
        wait = 5 * attempt
        log(f"    [yellow]↻ {label} retry {attempt}/{retries} in {wait}s — {result['error'][:80]}[/yellow]")
        time.sleep(wait)
    result.pop("retryable", None)
    return result


def extract_aggregate_fields(query: str) -> set:
    """Extract field names defined by aggregate functions in the query.
    e.g. 'aggregate count() as Count, sum(x) as Total' -> {'Count', 'Total'}
    """
    pattern = r'\b(?:count|sum|avg|min|max|countif|sumif|avgif|percentile|stddev|variance)\s*\([^)]*\)\s+as\s+(\w+)'
    return set(re.findall(pattern, query, re.IGNORECASE))


def detect_result_type(rows: list) -> str:
    """
    Detect result shape:
    - 'count'   : single row, single numeric field  -> {"Count": 779513}
    - 'grouped' : multiple rows with numeric + group keys -> {"API": "feeLookup", "Count": 123}
    - 'raw'     : log rows with mostly string fields
    """
    if not rows:
        return "raw"

    if len(rows) == 1 and len(rows[0]) == 1:
        val = list(rows[0].values())[0]
        try:
            float(val)
            return "count"
        except (TypeError, ValueError):
            pass

    numeric_fields = []
    for k, v in rows[0].items():
        try:
            float(v)
            numeric_fields.append(k)
        except (TypeError, ValueError):
            pass

    if numeric_fields:
        return "grouped"

    return "raw"


def merge_results(all_rows: list, result_type: str, aggregate_fields: set = None) -> list:
    """
    Merge rows across chunks:
    - count:   SUM all numeric fields across chunks -> one final row
    - grouped: group by non-aggregate keys, SUM only aggregate fields
    - raw:     concatenate all rows as-is

    aggregate_fields: set of column names that are actual aggregates (from query parsing).
                      Only these get summed. All other columns are group keys,
                      even if they contain numeric values (e.g. AgentID).
    """
    if not all_rows:
        return []

    def to_int_if_whole(v):
        if isinstance(v, float) and v == int(v):
            return int(v)
        return v

    if result_type == "count":
        merged = {}
        for row in all_rows:
            for k, v in row.items():
                try:
                    merged[k] = merged.get(k, 0) + float(v)
                except (TypeError, ValueError):
                    merged[k] = v
        return [{k: to_int_if_whole(v) for k, v in merged.items()}]

    elif result_type == "grouped":
        first = all_rows[0]
        numeric_keys = []
        group_keys = []

        if aggregate_fields:
            for k in first.keys():
                if k in aggregate_fields:
                    numeric_keys.append(k)
                else:
                    group_keys.append(k)
        else:
            for k, v in first.items():
                try:
                    float(v)
                    numeric_keys.append(k)
                except (TypeError, ValueError):
                    group_keys.append(k)

        merged = {}
        for row in all_rows:
            gk = json.dumps({k: row.get(k) for k in group_keys}, sort_keys=True)
            if gk not in merged:
                merged[gk] = {k: row.get(k) for k in group_keys}
                for nk in numeric_keys:
                    merged[gk][nk] = 0
            for nk in numeric_keys:
                try:
                    merged[gk][nk] += float(row.get(nk, 0))
                except (TypeError, ValueError):
                    pass

        result = list(merged.values())
        if numeric_keys:
            result.sort(key=lambda x: x.get(numeric_keys[0], 0), reverse=True)
        for row in result:
            for nk in numeric_keys:
                row[nk] = to_int_if_whole(row[nk])
        return result

    else:
        return all_rows


def generate_chunks(start: datetime, end: datetime, chunk_size_days: int):
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_size_days), end)
        yield current, chunk_end
        current = chunk_end


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s"


def print_table(rows: list, title: str = "Results"):
    if not rows:
        console.print("[yellow]No results.[/yellow]")
        return
    table = Table(title=title, show_lines=True)
    keys = list(rows[0].keys())
    for k in keys:
        table.add_column(str(k), overflow="fold")
    for row in rows[:500]:
        table.add_row(*[str(row.get(k, "")) for k in keys])
    console.print(table)


def write_csv(rows: list, path: str):
    if not rows:
        console.print("[yellow]No rows to write.[/yellow]")
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=keys).writeheader()
        csv.DictWriter(f, fieldnames=keys).writerows(rows)
    console.print(f"[green]✓ {len(rows)} rows → {path}[/green]")


def write_json(rows: list, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    console.print(f"[green]✓ {len(rows)} rows → {path}[/green]")


def log(msg: str):
    with print_lock:
        console.print(msg)


@click.command()
@click.option("--query", "-q", required=False, help="DataPrime query string")
@click.option("--query-file", "-f", type=click.Path(exists=True), help="Path to .dp query file")
@click.option("--start", "-s", required=True, help="Start date (ISO 8601, e.g. 2026-01-01)")
@click.option("--end", "-e", required=True, help="End date (ISO 8601)")
@click.option("--chunk-size", "-c", default=7, show_default=True, type=int, help="Initial chunk size in days")
@click.option("--min-chunk-size", default=1, show_default=True, type=int, help="Minimum chunk before giving up on a window")
@click.option("--api-key", "-k", envvar="CORALOGIX_API_KEY", required=True, help="API key (or CORALOGIX_API_KEY env var)")
@click.option("--region", "-r", default="eu1", show_default=True, help=f"Region: {', '.join(CORALOGIX_ENDPOINTS.keys())}")
@click.option("--base-url", default=None, help="Override base URL (e.g. PrivateLink)")
@click.option("--tier", "-t", default="TIER_ARCHIVE", show_default=True,
              type=click.Choice(["TIER_FREQUENT_SEARCH", "TIER_ARCHIVE"], case_sensitive=False),
              help="Data tier to query")
@click.option("--limit", default=2000, show_default=True, help="Max rows per chunk")
@click.option("--workers", "-w", default=4, show_default=True, type=int, help="Parallel workers (concurrent chunks)")
@click.option("--output", "-o", default=None, help="Output file (.csv or .json). Default: print table")
@click.option("--delay", default=0.2, show_default=True, type=float, help="Seconds between chunk submissions")
@click.option("--retries", default=3, show_default=True, type=int, help="Retries per chunk on timeout/transient errors")
@click.option("--dry-run", is_flag=True, help="Preview chunks without executing")
def main(query, query_file, start, end, chunk_size, min_chunk_size, api_key, region, base_url, tier, limit, workers, output, delay, retries, dry_run):
    """
    DataPrime Query Chunker — splits queries to bypass scan limits.

    \b
    Auto-detects result type (count / grouped / raw) and merges correctly.
    On scan limit: auto-splits the failing window into smaller chunks and retries.
    Runs chunks in parallel for faster execution.

    \b
    Examples:
      # Count query — 7d chunks, 4 workers
      dataprime-batch -f query.dp -s 2026-01-01 -e 2026-03-01 -k $CX_KEY -r us2 -o out.csv

      # 90-day query with 8 parallel workers
      dataprime-batch -f query.dp -s 2025-12-01 -e 2026-03-01 -k $CX_KEY -r eu1 -w 8 -o out.csv

      # Dry run — preview chunks without executing
      dataprime-batch -f query.dp -s 2026-01-01 -e 2026-03-01 -c 7 --dry-run
    """
    if query_file:
        with open(query_file) as f:
            dp_query = f.read().strip()
    elif query:
        dp_query = query
    else:
        raise click.UsageError("Provide --query or --query-file")

    try:
        start_dt = parse_date(start)
        end_dt = parse_date(end)
        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=timezone.utc)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
    except Exception as e:
        raise click.BadParameter(f"Date parse error: {e}")

    if start_dt >= end_dt:
        raise click.UsageError("--start must be before --end")

    total_days = (end_dt - start_dt).days
    initial_chunks = list(generate_chunks(start_dt, end_dt, chunk_size))

    console.print(f"\n[bold cyan]DataPrime Query Chunker[/bold cyan]")
    console.print(f"  Range      : {start_dt.date()} → {end_dt.date()} ({total_days} days)")
    console.print(f"  Chunks     : {len(initial_chunks)} × {chunk_size}d (min retry: {min_chunk_size}d)")
    console.print(f"  Region     : {region}")
    console.print(f"  Tier       : {tier}")
    console.print(f"  Workers    : {workers}")
    console.print(f"  Limit      : {limit} rows/chunk")
    console.print()

    if dry_run:
        table = Table(title="Dry Run — Chunks", show_lines=True)
        table.add_column("#", style="dim")
        table.add_column("Start")
        table.add_column("End")
        table.add_column("Days", justify="right")
        for i, (cs, ce) in enumerate(initial_chunks, 1):
            table.add_row(str(i), str(cs.date()), str(ce.date()), str((ce - cs).days))
        console.print(table)
        return

    wall_start = time.monotonic()
    base = get_base_url(region, base_url)
    all_rows = []
    chunk_summary = []
    result_type = None
    failed_windows = []
    completed_count = 0

    # Process chunks in parallel with scan-limit retry
    pending = list(initial_chunks)
    round_num = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        while pending:
            round_num += 1
            if round_num > 1:
                log(f"\n  [yellow]Retry round {round_num}: {len(pending)} sub-chunks[/yellow]")

            # Submit all pending chunks
            future_to_chunk = {}
            for i, (cs, ce) in enumerate(pending):
                fut = pool.submit(run_query, base, api_key, dp_query, cs, ce, limit, tier, retries)
                future_to_chunk[fut] = (cs, ce)
                if delay and i > 0:
                    time.sleep(delay)

            next_pending = []

            for fut in as_completed(future_to_chunk):
                cs, ce = future_to_chunk[fut]
                window_days = (ce - cs).days
                label = f"[{cs.date()} → {ce.date()}] ({window_days}d)"

                try:
                    result = fut.result()
                except Exception as e:
                    log(f"  [red]✗ EXCEPTION {label}: {e}[/red]")
                    chunk_summary.append({"start": cs, "end": ce, "days": window_days, "rows": 0, "status": "ERROR"})
                    failed_windows.append(label)
                    continue

                if result["scan_limit"]:
                    half = max(window_days // 2, 1)
                    if window_days <= 1 or half < min_chunk_size:
                        log(f"  [red]✗ SCAN LIMIT {label} — can't split further. Skipping.[/red]")
                        chunk_summary.append({"start": cs, "end": ce, "days": window_days, "rows": 0, "status": "SCAN_LIMIT_SKIP"})
                        failed_windows.append(label)
                    else:
                        log(f"  [yellow]⚠ SCAN LIMIT {label} — will retry as {half}d chunks...[/yellow]")
                        sub = list(generate_chunks(cs, ce, half))
                        next_pending.extend(sub)

                elif result["error"]:
                    log(f"  [red]✗ ERROR {label}: {result['error']}[/red]")
                    chunk_summary.append({"start": cs, "end": ce, "days": window_days, "rows": 0, "status": "ERROR"})
                    failed_windows.append(label)

                else:
                    rows = result["rows"]
                    if result_type is None and rows:
                        result_type = detect_result_type(rows)
                        log(f"  [dim]Auto-detected result type: [bold]{result_type}[/bold][/dim]")

                    all_rows.extend(rows)
                    completed_count += 1
                    elapsed = time.monotonic() - wall_start
                    log(f"  [green]✓[/green] {label} → {len(rows)} row(s)  [dim]({format_duration(elapsed)} elapsed)[/dim]")
                    chunk_summary.append({"start": cs, "end": ce, "days": window_days, "rows": len(rows), "status": "OK"})

                    for w in result.get("warnings", []):
                        if is_scan_limit_error(w):
                            log(f"    [yellow]⚠ {w}[/yellow]")

            # Sort next_pending by start date for orderly processing
            next_pending.sort(key=lambda x: x[0])
            pending = next_pending

    wall_elapsed = time.monotonic() - wall_start

    # Summary table — sort by start date
    chunk_summary.sort(key=lambda s: s["start"])
    console.print()
    stbl = Table(title="Chunk Summary", show_lines=True)
    stbl.add_column("Start")
    stbl.add_column("End")
    stbl.add_column("Days", justify="right")
    stbl.add_column("Rows", justify="right")
    stbl.add_column("Status")
    for s in chunk_summary:
        color = "green" if s["status"] == "OK" else ("yellow" if "SCAN" in s["status"] else "red")
        stbl.add_row(str(s["start"].date() if hasattr(s["start"], 'date') else s["start"]),
                     str(s["end"].date() if hasattr(s["end"], 'date') else s["end"]),
                     str(s["days"]), str(s["rows"]), f"[{color}]{s['status']}[/{color}]")
    console.print(stbl)

    if not all_rows:
        console.print("[yellow]No data returned.[/yellow]")
        console.print(f"\n[bold]Total time: {format_duration(wall_elapsed)}[/bold]")
        return

    # Merge across chunks
    rt = result_type or detect_result_type(all_rows)
    agg_fields = extract_aggregate_fields(dp_query)
    console.print(f"\n[dim]Merging {len(all_rows)} raw rows (type=[bold]{rt}[/bold], aggregates={agg_fields or 'auto'})...[/dim]")
    final_rows = merge_results(all_rows, rt, aggregate_fields=agg_fields or None)

    # Print totals
    if rt == "count":
        for k, v in final_rows[0].items():
            console.print(f"\n[bold green]TOTAL {k}: {v:,}[/bold green]")
    elif rt == "grouped":
        console.print(f"\n[bold]Merged → {len(final_rows)} groups[/bold]")

    if failed_windows:
        console.print(f"\n[red bold]⚠ PARTIAL RESULTS — {len(failed_windows)} window(s) skipped:[/red bold]")
        for fw in failed_windows:
            console.print(f"  [red]- {fw}[/red]")

    if output:
        if output.endswith(".json"):
            write_json(final_rows, output)
        else:
            write_csv(final_rows, output)
    else:
        print_table(final_rows, title=f"Results ({len(final_rows)} rows, type={rt})")

    console.print(f"\n[bold]Total time: {format_duration(wall_elapsed)}[/bold]")


if __name__ == "__main__":
    main()
