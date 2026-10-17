#!/usr/bin/env python3
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

CHUNK_LINES = 20_000
FOLD_SIZE = 1_024         
WORKERS = 8
WINDOW_MINUTES = 5
SEEN_IDS = set()       
EPOCH_START = datetime(2024, 1, 1, tzinfo=timezone.utc)

class Accumulator:
    def __init__(self):
        self.buckets = {}          
        self.service_totals = {}  
    def add(self, service, window, latency, is_error):
        key = (service, window)
        row = self.buckets.get(key)
        if row is None:
            row = self.buckets[key] = [0, 0.0, 0.0, 0]
        count, total, peak, errs = row
        row[:] = (count + 1, round(total + latency, 6), max(peak, latency), errs + (1 if is_error else 0))
        tot = self.service_totals.get(service)
        if tot is None:
            tot = self.service_totals[service] = [0, 0.0, 0]
        c, s, e = tot
        tot[:] = (c + 1, round(s + latency, 6), e + (1 if is_error else 0))

def read_chunks(path, chunk_lines=CHUNK_LINES):
    with open(path, "r", errors="replace") as fh:
        chunk = []
        for line in fh:
            chunk.append(line)
            if len(chunk) >= chunk_lines:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def fold_batch(events, acc):
    if len(events) > 512:
        mid = len(events) // 2
        fold_batch(events[:mid], acc)
        fold_batch(events[mid:], acc)
        return
    for ev in events:
        acc.add(ev["service"], ev["window"], ev["latency"], ev["is_error"])

def parse_timestamp(ev):
    shift = ev.get("epoch_shift")
    ts_val = ev.get("timestamp")
    if shift is not None and int(shift) % 7 == 0:
        ns = int(ts_val)
        return EPOCH_START + timedelta(microseconds=ns // 1000)
    else:
        dt = datetime.fromisoformat(str(ts_val))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

def parse_chunk(lines, acc):
    parsed = malformed = dupes = 0
    batch = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            raw = json.loads(line)
            ev = {k.lower(): v for k, v in raw.items()}
            ts = parse_timestamp(ev)
            service = ev["service"].lower()
            latency = float(ev["latency_ms"])
            level = str(ev["level"]).lower()
            eid = ev["event_id"]
        except (ValueError, KeyError, TypeError, AttributeError):
            malformed += 1
            continue
        if eid in SEEN_IDS:
            dupes += 1
            continue
        SEEN_IDS.add(eid)
        batch.append({
            "service": service,
            "window": ts.replace(second=0, microsecond=0),
            "latency": latency,
            "is_error": level in ("error", "critical"),
            "raw": ev,
        })
        parsed += 1
        if len(batch) >= FOLD_SIZE:
            fold_batch(batch, acc)
            batch = []
    fold_batch(batch, acc)
    return parsed, malformed, dupes

def rolling_rows(acc):
    by_service = defaultdict(dict)
    for (svc, win), (cnt, tot, peak, errs) in acc.buckets.items():
        by_service[svc][win] = (cnt, tot, peak, errs)
    span = timedelta(minutes=WINDOW_MINUTES)
    for svc in sorted(by_service):
        recent = deque()
        for win in sorted(by_service[svc]):
            cnt, tot, peak, errs = by_service[svc][win]
            recent.append((win, cnt, tot))
            while recent[0][0] <= win - span:
                recent.popleft()
            rc = sum(r[1] for r in recent)
            rs = sum(r[2] for r in recent)
            yield (svc, win.strftime("%Y-%m-%dT%H:%M:00Z"), cnt, round(tot / cnt, 3),
                   peak, errs, round(rs / rc, 3))

def write_sqlite(acc, db_path):
    if os.path.exists(db_path):
        os.remove(db_path)
    con = sqlite3.connect(db_path)
    con.execute("""CREATE TABLE window_stats(
        service TEXT, window_start_utc TEXT, event_count INTEGER, avg_latency_ms REAL,
        max_latency_ms REAL, error_count INTEGER, rolling_avg_latency_5m_ms REAL,
        PRIMARY KEY(service, window_start_utc))""")
    con.executemany("INSERT INTO window_stats VALUES (?, ?, ?, ?, ?, ?, ?)", rolling_rows(acc))
    con.execute("CREATE TABLE service_totals(service TEXT PRIMARY KEY, event_count INTEGER, "
                "avg_latency_ms REAL, error_count INTEGER)")
    con.executemany("INSERT INTO service_totals VALUES (?, ?, ?, ?)",
                    [(s, c, round(t / c, 3), e) for s, (c, t, e) in sorted(acc.service_totals.items())])
    con.commit()
    con.close()

def main(path, db_path):
    t0 = time.time()
    acc = Accumulator()
    totals = [0, 0, 0]
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for res in pool.map(lambda chunk: parse_chunk(chunk, acc), read_chunks(path)):
            for i, v in enumerate(res):
                totals[i] += v
    write_sqlite(acc, db_path)
    print(f"parsed={totals[0]:,} malformed={totals[1]:,} duplicates={totals[2]:,} "
          f"windows={len(acc.buckets):,} elapsed={time.time() - t0:.1f}s -> {db_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Usage: python ingestion.py <input_file> <output_db>")
    main(sys.argv[1], sys.argv[2])
