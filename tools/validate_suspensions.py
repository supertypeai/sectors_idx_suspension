"""Read-only validator: does every idx_suspension row agree with idx_daily_data?

A row is only meaningful if the symbol actually stopped trading at
suspension_date. This script checks that and reports buckets:

  genuine_start    traded the session before, nothing on the date
  lagged           halt started earlier than the recorded date
  traded_on_date   the symbol traded on its own suspension_date (date is wrong)
  illiquid         no trading in the window, cannot verify
  no_daily_rows    the symbol has no daily data around that date

Usage:
    python tools/validate_suspensions.py                # report
    python tools/validate_suspensions.py --fail-on traded_on_date
    python tools/validate_suspensions.py --symbols AAA.JK,BBB.JK --since 2025-01-01

Request budget: idx_suspension (paginated), the IHSG calendar (paginated), then one
(symbol, +-WINDOW_BEFORE/AFTER days) window per row, BATCH rows per request.
"""
import argparse
import datetime
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from bisect import bisect_left
from collections import Counter, defaultdict

PAGE = 1000
WINDOW_BEFORE = 6
WINDOW_AFTER = 6
BATCH = 12
SLEEP = 0.15
BUCKETS = ('genuine_start', 'lagged', 'traded_on_date', 'illiquid', 'no_daily_rows')


def rest(url: str, key: str, path: str) -> list[dict]:
    rows, start = [], 0
    while True:
        request = urllib.request.Request(
            f'{url}/rest/v1/{path}',
            headers={'apikey': key, 'Authorization': f'Bearer {key}',
                     'Range': f'{start}-{start + PAGE - 1}', 'Range-Unit': 'items'})
        chunk = json.load(urllib.request.urlopen(request, timeout=120))
        rows.extend(chunk)
        if len(chunk) < PAGE:
            return rows
        start += PAGE


def previous_session(calendar: list[str], day: str) -> str | None:
    """Last trading session strictly before `day` (bisect, so gaps are tolerated)."""
    position = bisect_left(calendar, day) - 1
    return calendar[position] if position >= 0 else None


def shift(day: str, days: int) -> str:
    date = datetime.date.fromisoformat(day)
    return (date + datetime.timedelta(days=days)).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description='Validate idx_suspension against idx_daily_data')
    parser.add_argument('--url', default=None, help='Supabase URL (default: env)')
    parser.add_argument('--key', default=None, help='Supabase key (default: env)')
    parser.add_argument('--symbols', default='', help='comma separated subset')
    parser.add_argument('--since', default='2018-01-01', help='only rows on/after this date')
    parser.add_argument('--fail-on', default='', help='bucket that should fail the run')
    parser.add_argument('--index', default='IHSG', help='calendar source index code')
    parser.add_argument('--proxy', default='BBCA.JK', help='liquid symbol that fills index gaps')
    args = parser.parse_args()
    if args.fail_on and args.fail_on not in BUCKETS:
        parser.error(f'--fail-on must be one of {", ".join(BUCKETS)}')

    if args.url and args.key:
        url, key = args.url, args.key
    else:
        from setup import SUPABASE_KEY, SUPABASE_URL
        url, key = SUPABASE_URL, SUPABASE_KEY
    if not url or not key:
        print('SUPABASE_URL / SUPABASE_KEY missing', file=sys.stderr)
        return 2

    query = ('idx_suspension?select=symbol,suspension_date,reason'
             f'&suspension_date=gte.{args.since}&order=symbol,suspension_date')
    if args.symbols:
        query += '&symbol=in.(' + urllib.parse.quote(args.symbols) + ')'
    rows = rest(url, key, query)
    # index_daily_data misses sessions that stocks traded (e.g. 2025-10-20), so the
    # calendar is the union of index rows and a liquid proxy's prices, and the
    # previous session is resolved by bisect rather than by adjacency.
    calendar = sorted({r['date'][:10] for r in rest(
        url, key, f'index_daily_data?select=date&index_code=eq.{args.index}&order=date')}
        | {r['date'][:10] for r in rest(
            url, key, f'idx_daily_data?select=date&symbol=eq.{args.proxy}&order=date')})
    print(f'rows {len(rows)} | sessions {len(calendar)}')

    windows = defaultdict(dict)
    for index in range(0, len(rows), BATCH):
        group = rows[index:index + BATCH]
        clause = ','.join(
            f'and(symbol.eq.{r["symbol"]},date.gte.{shift(r["suspension_date"], -WINDOW_BEFORE)}'
            f',date.lte.{shift(r["suspension_date"], WINDOW_AFTER)})' for r in group)
        try:
            data = rest(url, key, f'idx_daily_data?select=symbol,date,volume&or=({clause})')
        except urllib.error.HTTPError as error:
            print(f'request failed (HTTP {error.code}), stopping', file=sys.stderr)
            return 2
        for daily in data:
            day = daily['date'][:10]
            for row in group:
                if (daily['symbol'] == row['symbol']
                        and shift(row['suspension_date'], -WINDOW_BEFORE) <= day
                        <= shift(row['suspension_date'], WINDOW_AFTER)):
                    windows[(row['symbol'], row['suspension_date'])][day] = daily['volume'] or 0
        time.sleep(SLEEP)

    counts = Counter()
    examples = defaultdict(list)
    for row in rows:
        symbol, day = row['symbol'], row['suspension_date']
        window = windows.get((symbol, day), {})
        if not window:
            bucket = 'no_daily_rows'
        elif (window.get(day) or 0) > 0:
            bucket = 'traded_on_date'
        elif window.get(previous_session(calendar, day)) or 0:
            bucket = 'genuine_start'
        elif any(volume > 0 for date, volume in window.items() if date != day):
            bucket = 'lagged'
        else:
            bucket = 'illiquid'
        counts[bucket] += 1
        examples[bucket].append(f'{symbol} {day}')

    print()
    for bucket in BUCKETS:
        print(f'{counts.get(bucket, 0):5}  {bucket}')
        for example in examples[bucket][:5]:
            print(f'         {example}')

    if args.fail_on:
        failing = counts.get(args.fail_on, 0)
        print(f'\nfail-on {args.fail_on}: {failing}')
        return 1 if failing else 0
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
