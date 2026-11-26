"""Read-only: which quoted stocks stopped trading, and does idx_suspension cover them?

The slow way asks the database one question per symbol (944 round trips). This asks a
handful and only digs where the answer can still change:

  1   query   recent sessions                        (calendar)
  1   query   symbols quoted on the last session     (universe)
  N   queries symbols with volume > 0 in the window  (N = --sessions)
  2   queries idx_suspension
  *   queries trades after each stale stock's newest suspension date

A stock is stale when it is quoted but did not trade in the window. Covered means the
newest suspension row is not contradicted by the tape: no trade after that date. The
per-date queries only return rows for stocks that kept trading, so they are near-empty.

Usage:
    python tools/verify_suspension_coverage.py
    python tools/verify_suspension_coverage.py --sessions 10 --symbols ABBA.JK,META.JK
"""
import argparse
import collections
import json
import os
import urllib.parse
import urllib.request

PAGE = 1000


def rest(url: str, key: str, path: str, params: dict | None = None) -> list[dict]:
    rows, start = [], 0
    while True:
        query = f'{url}/rest/v1/{path}'
        if params:
            query += '?' + urllib.parse.urlencode(params)
        request = urllib.request.Request(query, headers={
            'apikey': key, 'Authorization': f'Bearer {key}',
            'Range': f'{start}-{start + PAGE - 1}', 'Range-Unit': 'items'})
        chunk = json.load(urllib.request.urlopen(request, timeout=120))
        rows.extend(chunk)
        if len(chunk) < PAGE:
            return rows
        start += PAGE


def main() -> int:
    parser = argparse.ArgumentParser(description='Suspension coverage for stocks that stopped trading')
    parser.add_argument('--url', default=os.getenv('SUPABASE_URL'))
    parser.add_argument('--key', default=os.getenv('SUPABASE_KEY'))
    parser.add_argument('--sessions', type=int, default=7,
                        help='lookback in trading sessions (default 7)')
    parser.add_argument('--symbols', default='', help='only these symbols (comma separated)')
    parser.add_argument('--fail-on', default='', choices=('', 'missing', 'contradicted', 'any'),
                        help='exit 1 when this bucket is non-empty: missing|contradicted|any')
    args = parser.parse_args()
    if not args.url or not args.key:
        parser.error('SUPABASE_URL / SUPABASE_KEY missing (env or --url/--key)')

    calendar = sorted({r['date'][:10] for r in rest(args.url, args.key, 'idx_daily_data', {
        'select': 'date', 'symbol': 'eq.BBCA.JK', 'order': 'date.desc', 'limit': 30})})
    if len(calendar) < args.sessions + 1:
        parser.error(f'only {len(calendar)} sessions found, need {args.sessions + 1}')
    window, cutoff = calendar[-args.sessions:], calendar[-args.sessions]

    universe = {r['symbol'] for r in rest(args.url, args.key, 'idx_daily_data', {
        'select': 'symbol', 'date': f'eq.{calendar[-1]}'})}
    if args.symbols:
        universe &= {s.strip() for s in args.symbols.split(',') if s.strip()}

    traded = set()
    for day in window:
        traded.update(r['symbol'] for r in rest(args.url, args.key, 'idx_daily_data', {
            'select': 'symbol', 'date': f'eq.{day}', 'volume': 'gt.0'}))

    stale = sorted(universe - traded)

    suspensions = collections.defaultdict(list)
    for row in rest(args.url, args.key, 'idx_suspension',
                    {'select': 'symbol,suspension_date,reason'}):
        suspensions[row['symbol']].append(row)

    newest = {s: max(r['suspension_date'] for r in suspensions[s]) for s in stale
              if s in suspensions}

    # Stocks whose newest suspension row is contradicted by later trading: one query
    # per distinct date, so an all-clear costs nothing.
    by_date = collections.defaultdict(list)
    for symbol, date in newest.items():
        by_date[date].append(symbol)
    after: dict[str, str] = {}
    for date, symbols in sorted(by_date.items()):
        traded_after = rest(args.url, args.key, 'idx_daily_data', {
            'select': 'symbol,date', 'volume': 'gt.0', 'date': f'gt.{date}',
            'symbol': f'in.({",".join(sorted(symbols))})', 'order': 'date.desc'})
        for row in traded_after:
            day = row['date'][:10]
            if day > after.get(row['symbol'], ''):
                after[row['symbol']] = day

    covered = [(s, newest[s]) for s in stale if s in newest and s not in after]
    contradicted = [(s, newest[s], after[s]) for s in stale if s in newest and s in after]
    missing = [s for s in stale if s not in suspensions]

    print(f"sessions {window[0]}..{window[-1]} | quoted {len(universe)} | stopped trading {len(stale)}")
    print(f"  suspension covers the stop (no trade after it) : {len(covered)}")
    print(f"  traded again, so the row is history            : {len(contradicted)}")
    print(f"  no suspension row at all                       : {len(missing)}")
    if args.fail_on == 'any':
        failed = bool(missing or contradicted)
    elif args.fail_on == 'missing':
        failed = bool(missing)
    elif args.fail_on == 'contradicted':
        failed = bool(contradicted)
    else:
        failed = False

    if missing:
        print('\nno record:')
        for symbol in missing:
            print(f"  {symbol}")
    if contradicted:
        print('\nnewest row is older than the last trade (re-halted without a record):')
        for symbol, date, day in sorted(contradicted, key=lambda x: x[2], reverse=True):
            print(f"  {symbol:10} row {date}  traded after on {day}")
    if args.fail_on and failed:
        print(f"\nFAIL: --fail-on {args.fail_on}")
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
