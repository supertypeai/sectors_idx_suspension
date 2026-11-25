"""L4 gate: keep only rows that start a halt.

Shared by the daily pipeline and the backfill runner so a continuation notice is
filtered the same way in both.

A row starts a halt when the symbol traded on the session immediately before the
row's raw (pre-normalisation) date. A halt starts where trading stops, so a notice
whose symbol was already silent is a continuation, dated too late, and is
quarantined as DATE_LATE with the last traded session as evidence.

Why those bounds:
  * the interval is open at both ends. A mid-session halt (POLY: Sesi IV on the
    announcement day, normalised to the next session) traded that morning, and that
    volume is not evidence that an older halt had ended. Using the raw date keeps
    the halt day itself out of the interval.
  * why so strict: counting sessions or allowing a "first-ever halt" exemption let
    two late-dated rows through - GAMA (traded 2024-09, notice 2026-10) and WSBP
    (traded 2026-06-30, notice 2026-09-28, actually halted by the 2026-07-01
    negative-equity suspension). Requiring the immediately preceding session removes
    both without inventing a date.
  * trade-off: a genuinely new listing that has never traded cannot be verified and
    is quarantined rather than guessed.
"""

PROBE_LIMIT = 50
EPOCH = '1900-01-01'   # "any time before": the tape is the only bound
PUBLIC_COLUMNS = ('symbol', 'suspension_date', 'reason', 'pdf_url')


def public_rows(payload: list[dict]) -> list[dict]:
    """Drop internal bookkeeping (raw_date) so upserts only send table columns."""
    return [{k: row.get(k) for k in PUBLIC_COLUMNS} for row in payload]


def existing_keys(client, symbols: list[str] | None = None) -> set[tuple[str, str]]:
    """(symbol, suspension_date) already recorded, optionally limited to symbols."""
    keys: set[tuple[str, str]] = set()
    if symbols is None:
        start = 0
        while True:
            page = (client.from_("idx_suspension")
                    .select("symbol,suspension_date")
                    .range(start, start + 999)
                    .execute().data)
            keys.update((row["symbol"], row["suspension_date"]) for row in page)
            if len(page) < 1000:
                return keys
            start += 1000
    for index in range(0, len(symbols), 100):
        page = (client.from_("idx_suspension")
                .select("symbol,suspension_date")
                .in_("symbol", symbols[index:index + 100])
                .execute().data)
        keys.update((row["symbol"], row["suspension_date"]) for row in page)
    return keys


def previous_session(sessions: list[str], day: str) -> str | None:
    """The trading session immediately before `day`."""
    earlier = [s for s in sessions if s < day]
    return earlier[-1] if earlier else None


def last_traded(client, symbol: str, after: str, before: str) -> str | None:
    """Latest session with volume strictly inside (after, before)."""
    rows = (client.from_("idx_daily_data")
            .select("date")
            .eq("symbol", symbol)
            .gt("date", after)
            .lt("date", before)
            .gt("volume", 0)
            .order("date", desc=True)
            .limit(PROBE_LIMIT)
            .execute().data)
    days = sorted({row["date"][:10] for row in rows})
    return days[-1] if days else None


def traded_sessions(client, symbol: str, after: str, before: str) -> int:
    """Distinct sessions with volume strictly between the two dates."""
    rows = (client.from_("idx_daily_data")
            .select("date")
            .eq("symbol", symbol)
            .gt("date", after)
            .lt("date", before)
            .gt("volume", 0)
            .limit(PROBE_LIMIT)
            .execute().data)
    return len({row["date"][:10] for row in rows})


def apply_l4(payload: list[dict], existing: set[tuple[str, str]], client,
             sessions: list[str], log=None, quarantine: list | None = None) -> list[dict]:
    """Keep only rows whose symbol traded on the session before the halt."""
    previous = previous_session
    kept = []
    for row in sorted(payload, key=lambda r: (r['symbol'], r['suspension_date'])):
        symbol, date = row['symbol'], row['suspension_date']
        upper = row.get('raw_date') or date
        if (symbol, date) in existing:
            if log:
                log(f"L4 skip {symbol} {date}: already recorded")
            continue
        session = previous(sessions, upper)
        stop = last_traded(client, symbol, EPOCH, upper) if session else None
        if session and stop == session:
            if log:
                log(f"L4 keep {symbol} {date}: traded {stop}, halt starts here")
            kept.append(row)
            continue
        if log:
            log(f"L4 quarantine {symbol} {date}: last traded {stop or 'never'} "
                f"(expected {session or 'n/a'})")
        if quarantine is not None:
            quarantine.append({
                'symbol': symbol,
                'announcement_date': date,
                'pdf_url': row.get('pdf_url', ''),
                'verdict': 'DATE_LATE',
                'detail': f'halt date {date} but last traded {stop or "never"}',
                'evidence': f'prev session {session or "n/a"}, volume 0 on {upper}',
            })
    return kept
