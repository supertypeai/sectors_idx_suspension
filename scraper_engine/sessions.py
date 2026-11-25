"""IDX trading-session calendar.

A halt announced "sejak Sesi II/IV ..." on the announcement day only starts after
earlier sessions traded, so the row belongs on the next trading session. That must
be the next session on the exchange calendar, not the next calendar day: a Friday
halt would otherwise be recorded on a Saturday.
"""
from bisect import bisect_left, bisect_right

PAGE = 1000
# index_daily_data has gaps: 2025-10-20 is missing for every index code while
# individual stocks traded. A liquid name fills those holes, and the two sources are
# unioned so neither can hide a session.
PROXY_SYMBOL = "BBCA.JK"


def _dates(client, table: str, column: str, value: str) -> list[str]:
    dates, start = [], 0
    while True:
        page = (client.from_(table)
                .select("date")
                .eq(column, value)
                .order("date")
                .range(start, start + PAGE - 1)
                .execute().data)
        dates.extend(row["date"][:10] for row in page)
        if len(page) < PAGE:
            return dates
        start += PAGE


def load_sessions(client, index_code: str = "IHSG", proxy: str = PROXY_SYMBOL) -> list[str]:
    """Ascending ISO dates on which the exchange traded."""
    dates = set(_dates(client, "index_daily_data", "index_code", index_code))
    dates.update(_dates(client, "idx_daily_data", "symbol", proxy))
    return sorted(dates)


def next_session_factory(sessions: list[str]):
    """Callable: ISO date -> the first trading session strictly after it."""
    def next_session(day: str) -> str:
        position = bisect_right(sessions, day)
        return sessions[position] if position < len(sessions) else day
    return next_session


def prev_session_factory(sessions: list[str]):
    """Callable: ISO date -> the last trading session strictly before it."""
    def prev_session(day: str) -> str | None:
        position = bisect_left(sessions, day) - 1
        return sessions[position] if position >= 0 else None
    return prev_session
