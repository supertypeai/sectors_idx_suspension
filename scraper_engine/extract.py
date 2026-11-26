"""Announcement extraction: dates, reasons, mass tables, unsuspension routing.

Every announcement ends in exactly one verdict, so a PDF that yields nothing is
visible instead of silently absent from idx_suspension.

Verdicts:
    OK_START        row(s) with a real halt date and a regulatory reason
    OK_CONTINUATION mass-list rows whose status is not "Active"
    UNSUSPENSION    first decision clause opens a suspension, it does not impose one
    NO_DATE         no date inside the decision block
    NO_REASON       no regulatory cause in the document
    NON_EQUITY      ticker is not a listed company (ETF, reksa dana, DINFRA)
    PARSE_FAILED    unexpected document shape

Dates are never invented: when the decision block cannot be read, the row is
quarantined rather than dated from the announcement header.
"""
import re

from dataclasses import dataclass, field
from typing import Callable

# ---------------------------------------------------------------- vocabulary
MONTHS = {}
for _num, _names in enumerate([
        ('januari', 'january', 'jan'), ('februari', 'february', 'feb'),
        ('maret', 'march', 'mar'), ('april', 'apr'), ('mei', 'may'),
        ('juni', 'june', 'jun'), ('juli', 'july', 'jul'),
        ('agustus', 'august', 'aug', 'agu', 'agt'),
        ('september', 'sep', 'sept'), ('oktober', 'october', 'okt', 'oct'),
        ('november', 'nov'), ('desember', 'december', 'des', 'dec')], 1):
    for _name in _names:
        MONTHS[_name] = _num

# "memandang perlu untuk" is how the cooling-down notices word the same decision
DECISION = r'(?:memutuskan\s+untuk|memandang\s+perlu\s+untuk|decided\s+to)'
ANCHOR = (r'(?:terhitung\s+sejak|efektif\s+(?:mulai|pada)|mulai\s+sesi|sejak\s+sesi'
          r'|pada\s+tanggal|effective\s+from)')
ID_DATE = r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})'
EN_DATE = r'([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})'
# Sesi I / opening auctions start the day, so only Sesi II and later means a
# halt that begins after the stock already traded. "Periodic Call Auction" alone
# is a trading venue, not a late session (2026-07-01 lists say "Session I of the
# Periodic Call Auction").
MID_SESSION = (r'sesi\s+(?:ii|iii|iv|2|3|4)\b'
               r'|session\s+(?:ii|iii|iv|2|3|4)\b')

# family -> canonical reason text stored in idx_suspension.reason
# Families that describe a board placement rather than an event-driven halt. A
# multi-company negative-equity / Special Monitoring list is not a discrete trading
# suspension for this table: the badge belongs to the board status. Single-company
# announcements that cite the same wording stay valid halts.
BOARD_STATUS_FAMILIES = {'special-monitoring'}
# The decision clause itself, before the ticker list: a board list that suspends trading
# here is a halt (2025-01-31, 2025-04-30, 2025-08-01 all place names on the special
# monitoring board AND suspend them), while a placement notice never says it suspends.
DECISION_SUSPENDS = re.compile(r'melakukan suspensi|pengh?entian sementara|suspend\w*', re.I)

REASONS: list[tuple[str, str, str]] = [
    ('peningkatan harga kumulatif yang signifikan',
     'price-increase', 'Terjadinya peningkatan harga kumulatif yang signifikan pada saham {symbol}'),
    ('penurunan harga kumulatif yang signifikan',
     'price-decrease', 'Terjadinya penurunan harga kumulatif yang signifikan pada saham {symbol}'),
    ('sebagai bentuk perlindungan bagi investor',
     'investor-protection', 'Dalam rangka cooling down sebagai bentuk perlindungan bagi investor'),
    ('penundaan pembayaran bunga obligasi',
     'bond-coupon-delay', 'Perseroan menunda pembayaran bunga obligasi'),
    ('penundaan pembayaran bunga sukuk',
     'sukuk-delay', 'Perseroan menunda pembayaran bunga sukuk mudharabah'),
    ('kesiapan dana pembayaran bunga',
     'coupon-funding', 'Perseroan belum menyiapkan dana pembayaran bunga obligasi/sukuk'),
    ('sanksi atas penyampaian laporan keuangan',
     'lk-sanction', 'Sanksi atas penyampaian laporan keuangan'),
    ('laporan keuangan interim',
     'lk-interim', 'Belum menyampaikan laporan keuangan interim'),
    ('laporan keuangan auditan',
     'lk-audited', 'Belum menyampaikan laporan keuangan auditan tahunan'),
    ('annual audited financial statements',
     'lk-audited', 'Belum menyampaikan laporan keuangan auditan tahunan'),
    ('audited financial statements',
     'lk-audited', 'Belum menyampaikan laporan keuangan auditan tahunan'),
    ('interim financial statements',
     'lk-interim', 'Belum menyampaikan laporan keuangan interim'),
    # 2026-02-18: annual listing fee sanction. The old phrase carried the year, so a
    # 50-ticker notice matched nothing and was dropped as NO_REASON.
    ('biaya pencatatan tahunan',
     'listing-fee', 'Keterlambatan pembayaran biaya pencatatan tahunan (annual listing fee)'),
    ('voluntary delisting',
     'delisting', 'Perseroan berencana melakukan voluntary delisting dan go private'),
    ('pembatalan pencatatan',
     'delisting-request', 'Permohonan pembatalan pencatatan (delisting) saham Perseroan'),
    # 2026-07-01: trading was halted for companies still trading while on the special
    # monitoring board, so the notice is a suspension, not a board placement.
    ('with active trading status',
     'board-suspension', 'Suspensi perdagangan Efek bagi Perusahaan Tercatat dengan status '
                         'perdagangan aktif di Papan Pemantauan Khusus'),
    ('papan pemantauan khusus',
     'special-monitoring', 'Efek Perseroan telah berada dalam papan pemantauan khusus'),
    ('special monitoring board',
     'special-monitoring', 'Efek Perseroan telah berada dalam papan pemantauan khusus'),
    ('ketidakpastian atas kelangsungan usaha',
     'going-concern', 'Sehubungan dengan adanya ketidakpastian atas kelangsungan usaha'),
]


@dataclass
class Row:
    symbol: str
    suspension_date: str | None   # None only for a row kept for the ledger, never inserted
    reason: str | None
    reason_family: str | None
    evidence: str | None
    raw_date: str | None = None   # date stated by IDX before any L3 shift


@dataclass
class Extraction:
    verdict: str
    rows: list[Row] = field(default_factory=list)
    evidence: str | None = None
    detail: str | None = None


def _flat(text: str) -> str:
    return ' '.join(text.split())


def decision_block(text: str) -> str:
    """Text from the decision clause onwards, bounded so footers cannot win."""
    flat = _flat(text)
    m = re.search(DECISION, flat, re.I)
    if not m:
        return ''
    return flat[m.start():m.start() + 1500]


def _iso(year: str, month: str, day: str) -> str | None:
    number = MONTHS.get(month.lower())
    if not number:
        return None
    return f'{int(year):04d}-{number:02d}-{int(day):02d}'


def parse_effective_date(text: str, announced: str | None = None,
                         next_session: Callable[[str], str] | None = None
                         ) -> tuple[str | None, str | None, str]:
    """Date the halt starts, read only from the decision block.

    A halt announced "sejak Sesi II/IV ..." on the announcement day itself only
    begins after earlier sessions traded, so it is normalised to the next trading
    session (weekends and holidays skipped by the caller's calendar). Without a
    calendar the raw date is returned and the caller decides.

    Returns (iso_date | None, evidence | None, kind) with kind in
    direct | mid_session | unresolved.
    """
    block = decision_block(text)
    if not block:
        return None, None, 'no_decision'
    for pattern, order in ((ID_DATE, 'dmy'), (EN_DATE, 'mdy')):
        m = re.search(ANCHOR + r'.{0,200}?' + pattern, block, re.I)
        if not m:
            continue
        if order == 'dmy':
            date = _iso(m.group(3), m.group(2), m.group(1))
        else:
            date = _iso(m.group(3), m.group(1), m.group(2))
        if not date:
            continue
        evidence = block[m.start():m.start() + 140]
        mid = re.search(MID_SESSION, evidence, re.I)
        if mid and announced and date == announced:
            if next_session is None:
                return date, evidence, 'unresolved'
            return next_session(date), evidence, 'mid_session'
        return date, evidence, 'direct'
    return None, None, 'no_date'


def parse_reason(text: str, symbol: str) -> tuple[str | None, str | None, str | None]:
    """(reason text, family, matched phrase). Title text is never used."""
    low = _flat(text).lower()
    for phrase, family, template in REASONS:
        if phrase in low:
            return template.format(symbol=symbol), family, phrase
    return None, None, None


# IDX files repeals under the same ">1 Code" flag as halts (20 of them in the current
# feed). They carry no decision block, so the subject line is what identifies them.
REPEAL = re.compile(r'pencabutan\s+(?:penghentian\s+sementara|suspensi)'
                    r'|pembukaan\s+kembali'
                    r'|penyesuaian\s+atas\s+penghentian\s+sementara', re.I)
# "Melakukan suspensi" imposes a halt; "tetap melakukan" keeps one. Anything else in the
# block (membuka/mencabut/menyesuaikan) is relief, even when the subject says repeal.
IMPOSES = re.compile(r'melakukan\s+(?:suspensi|penghentian)|tetap\s+melakukan', re.I)

def is_unsuspension(text: str) -> bool:
    """True when the FIRST decision clause opens a suspension.

    Later clauses may re-suspend (HDTX does), but a notice whose first act is
    "Membuka penghentian sementara" is not a new halt.
    """
    block = decision_block(text) or ''
    if REPEAL.search(' '.join(text.split())[:600]) and not IMPOSES.search(block):
        # A halt can cite an earlier repeal in its references, and a partial repeal still
        # says "membuka penghentian sementara" - only an imposing decision wins.
        return True
    if not block:
        return False
    first = re.split(r'(?:^|\s)2\.\s', block, maxsplit=1)[0].lower()
    return 'membuka penghentian sementara' in first


def parse_mass_list(text: str) -> list[tuple[str, str]]:
    """(ticker, status) pairs from a multi-company table; status may be ''.

    The table arrives in three shapes: a ticker alone, "10. KARW", and
    "KARW PT Meratus Jasa Prima Tbk" with the number on the line above. All three are
    a ticker, so a notice's list reads the same however the PDF wrapped it.
    """
    lines = [' '.join(line.split()) for line in text.split('\n')]
    found: dict[str, str] = {}
    for index, line in enumerate(lines):
        bare = re.fullmatch(r'(?:\d+\.\s*)?([A-Z]{4})', line)
        numbered = re.match(r'^\d+\.\s*([A-Z]{4})\b', line)
        named = re.match(r'^([A-Z]{4})\s+PT\b', line)
        hit = bare or numbered or named
        if not hit:
            continue
        ticker = hit.group(1)
        status = ''
        for probe in lines[index:index + 4]:
            hit = re.search(r'\b(Active|Suspended)\b', probe)
            if hit:
                status = hit.group(1)
                break
        found.setdefault(ticker, status)
    return list(found.items())


def extract_announcement(text: str, kode: str, announced: str | None,
                         allowed_symbols: set[str],
                         next_session: Callable[[str], str] | None = None) -> Extraction:
    """Route one announcement to rows or to a verdict. Never invents data."""
    kode = str(kode or '').strip()
    multi = kode.startswith('>1')

    if multi:
        # Some single-company notices are flagged multi: fall back to the first ticker
        # in parentheses (company codes come before warrant codes) before calling the
        # document a broken table.
        table = parse_mass_list(text)
        if not table:
            first = re.search(r'\(([A-Z]{4})\)', text)
            if first:
                kode, multi = first.group(1), False
        if multi and not table:
            return Extraction('PARSE_FAILED', detail='mass list without tickers')

    if not multi and f'{kode}.JK' not in allowed_symbols:
        return Extraction('NON_EQUITY', detail=kode)
    if is_unsuspension(text):
        return Extraction('UNSUSPENSION', detail='first clause opens a suspension')

    raw_date, evidence, kind = parse_effective_date(text, announced, None)
    if kind == 'unresolved' and next_session:
        # mid-session halt: the first fully halted session is the next one
        date, kind = next_session(raw_date), 'mid_session'
    else:
        date = raw_date
    if not date:
        # A date-less mass list still names the companies it halts. Carry the tickers
        # so the ledger can show them; the date stays unset rather than guessed.
        named = [Row(f'{t}.JK', None, None, '', evidence, None)
                 for t, _status in (table if multi else [])]
        return Extraction('NO_DATE', named, evidence, kind)

    if multi:
        if not table:
            return Extraction('PARSE_FAILED', detail='mass list without tickers')
        reason, family, phrase = parse_reason(text, 'Efek')
        if not reason:
            return Extraction('NO_REASON',
                              [Row(f'{t}.JK', date, None, '', evidence, raw_date)
                               for t, _status in table],
                              evidence, f'{len(table)} tickers, no cause')
        # A board placement is not a halt. The 2026-07-01 notice is a halt on board
        # names, so it carries its own reason family ('with active trading status')
        # and never reaches this branch.
        if family in BOARD_STATUS_FAMILIES:
            block_text = decision_block(text) or ''
            clause = ' '.join(re.split(r'\bNo\.|\bTicker\b|\bKode\b', block_text,
                                       maxsplit=1)[0].split())[:300]
            if not DECISION_SUSPENDS.search(clause):
                return Extraction('BOARD_STATUS', detail=f'{len(table)} tickers, board placement')
        # '' is an unreadable status, not "already suspended": the 2026-06-30 notice has
        # no parseable status column at all. Only an explicit "Suspended" drops a ticker;
        # the L4 gate then keeps the ones that really stopped trading on that date.
        active = [t for t, status in table if status != 'Suspended']
        if not active:
            return Extraction('OK_CONTINUATION',
                              detail=f'{len(table)} tickers already suspended')
        rows = [Row(f'{t}.JK', date, reason, family, evidence, raw_date)
                for t in active if f'{t}.JK' in allowed_symbols]
        skipped = [t for t in active if f'{t}.JK' not in allowed_symbols]
        if not rows:
            return Extraction('NON_EQUITY', detail=f'{len(skipped)} active tickers outside profile')
        return Extraction('OK_START', rows, evidence,
                          detail=f'{len(rows)} of {len(table)} tickers; skipped {len(skipped)}')

    symbol = f'{kode}.JK'
    if symbol not in allowed_symbols:
        return Extraction('NON_EQUITY', detail=symbol)
    reason, family, phrase = parse_reason(text, symbol)
    if not reason:
        return Extraction('NO_REASON', evidence=evidence)
    return Extraction('OK_START', [Row(symbol, date, reason, family, evidence, raw_date)],
                      evidence, detail=kind)
