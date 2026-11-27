from datetime   import datetime 

from setup          import LOGGER
from .api_requester import APIRequester
from .extract       import (extract_announcement, general_imposition, parse_effective_date,
                            parse_mass_list, parse_reason)

import pandas as pd
import json  
import re 
import os 

QUARANTINE_FILE = "data_incomplete/idx_suspension_quarantine.csv"


QUARANTINE_LIMIT = 3000


def write_quarantine(records: list[dict]) -> None:
    """Record skipped announcements with the verdict that explains the skip.

    Deduplicated against what is already on disk and capped, so a daily job that
    keeps hitting the same unparsable announcement does not grow the file without
    bound.
    """
    if not records:
        return
    os.makedirs('data_incomplete', exist_ok=True)
    frame = pd.DataFrame(records)
    if os.path.exists(QUARANTINE_FILE):
        existing = pd.read_csv(QUARANTINE_FILE, dtype=str).fillna('')
        frame = pd.concat([existing, frame.astype(str)], ignore_index=True)
    keys = ['symbol', 'announcement_date', 'verdict', 'pdf_url']
    frame = frame.drop_duplicates(subset=keys, keep='last').tail(QUARANTINE_LIMIT)
    frame.to_csv(QUARANTINE_FILE, index=False)
    LOGGER.info(f"Quarantine: {len(records)} skipped this run, {len(frame)} tracked "
                f"-> {QUARANTINE_FILE}")


def prepare_df_suspend_six_month(requester: APIRequester) -> pd.DataFrame:
    """ 
    Prepares a DataFrame containing suspension data for stocks suspended for more than six months.

    Args:
        requester (APIRequester): An instance of APIRequester to fetch the data.
    
    Returns:
        pd.DataFrame: A DataFrame with columns 'Kode' and 'Tanggal Suspensi'.
    """
    df = requester.fetch_xlsx_file() 
    if df.empty or not {'Kode', 'Tanggal Suspensi'}.issubset(df.columns):
        LOGGER.warning("Six-month workbook unusable (empty or wrong shape), skipping override")
        return pd.DataFrame()
    df = df[['Kode', "Tanggal Suspensi"]].copy()
    df['Tanggal Suspensi'] = pd.to_datetime(df['Tanggal Suspensi']).dt.strftime('%Y-%m-%d')
    return df 


def get_pdf_texts(requester: APIRequester, pdf_url: str) -> str:
    """ 
    Fetches the PDF file from the given URL and extracts its text content.

    Args:
        requester (APIRequester): An instance of APIRequester to handle the request.
        pdf_url (str): The URL of the PDF file to fetch.
    
    Returns:
        str: The text content of the PDF file.
    """
    pdf_doc = requester.fetch_pdf_file(pdf_url)

    full_pdf_doc = ""
    try:
        # Loop length pdf page
        for page_num in range(len(pdf_doc)):
            page = pdf_doc[page_num]
            text = page.get_text()
            full_pdf_doc+=text
    finally:
        if pdf_doc:
            pdf_doc.close()
    
    return full_pdf_doc


def check_suspend_six_month(df_payload: pd.DataFrame, requester: APIRequester) -> pd.DataFrame:
    """ 
    Checks if any symbols in the payload are suspended for more than six months

    Args:
        df_payload (pd.DataFrame): The DataFrame containing suspension data.
        requester (APIRequester): An instance of APIRequester to fetch the suspension data.
    
    Returns:
        pd.DataFrame: The updated DataFrame with suspension reasons and dates for symbols suspended for more
    """
    df_suspend_six_month = prepare_df_suspend_six_month(requester)
    if df_suspend_six_month.empty:
        LOGGER.warning("Six-month suspension list unavailable, skipping the override")
        return df_payload

    df_suspend_six_month['Kode'] = df_suspend_six_month['Kode'].apply(lambda x: x + '.JK')
    suspend_dict = df_suspend_six_month.set_index('Kode')['Tanggal Suspensi'].to_dict()

    LOGGER.info(f"Check data suspend six month: \n{df_suspend_six_month.head(2)}")

    mask = df_payload['symbol'].isin(df_suspend_six_month['Kode'])

    LOGGER.info(f"Matched count suspend six month: {mask.sum()}")

    # Only fill what the PDF parse could not establish: a specific reason or date
    # already extracted from the announcement must never be replaced by the generic
    # six-month wording or by the workbook's date.
    fill_reason = mask & df_payload['reason'].isna()
    fill_date = mask & df_payload['suspension_date'].isna()

    df_payload.loc[fill_reason, 'reason'] = "Suspend more than 6 month"
    df_payload.loc[fill_date, 'suspension_date'] = df_payload.loc[fill_date, 'symbol'].map(suspend_dict)

    LOGGER.info(f"Six-month override: reason filled {fill_reason.sum()}, date filled {fill_date.sum()}")
    return df_payload


def parse_mixed_date(date_str: str) -> str | None:
    """ 
    Parses a date string that may contain mixed formats, including Indonesian month names.

    Args:
        date_str (str): The date string to parse
    
    Returns:
        str | None: The parsed date in 'YYYY-MM-DD' format, or None if parsing fails
    """
    month_map = {
        'januari': '01', 'jan': '01',
        'februari': '02', 'feb': '02',
        'maret': '03', 'mar': '03',
        'april': '04', 'apr': '04',
        'mei': '05',
        'juni': '06', 'jun': '06',
        'juli': '07', 'jul': '07',
        'agustus': '08', 'agu': '08', 'agt': '08',
        'september': '09', 'sep': '09', 'sept': '09',
        'oktober': '10', 'okt': '10',
        'november': '11', 'nov': '11',
        'desember': '12', 'des': '12'
    }
    
    # Try direct parse (for YYYY-MM-DD and datetimes)
    try:
        return pd.to_datetime(date_str, errors='raise').date()
    except:
        pass

    # Lowercase for matching
    date_str = str(date_str).strip().lower()
    
    # Replace month names
    for month_name, month_num in month_map.items():
        date_str = re.sub(rf"\b{month_name}\b", month_num, date_str)

    # Normalize spaces
    clean_date = re.sub(r"\s+", " ", date_str)

    try:
        parsed_date = datetime.strptime(clean_date, "%d %m %Y").date()
        return parsed_date.strftime("%Y-%m-%d")
    except Exception as error:
        LOGGER.error(f"Failed to parse date '{clean_date}': {error}")
        return None


def process_incomplete_data(df_missing: pd.DataFrame, file_name: str):
    """ 
    Processes and saves incomplete data to a CSV file, handling duplicates.

    Args:
        df_missing (pd.DataFrame): The DataFrame containing missing data.
        file_name (str): The name of the CSV file to save the missing data.
    """
    if os.path.exists(file_name):
        try:
            existing_missing = pd.read_csv(file_name)
            # Combine existing and new missing data
            combined_missing = pd.concat([existing_missing, df_missing], ignore_index=True)
            # Remove duplicate
            combined_missing = combined_missing.drop_duplicates(subset=['symbol', 'pdf_url'])
            
            combined_missing.to_csv(file_name, index=False)
            new_rows = len(combined_missing) - len(existing_missing)
            LOGGER.info(f"Added {new_rows} new missing data rows (total: {len(combined_missing)} rows)")
        except Exception as error:
            LOGGER.warning(f"Could not read existing missing data file: {error} Appending new dataa")
    else:
        # File doesn't exist, create new one
        df_missing.to_csv(file_name, index=False)
        LOGGER.info(f"Missing data saved to {file_name} with {len(df_missing)} rows")


def clean_dataframe_payload(df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Cleans the DataFrame by removing rows with missing values and parsing dates. 
    Saves rows with missing data to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
    
    Returns:
        pd.DataFrame: The cleaned DataFrame with parsed dates and no missing values.
    """
    os.makedirs('data_incomplete', exist_ok=True)

    df_missing = df[df.isnull().any(axis=1)] 
    file_name = "data_incomplete/idx_suspension_missing_data.csv"
    
    # process_incomplete_data already appends the deduped rows; writing them again here
    # duplicated every missing row (566 lines for 442 unique pairs).
    if len(df_missing) > 0:
        process_incomplete_data(df_missing, file_name)
    LOGGER.info(f"Missing data saved to {file_name} with {len(df_missing)} rows")

    df_clean = df.dropna(subset=['suspension_date', 'reason']).copy()
    df_clean['suspension_date'] = (
        df_clean['suspension_date']
        .apply(parse_mixed_date)
        .astype(str)  
    )
    LOGGER.info(f"df after parse date: {df_clean['suspension_date'].head()}")

    df_clean = df_clean.drop_duplicates(subset=['symbol', 'suspension_date'])
    LOGGER.info(f"Data after removing missing values and Duplicate: {len(df_clean)} rows")
    return df_clean


def run_general_suspensions(allowed_symbols: list[str], requester: APIRequester,
                            start_date: str, end_date: str, next_session=None
                            ) -> tuple[list[dict], list[dict]]:
    """Halts published as ordinary announcements inside [start_date, end_date].

    The suspension endpoint only carries a subset of IDX's notices; the rest are filed in
    the general feed, where the decision lives in a table instead of a clause. The feed has
    no date filter, so the window is applied to PublishDate here.
    Returns (rows, unreadable): rows are ready for the L4 gate, unreadable lists notices
    that were flagged as a halt but yielded nothing.
    """
    allowed = set(allowed_symbols)
    rows: list[dict] = []
    unreadable: list[dict] = []

    for item in requester.fetch_general_suspensions():
        published = str(item.get('PublishDate') or '')[:10]
        if not published:
            continue
        compact = published.replace('-', '')
        if not (start_date <= compact <= end_date):
            continue
        if not general_imposition(str(item.get('Title') or '')):
            continue

        symbol = str(item.get('Code') or '').strip()
        if not symbol:
            continue
        if f'{symbol}.JK' not in allowed:
            continue

        try:
            attachments = item.get('Attachments')
            if isinstance(attachments, str):
                attachments = json.loads(attachments or '[]')
            attachments = attachments or []
        except (TypeError, ValueError) as error:
            LOGGER.warning(f"general feed {symbol} {published}: bad attachments list ({error})")
            attachments = []
        main = [a for a in attachments if str(a.get('IsAttachment')) == '0'] or attachments
        pdf_url = (main[0].get('FullSavePath') if main else '') or item.get('PdfPath') or ''
        if not pdf_url:
            unreadable.append({'kode': symbol, 'announcement_date': published,
                               'verdict': 'NO_PDF', 'detail': ''})
            continue

        try:
            pdf_text = get_pdf_texts(requester, pdf_url)
            extraction = extract_announcement(pdf_text, symbol, published, allowed,
                                              next_session=next_session)
        except Exception as error:
            LOGGER.error(f"general feed {symbol} {published}: {error}")
            unreadable.append({'kode': symbol, 'announcement_date': published,
                               'verdict': 'ERROR', 'detail': f'{type(error).__name__}'})
            continue

        if extraction.verdict == 'OK_START':
            rows.extend({'symbol': r.symbol, 'suspension_date': r.suspension_date,
                         'reason': r.reason, 'raw_date': r.raw_date, 'pdf_url': pdf_url}
                        for r in extraction.rows if r.suspension_date)
        elif not extraction.rows:
            unreadable.append({'kode': symbol, 'announcement_date': published,
                               'verdict': extraction.verdict,
                               'detail': extraction.detail or ''})

    if unreadable:
        LOGGER.error(f"{len(unreadable)} general-feed notice(s) not read: {unreadable[:3]}")
    return rows, unreadable


def run_get_idx_suspension(allowed_symbols: list[str], requester: APIRequester,
                           next_session=None) -> pd.DataFrame | list:
    """
    Main function to run the IDX suspension scraper.
    Processes the suspension data for allowed symbols and returns a DataFrame.

    Args:
        allowed_symbols: symbols present in idx_company_profile.
        requester: APIRequester for this window.
        next_session: callable mapping an ISO date to the next IDX trading day,
            used to normalise a halt that only starts in a later session of the
            announcement day. Without it such a row keeps the raw date.

    Args:
        allowed_symbols (list[str]): List of allowed stock symbols to filter the results.  
        requester (APIRequester): An instance of APIRequester to handle API requests.
    
    Returns:
        pd.DataFrame: A DataFrame containing the processed suspension data.
    """
    final_payload = []
    quarantine: list[dict] = []
    failures: list[dict] = []
    unreadable: list[dict] = []
    root_pdf_url = requester.root_url
    allowed = set(allowed_symbols)

    # fetch_url raises when IDX blocks every route: a blocked run must fail loudly
    # instead of looking like a quiet day with no suspensions.
    datas = json.loads(requester.fetch_url(requester.api_url))

    data_results = datas.get('Results')
    LOGGER.info(f'Length data need to process: {len(data_results)}')

    for data in data_results:
        try:
            pdf_url = data.get('Data_Download')
            announced = str(data.get('Date') or '')[:10] or None
            pdf_texts = get_pdf_texts(requester, pdf_url)

            extraction = extract_announcement(
                pdf_texts, data.get('Kode'), announced, allowed, next_session=next_session)
            LOGGER.info(f"{data.get('Kode')} -> {extraction.verdict} {extraction.detail or ''}")

            # A notice IDX flags as a multi-company list that yields no list (or a single
            # ticker scraped from prose) was not read. That is a parse failure, not a
            # quiet day: record it and let the run go red.
            flagged_multi = str(data.get('Kode') or '').startswith('>1')
            # Only "nothing came back" is a failure: a notice that yields one ticker may
            # simply be a single-company filing IDX flagged as a list, and failing on
            # those would flap the job red every time one is published.
            unread = extraction.verdict == 'PARSE_FAILED' or (
                flagged_multi and extraction.verdict != 'UNSUSPENSION'
                and not extraction.rows)
            if unread:
                unreadable.append({'kode': str(data.get('Kode')),
                                   'announcement_date': announced or '',
                                   'pdf_url': root_pdf_url + pdf_url,
                                   'verdict': extraction.verdict,
                                   'detail': extraction.detail or '',
                                   'tickers': len(extraction.rows)})

            if extraction.verdict == 'OK_START':
                final_payload.extend(
                    {"symbol": row.symbol,
                     'pdf_url': root_pdf_url + pdf_url,
                     'suspension_date': row.suspension_date,
                     'reason': row.reason,
                     'raw_date': row.raw_date}
                    for row in extraction.rows)
            else:
                # A mass list names many companies: log one ledger row per ticker so a
                # dropped list stays attributable, not a single opaque '>1' entry.
                for symbol in [row.symbol for row in extraction.rows] or \
                              [str(data.get('Kode') or 'UNKNOWN')]:
                    quarantine.append({
                        'symbol': symbol,
                        'announcement_date': announced or '',
                        'pdf_url': root_pdf_url + pdf_url,
                        'verdict': extraction.verdict,
                        'detail': extraction.detail or '',
                        'evidence': extraction.evidence or '',
                    })

        except Exception as error:
            symbol_for_error = data.get('Kode', 'UNKNOWN')
            LOGGER.error(f"A critical error occurred while processing symbol {symbol_for_error}: {error}")
            failures.append({'symbol': str(symbol_for_error),
                             'pdf_url': data.get('Data_Download'),
                             'error': f'{type(error).__name__}: {error}'})
            continue
    
    LOGGER.info(f"Check final payload: {final_payload[:5]}")
    LOGGER.info(f"Successfully processed and found dates for {len(final_payload)} items.")
    write_quarantine(quarantine)
    if failures:
        LOGGER.error(f"{len(failures)} announcement(s) failed to process: {failures[:5]}")
    
    if unreadable:
        LOGGER.error(f"{len(unreadable)} mass list(s) could not be read: {unreadable[:3]}")

    if not final_payload:
        LOGGER.info(f"final payload is empty: {final_payload}, terminating the rest process")
        empty = pd.DataFrame()
        empty.attrs['failures'] = failures
        empty.attrs['unreadable'] = unreadable
        return empty

    df_payload = pd.DataFrame(final_payload)
    # Check dataframe suspend six month
    df_payload = check_suspend_six_month(df_payload, requester)

    check_payload = df_payload.to_json(orient="records")
    LOGGER.info(check_payload)

    # Drop missing values and saved dataframe that has missing values as csv
    df_final_payload = clean_dataframe_payload(df_payload)
    df_final_payload.attrs['failures'] = failures
    df_final_payload.attrs['unreadable'] = unreadable
    
    return df_final_payload 
    
   