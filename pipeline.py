from supabase import create_client
from datetime import datetime, timedelta

from setup                                  import LOGGER, SUPABASE_KEY, SUPABASE_URL
from scraper_engine.api_requester           import APIRequester
from scraper_engine.idx_suspension_scraper  import (run_general_suspensions,
                                                  run_get_idx_suspension, write_quarantine)
from scraper_engine.l4                      import apply_l4, existing_keys, public_rows
from scraper_engine.sessions                import load_sessions, next_session_factory

import pandas as pd 
import argparse


def get_company_profile_symbol(supabase_client: create_client) -> list[str]:
    """
    Fetches the list of allowed symbols from the company profile table in Supabase.
    
    Args:
        supabase_client (create_client): The Supabase client instance.
    
    Returns:
        list[str]: A list of allowed symbols.
    """
    try:
        allowed_symbols = [data_symbol.get('symbol') for data_symbol in
                                supabase_client.from_("idx_company_profile").select("symbol").execute().data]

        return allowed_symbols
    except Exception as error:
        LOGGER.error(f"Error fetching company profile: {error}")
        return {}


def upsert_to_db(df_payload: pd.DataFrame | list[dict[str]], supabase_client: create_client):
    """
    Upserts the provided DataFrame to the Supabase database.
    
    Args:
        df_payload (pd.DataFrame): The DataFrame containing the data to upsert.
        supabase_client (create_client): The Supabase client instance.
    """
    try:
        if isinstance(df_payload, list):
            LOGGER.warning("No data to upsert. Exiting.")
            return
        
        if df_payload.empty:
            LOGGER.info("DataFrame is empty after cleaning. Skipping upsert.")
            return
        
        payload_list = public_rows(df_payload.to_dict(orient="records"))
        
        LOGGER.info(f"Check payload to upsert: \n{payload_list}")

        response = (
            supabase_client
            .table("idx_suspension")
            .upsert(
                payload_list, 
                on_conflict='symbol,suspension_date'
            )
            .execute()
        )

        if hasattr(response, 'data') and response.data is not None:
            LOGGER.info(f"Successfully upserted {len(payload_list)} records to database")
        
        else:
            LOGGER.error(f"Upsert returned unexpected response: {response}")
            raise RuntimeError(f"Upsert failed with response: {response}")

    except Exception as error:
        raise Exception(f"Error upserting to database: {error}")


# The daily window is a lookback, not one day: a notice published after the run
# (or a failed run) would otherwise never be fetched again and a whole halt cohort
# would vanish. Duplicates cost nothing - existing_keys and the L4 gate drop them.
LOOKBACK_DAYS = 7

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='IDX Suspension Scraper')

    today = datetime.now().strftime('%Y%m%d')
    lookback = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime('%Y%m%d')

    parser.add_argument('--start_date', default=lookback ,help='Start date in YYYYMMDD format (e.g., 20250801)')
    parser.add_argument('--end_date', default=today, help='End date in YYYYMMDD format (e.g., 20250807)')

    args = parser.parse_args()
    
    LOGGER.info(f'Running start date: {args.start_date} | end date: {args.end_date}')
    requester = APIRequester(start_date=args.start_date, end_date=args.end_date)
    
    supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

    allowed_symbols = get_company_profile_symbol(supabase_client)
    sessions = load_sessions(supabase_client)
    df_payload = run_get_idx_suspension(allowed_symbols, requester,
                                        next_session=next_session_factory(sessions))

    # The suspension endpoint carries only part of IDX's notices; the rest (tabular ones)
    # are filed as ordinary announcements. Both legs feed one gate and one upsert.
    general_rows, general_unreadable = run_general_suspensions(
        allowed_symbols, requester, args.start_date, args.end_date,
        next_session=next_session_factory(sessions))
    LOGGER.info(f"general announcement feed: {len(general_rows)} halt row(s), "
                f"{len(general_unreadable)} not read")

    attrs = getattr(df_payload, 'attrs', {})
    records = [] if isinstance(df_payload, list) else df_payload.to_dict(orient="records")
    records.extend(general_rows)
    unreadable = list(attrs.get('unreadable') or []) + general_unreadable

    # Filter continuation notices here as well: a daily run must not upsert a
    # "still suspended" announcement as if the halt started that day.
    if records:
        symbols = sorted({row['symbol'] for row in records})
        late: list[dict] = []
        kept = apply_l4(records, existing_keys(supabase_client, symbols),
                        supabase_client, sessions, log=LOGGER.info, quarantine=late)
        write_quarantine(late)
        LOGGER.info(f"L4 gate kept {len(kept)} of {len(records)} parsed row(s)")
        df_payload = pd.DataFrame(kept)
    else:
        df_payload = pd.DataFrame()

    upsert_to_db(df_payload, supabase_client)

    # A partial fetch must not pass as a clean run: the CI job has to go red so the
    # missing days get noticed and re-scraped.
    failures = attrs.get('failures') or []
    if failures:
        LOGGER.error(f"Run finished with {len(failures)} unprocessed announcement(s); exiting non-zero")
        raise SystemExit(1)

    # An unreadable mass list hides an unknown number of halts, so it fails the run
    # instead of leaving a note in the ledger nobody reads.
    if unreadable:
        LOGGER.error(f"{len(unreadable)} notice(s) still unreadable after parsing: "
                     f"{[u['kode'] for u in unreadable]}; exiting non-zero")
        raise SystemExit(1)

    # Example Run
    # python idx_suspension/suspension.py --start_date 20250801 --end_date 20250807 