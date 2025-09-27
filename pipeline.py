from supabase import create_client
from datetime import datetime, timedelta

from setup                                  import LOGGER, SUPABASE_KEY, SUPABASE_URL
from scraper_engine.api_requester           import APIRequester
from scraper_engine.idx_suspension_scraper  import run_get_idx_suspension

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


def upsert_to_db(df_payload: pd.DataFrame | list, supabase_client: create_client):
    """
    Upserts the provided DataFrame to the Supabase database.
    
    Args:
        df_payload (pd.DataFrame): The DataFrame containing the data to upsert.
        supabase_client (create_client): The Supabase client instance.
    """
    try:
        if not df_payload:
            LOGGER.warning("No data to upsert. Exiting.")
            return
        
        payload_list = df_payload.to_dict(orient="records")
        
        LOGGER.info(f"Check payload to upsert: \n{payload_list}")

        supabase_client.table("idx_suspension").upsert(
            payload_list
        ).execute()

        LOGGER.info(f"Successfully upserted {len(payload_list)} data to database")
    except Exception as error:
        raise Exception(f"Error upserting to database: {error}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='IDX Suspension Scraper')

    today = datetime.now().strftime('%Y%m%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

    parser.add_argument('--start_date', default=yesterday ,help='Start date in YYYYMMDD format (e.g., 20250801)')
    parser.add_argument('--end_date', default=today, help='End date in YYYYMMDD format (e.g., 20250807)')

    args = parser.parse_args()
    
    requester = APIRequester(start_date=args.start_date, end_date=args.end_date)
    
    supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    allowed_symbols = get_company_profile_symbol(supabase_client)
    df_payload = run_get_idx_suspension(allowed_symbols, requester)
    upsert_to_db(df_payload, supabase_client)

    # Example Run
    # python idx_suspension/suspension.py --start_date 20250801 --end_date 20250807 