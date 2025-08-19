from datetime   import datetime 

from setup import LOGGER
from api_requester import APIRequester

import pandas as pd
import json  
import re 
import os 


def prepare_df_suspend_six_month(requester: APIRequester) -> pd.DataFrame:
    """ 
    Prepares a DataFrame containing suspension data for stocks suspended for more than six months.

    Args:
        requester (APIRequester): An instance of APIRequester to fetch the data.
    
    Returns:
        pd.DataFrame: A DataFrame with columns 'Kode' and 'Tanggal Suspensi'.
    """
    df = requester.fetch_xlsx_file() 
    df = df[['Kode', "Tanggal Suspensi"]].copy()
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
    except Exception as error:
        LOGGER.error(f'Error getting pdf texts {error}')
    
    finally:
        if pdf_doc:
            pdf_doc.close()
    
    return full_pdf_doc


def get_date_from_pdf(full_pdf_doc: str) -> str | None:
    """ 
    Extracts the suspension date from the PDF document text based on specific patterns.

    Args:
        full_pdf_doc (str): The text content of the PDF document.
    
    Returns:
        str | None: The extracted suspension date in 'YYYY-MM-DD' format, or None.
    """
    text = " ".join(full_pdf_doc.split())
    date_pattern = r"(\d{1,2}\s+\w+\s+\d{4})"
    keywords = ["suspensi", "penghentian\s+sementara"]

    all_candidates = []

    for kw in keywords:
        # Only match keyword -> date within 200 chars
        pattern_after = rf"{kw}.{{0,200}}?(?:tanggal\s+)?{date_pattern}"
        matches = re.finditer(pattern_after, text, re.IGNORECASE)
        for m in matches:
            snippet = text[m.start():m.end()]
            all_candidates.append((m.group(1), snippet.lower()))

    # Try to matches that mention 'sesi ii' first
    sesi_matches = [date for date, snippet in all_candidates if "sesi ii" in snippet]
    if sesi_matches:
        return sesi_matches[-1] 

    # Otherwise, take the last match overall
    if all_candidates:
        return all_candidates[-1][0]

    return None


def get_reason(full_pdf_doc: str, symbol: str) -> str | None:
    """
    Extract suspension reason from PDF document text based on predefined patterns.
    
    Args:
        full_pdf_doc: Raw PDF document text
        symbol: Stock symbol for personalized messages
        
    Returns:
        Formatted reason string or None if no match found
    """
    # Normalize spacing and lowercase for consistent matching
    pdf_texts = " ".join(full_pdf_doc.split())
    pdf_texts_lower = pdf_texts.lower()
    
    # Define reason pattern, format: {search_pattern: formatted_message_to_return}
    reason_patterns = {
        "peningkatan harga kumulatif yang signifikan": 
            f"Terjadinya peningkatan harga kumulatif yang signifikan pada saham {symbol}",
        
        "cooling down sebagai bentuk perlindungan bagi investor":
            "Dalam rangka cooling down sebagai bentuk perlindungan bagi investor",
        
        "untuk melakukan pembubaran dan likuidasi":
            f"Berencana untuk melakukan pembubaran dan likuidasi {symbol}",
        
        "penurunan harga kumulatif yang signifikan":
            f"Terjadinya penurunan harga kumulatif yang signifikan pada saham {symbol}",
        
        "perihal penundaan pembayaran pelunasan pokok & bunga mtn xv pp properti tahun 2022 ke-12 (ppro15xxmf)":
            "Perihal Penundaan Pembayaran Pelunasan Pokok & Bunga MTN XV PP Properti Tahun 2022 Ke-12 (PPRO15XXMF)",
        
        "belum menyampaikan laporan keuangan auditan tahunan":
            "Belum menyampaikan laporan keuangan auditan tahunan",
        
        "berada dalam papan pemantauan khusus selama lebih dari 1 (satu) tahun berturut-turut":
            "Efek Perseroan telah berada dalam papan pemantauan khusus selama lebih dari 1 (satu) tahun berturut-turut",
        
        "pengalihan saham hasil pelaksanaan pembelian kembali saham":
            "Dalam rangka pengalihan saham hasil pelaksanaan pembelian kembali saham (buyback) dalam rangka delisting perseroan",
        
        "belum menyampaikan laporan keuangan interim per 31 maret 2025":
            "Belum menyampaikan laporan keuangan interim per 31 maret 2025 dan/atau belum melakukan pembayaran denda atas keterlambatan penyampaian laporan keuangan tersebut",
        
        "belum memenuhi ketentuan v.1.1.":
            "Belum memenuhi ketentuan V.1.1. dan/atau V.1.2. peraturan bursa nomor I-A",
        
        "keterlambatan pembayaran biaya pencatatan tahunan 2025":
            "Keterlambatan pembayaran biaya pencatatan tahunan 2025",
        
        "terdapat keraguan atas kelangsungan usaha perseroan":
            "Bursa menilai bahwa terdapat keraguan atas kelangsungan usaha perseroan"
    }
    
    # Special case Combined conditions
    has_price_increase = "peningkatan harga kumulatif yang signifikan" in pdf_texts_lower
    has_cooling_down = "cooling down sebagai bentuk perlindungan bagi investor" in pdf_texts_lower
    
    if has_price_increase and has_cooling_down:
        return (
            f"Terjadinya peningkatan harga kumulatif yang signifikan pada saham {symbol}, "
            f"dalam rangka cooling down sebagai bentuk perlindungan bagi investor"
        )
    
    for pattern, message in reason_patterns.items():
        if pattern in pdf_texts_lower:
            return message
    
    return None


def process_multiple_data_from_pdf(full_pdf_doc: str, symbol: str, 
                                   pdf_url: str, root_pdf_url: str) -> list[dict[str]]:
    """ 
    Processes a PDF document containing multiple suspension data for a given stock symbol.

    Args:
        full_pdf_doc (str): The text content of the PDF document.
        symbol (str): The stock symbol for which the data is being processed.
        pdf_url (str): The URL of the PDF file.
        root_pdf_url (str): The root URL for constructing the full PDF URL.
    
    Returns:
        list[dict[str]]: A list of dictionaries containing the processed suspension data.
    """
    text = " ".join(full_pdf_doc.split())

    results = []

    # Get reason
    reason = get_reason(text, symbol)

    decision_match = re.search(
        r"Atas dasar hal tersebut di atas, Bursa memutuskan untuk:(.*)",
        text,
        flags=re.S | re.I
    )

    # Work with only the decision section if found
    decision_text = decision_match.group(1) if decision_match else text
    LOGGER.info(f"decision text {decision_text}")

    # Split by "a." an  d "b."
    section_a_match = re.search(r"a\.(.*?)(?=b\.)", decision_text, flags=re.S | re.I)
    section_b_match = re.search(r"b\.(.*)", decision_text, flags=re.S | re.I)
    
    LOGGER.info(f"section_a {section_a_match}")

    # Date pattern
    date_pattern = r"(\d{1,2}\s+\w+\s+\d{4})"
    
    # Process Section A 
    if section_a_match:
        section_a = section_a_match.group(1)

        # Find date in section A
        date_match = re.search(date_pattern, section_a, flags=re.I)
        suspend_date = date_match.group(1) if date_match else None

        # Find symbols in parentheses
        symbols = re.findall(r"\(([A-Z]{4})\)", section_a)
        for sym in symbols:
            results.append({
                "symbol": f"{sym}.JK", 
                "pdf_url": root_pdf_url + pdf_url,
                "suspension_date": suspend_date, 
                "reason": reason
            })

    # Process Section B 
    if section_b_match:
        section_b = section_b_match.group(1)
        # Find symbols in parentheses
        symbols = re.findall(r"\(([A-Z]{4})\)", section_b)
        for sym in symbols:
            results.append({
                "symbol": f"{sym}.JK", 
                "pdf_url": root_pdf_url + pdf_url,
                "suspension_date": None, 
                "reason": reason
            })

    # Fallback if section a and b not appear
    if not section_a_match and not section_b_match:
        symbols = re.findall(r"\d+\.\s+([A-Z]{3,4})\b", text)
        suspend_date = get_date_from_pdf(text)
        for sym in symbols:
            results.append({
                "symbol": f"{sym}.JK", 
                "pdf_url": root_pdf_url + pdf_url,
                "suspension_date": suspend_date, 
                "reason": reason
            })

    return results


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
    suspend_dict = df_suspend_six_month.set_index('Kode')['Tanggal Suspensi'].to_dict()
    LOGGER.info(f"Check data suspend six month: \n{df_suspend_six_month.head(2)}")

    mask = df_payload['symbol'].isin(df_suspend_six_month['Kode'])
    LOGGER.info(f"Matched count suspend six month: {mask.sum()}")
    df_payload.loc[mask, 'reason'] = "Suspend more than 6 month"
    df_payload.loc[mask, 'suspension_date'] = df_payload.loc[mask, 'symbol'].map(suspend_dict)
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
    file_name = "idx_suspension_missing_data.csv"
    df_missing.to_csv(file_name, mode="a", header=False, index=False)
    LOGGER.info(f"Missing data saved to data_incomplete/idx_suspension_missing_data.csv with {len(df_missing)} rows")

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


def run_get_idx_suspension(allowed_symbols: list[str], requester: APIRequester) -> pd.DataFrame:
    """
    Main function to run the IDX suspension scraper.
    Processes the suspension data for allowed symbols and returns a DataFrame.

    Args:
        allowed_symbols (list[str]): List of allowed stock symbols to filter the results.  
        requester (APIRequester): An instance of APIRequester to handle API requests.
    
    Returns:
        pd.DataFrame: A DataFrame containing the processed suspension data.
    """
    final_payload = []
    root_pdf_url = requester.root_url

    response = requester.fetch_url(requester.api_url)
    if response == False:
        LOGGER.warning("Error accesing api url.") 
    
    datas = json.loads(response)

    data_results = datas.get('Results')
    LOGGER.info(f'Length data need to process: {len(data_results)}')

    for data in data_results:
        try:    
            # Get pdf url
            pdf_url = data.get('Data_Download')

            # Get full pdf text
            pdf_texts = get_pdf_texts(requester, pdf_url)
            
            # Check if multiple symbols
            title = data.get('Judul')

            if "(\u003E1 kode)" in title.lower() or ">1 kode" in title.lower():
                symbol = data.get('Kode')
                LOGGER.info(f"Process multiple data {symbol}")

                # Process multiple
                multiple_data = process_multiple_data_from_pdf(pdf_texts, symbol, pdf_url, root_pdf_url)

                # Filter out symbols not in allowed_symbols
                filtered_data = [
                    item for item in multiple_data 
                    if item.get('symbol') in allowed_symbols
                ]

                if filtered_data:  
                    final_payload.extend(filtered_data)
                else:
                    LOGGER.info(f"No allowed symbols found in multiple data")
            else: 
                # Get symbol and validate
                symbol = data.get('Kode')
                symbol = f"{symbol}.JK"
                if symbol not in allowed_symbols:
                    LOGGER.warning(f"Symbol {symbol} not in allowed symbols, skipping")
                    continue

                LOGGER.info(f"Process single data {symbol}")
                # Get date from pdf 
                date = get_date_from_pdf(pdf_texts)
                # Get reason from pdf
                reason = get_reason(pdf_texts, symbol)

                final_payload.append(
                    {
                        "symbol": symbol,
                        'pdf_url': root_pdf_url + pdf_url,
                        'suspension_date': date,
                        'reason': reason
                    }
                )

        except Exception as error:
            symbol_for_error = data.get('Kode', 'UNKNOWN')
            LOGGER.error(f"A critical error occurred while processing symbol {symbol_for_error}: {error}")
            continue
    
    LOGGER.info(f"Check final payload: {final_payload[:5]}")
    LOGGER.info(f"Successfully processed and found dates for {len(final_payload)} items.")
    
    df_payload = pd.DataFrame(final_payload)
    # Check dataframe suspend six month
    df_payload = check_suspend_six_month(df_payload)

    check_payload = df_payload.to_json(orient="records")
    LOGGER.info(check_payload)

    # Drop missing values and saved dataframe that has missing values as csv
    df_final_payload = clean_dataframe_payload(df_payload)

    return df_final_payload