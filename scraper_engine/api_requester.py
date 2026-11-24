from io         import BytesIO
from bs4        import BeautifulSoup

from setup import PROXY, LOGGER
from idx_fetch import idx_get

import pandas as pd 
import fitz 
import time


class APIRequester:
    def __init__(self, start_date: str, end_date: str, proxy=PROXY):
        """
        Initializes the ProxyRequester class with the provided proxy

        Args:
            proxy (str, optional): the proxy to be used. Defaults to None. Example: 'brd-customer-xxx-zone-xxx:xxx@brd.superproxy.io:xxx'
        """
        # Set up SSL context to unverified
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        
        # Setup api url
        self.root_url ='https://www.idx.co.id'
        self.api_url = f"https://www.idx.co.id/primary/NewsAnnouncement/GetSuspension?indexFrom=1&dateFrom={start_date}&dateTo={end_date}&pageSize=9999&lang=en&type=spt"
        self.api_url_suspend_more_six_month = "https://www.idx.co.id/id/perusahaan-tercatat/suspensi-6-bulan/"

    def fetch_url(self, url: str) -> str | bool:
        """
        Fetches the content of a URL using the installed opener.
        
        Args:
            url (str): The URL to fetch.

        Returns:
            str | bool: The decoded content of the URL as a string if successful,
                        False if an error occurs during fetching.
        """
        # Use the installed opener to fetch the URL
        try:
            return idx_get(url if url else self.api_url, kind="json", proxies=self.proxies).decode()
        except Exception as error:
            print(f"Error fetching URL: {error}")
            return False
    
    def fetch_pdf_file(self, pdf_url: str) -> fitz.Document:
        """ 
        Fetches a PDF file from the given URL and returns it as a fitz.Document object.

        Args:
            pdf_url (str): The URL of the PDF file to fetch.
        
        Returns:
            fitz.Document: The PDF document object containing the content of the PDF file.
        """
        pdf_url = pdf_url.strip()
        full_url = self.root_url + pdf_url

        try:
            file_content = idx_get(full_url, kind="raw", proxies=self.proxies)
        
            pdf_text = fitz.open(stream=file_content, filetype="pdf")
            return pdf_text

        except fitz.FileDataError as e:
            print(f"PDF corruption/format error for URL {full_url}: {str(e)}")
            raise
        
        except Exception as e:
            print(f"Unexpected error processing URL {full_url}: {type(e).__name__}: {str(e)}")
            raise

    def get_xlsx_url_proxy(self) -> str | None:
        """ 
        Gets the URL of the XLSX file containing suspension data from IDX.

        Args:
            None
        
        Returns:
            str | None: The URL of the XLSX file if found, None if not found.
        """
        try:
            data = idx_get(self.api_url_suspend_more_six_month, kind="raw",
                           proxies=self.proxies).decode('utf-8')

            soup = BeautifulSoup(data, 'html.parser')
            link_tag = soup.find("a", href=lambda href: href and href.endswith(".xlsx"))
            if link_tag:
                xlsx_url = 'https://www.idx.co.id' + link_tag.get('href')
                LOGGER.info(f"Successfully found URL: {xlsx_url}")
                return xlsx_url
            else:
                LOGGER.error("Error: Could not find the .xlsx link in the HTML.")
                return None
            
        except Exception as error:
            LOGGER.error(f"Error fetching the URL: {error}")
            return ""

    def fetch_xlsx_file(self) -> pd.DataFrame: 
        """ 
        Fetches the XLSX file from the IDX website and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: The DataFrame containing the data from the XLSX file.
        """
        xlsx_url = self.get_xlsx_url_proxy()

        if not xlsx_url:
            LOGGER.warning("Failed to get XLSX URL. Returning empty DataFrame.")
            return pd.DataFrame()
        
        max_retries = 3
        timeout = 30

        for attempt in range(max_retries):
            try:
                LOGGER.info('Downloading xlsx file')
                # Download file as binary
                file_bytes = idx_get(xlsx_url, kind="raw", proxies=self.proxies, timeout=(10, timeout))

                df = pd.read_excel(BytesIO(file_bytes))
                return df
            
            except Exception as error:
                LOGGER.warning(f"Attempt {attempt + 1} failed: {error}") 
                
                if attempt < max_retries - 1:
                    # Exponential backoff
                    wait_time = (attempt + 1) * 2  
                    LOGGER.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    LOGGER.error(f"All {max_retries} attempts failed. Returning empty DataFrame.")
                    return pd.DataFrame()
       
        return pd.DataFrame()