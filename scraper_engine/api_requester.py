import json
import time
from io         import BytesIO

from bs4        import BeautifulSoup

from setup import PROXY, LOGGER
from idx_fetch import idx_get, _looks_right

import pandas as pd 
import fitz 


class APIRequester:
    # The general announcement feed has no date range: it is searched by keyword and
    # paged, so callers filter the returned PublishDate themselves.
    ANNOUNCEMENT_KEYWORDS = ('Suspensi', 'Penghentian Sementara')
    ANNOUNCEMENT_PAGE = 1000

    def __init__(self, start_date: str, end_date: str, proxy=PROXY, session=None):
        """
        Initializes the ProxyRequester class with the provided proxy

        Args:
            proxy (str, optional): the proxy to be used. Defaults to None. Example: 'brd-customer-xxx-zone-xxx:xxx@brd.superproxy.io:xxx'
            session (requests.Session, optional): an already-cleared IDX session
                (real browser cookies). When given, every fetch uses it and the
                fingerprint ladder is skipped.
        """
        # Set up SSL context to unverified
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        self.session = session
        
        # Setup api url
        self.root_url ='https://www.idx.co.id'
        self.api_url = f"https://www.idx.co.id/primary/NewsAnnouncement/GetSuspension?indexFrom=1&dateFrom={start_date}&dateTo={end_date}&pageSize=9999&lang=en&type=spt"
        self.api_url_suspend_more_six_month = "https://www.idx.co.id/id/perusahaan-tercatat/suspensi-6-bulan/"

    def fetch_general_suspensions(self, retries: int = 3) -> list[dict]:
        """Suspension items from the general announcement feed (impositions + reliefs).

        The suspension endpoint only carries a subset of them; IDX also files suspensions
        as ordinary announcements, which is the source for the tabular notices.
        """
        items: dict[str, dict] = {}
        for keyword in self.ANNOUNCEMENT_KEYWORDS:
            page, pages = 1, 1
            while page <= pages:
                url = ("https://www.idx.co.id/primary/NewsAnnouncement/GetAllAnnouncement"
                       f"?keywords={keyword}&pageNumber={page}&pageSize={self.ANNOUNCEMENT_PAGE}"
                       "&lang=id")
                last_error = None
                for attempt in range(1, retries + 1):
                    try:
                        if self.session is not None:
                            body = self._browser_get(url, kind="json")
                        else:
                            body = idx_get(url, kind="json", proxies=self.proxies)
                        payload = json.loads(body)
                        pages = int(payload.get('PageCount') or 1)
                        for item in payload.get('Items') or []:
                            items[item.get('AnnouncementNo') or item.get('Id')] = item
                        break
                    except Exception as error:
                        last_error = error
                        LOGGER.error(f"announcement feed attempt {attempt}/{retries} failed: {error}")
                        if attempt < retries:
                            time.sleep(attempt * 5)
                else:
                    # Both keys are needed downstream: a half-read feed would hide halts.
                    raise RuntimeError(f"announcement feed unavailable: {last_error}")
                page += 1
        return list(items.values())

    def _browser_get(self, url: str, kind: str) -> bytes:
        """One GET through the cleared browser session; the body shape is the status."""
        response = self.session.get(url, timeout=(10, 120), verify=False)
        response.raise_for_status()
        if not _looks_right(response, kind):
            raise RuntimeError(f"non-{kind} body (HTTP {response.status_code}) - challenge page")
        return response.content

    def fetch_url(self, url: str, retries: int = 3) -> str:
        """
        Fetches the content of a URL using the installed opener.

        Args:
            url (str): The URL to fetch.
            retries (int): Attempts before giving up.

        Returns:
            str: The decoded content of the URL.

        Raises:
            RuntimeError: when every attempt fails, so a blocked run is loud
                instead of silently producing an empty batch.
        """
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                if self.session is not None:
                    return self._browser_get(url if url else self.api_url, kind="json").decode()
                return idx_get(url if url else self.api_url, kind="json", proxies=self.proxies).decode()
            except Exception as error:
                last_error = error
                LOGGER.error(f"IDX API attempt {attempt}/{retries} failed: {error}")
                if attempt < retries:
                    time.sleep(attempt * 5)
        raise RuntimeError(f"IDX API unavailable after {retries} attempts: {last_error}")
    
    def fetch_pdf_file(self, pdf_url: str) -> fitz.Document:
        """ 
        Fetches a PDF file from the given URL and returns it as a fitz.Document object.

        Args:
            pdf_url (str): The URL of the PDF file to fetch.
        
        Returns:
            fitz.Document: The PDF document object containing the content of the PDF file.
        """
        pdf_url = pdf_url.strip()
        # The general announcement feed hands out absolute URLs; this endpoint relative.
        full_url = pdf_url if pdf_url.startswith('http') else self.root_url + pdf_url

        try:
            file_content = (self._browser_get(full_url, kind="raw") if self.session is not None
                            else idx_get(full_url, kind="raw", proxies=self.proxies))
        
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

        The page has carried unrelated workbooks (a potential-delisting list among
        them), so only a link that names a suspension is accepted. No match means no
        override, never the wrong file.

        Returns:
            str | None: The URL of the XLSX file if found, None if not found.
        """
        try:
            data = (self._browser_get(self.api_url_suspend_more_six_month, kind="raw")
                    if self.session is not None
                    else idx_get(self.api_url_suspend_more_six_month, kind="raw",
                                 proxies=self.proxies)).decode('utf-8')

            soup = BeautifulSoup(data, 'html.parser')
            candidates = [tag for tag in soup.find_all("a", href=lambda href: href and href.endswith(".xlsx"))]
            matches = [tag for tag in candidates if 'suspensi' in (tag.get('href') or '').lower()
                       or 'suspensi' in tag.get_text(' ').lower()]
            if not matches:
                LOGGER.warning("No suspension xlsx on the six-month page (found: %s), skipping override",
                               [tag.get('href') for tag in candidates][:5])
                return None

            xlsx_url = 'https://www.idx.co.id' + matches[0].get('href')
            LOGGER.info(f"Successfully found URL: {xlsx_url}")
            return xlsx_url

        except Exception as error:
            LOGGER.error(f"Error fetching the URL: {error}")
            return None

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
                file_bytes = (self._browser_get(xlsx_url, kind="raw") if self.session is not None
                              else idx_get(xlsx_url, kind="raw", proxies=self.proxies,
                                           timeout=(10, timeout)))

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