from dotenv import load_dotenv

import logging
import os 


# Setup Logging
logging.basicConfig(
    level=logging.INFO, # Set the logging level
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )
LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")


# Setup .env
load_dotenv(override=True)

PROXY = os.getenv('PROXY')
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
