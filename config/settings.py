import os

from dotenv import load_dotenv

load_dotenv() # reads .env (listed in .gitignore)
API_BASE_URL = os.environ["API_BASE_URL"] # KeyError at start-up if missing
API_USERNAME = os.environ["API_USERNAME"]
API_PASSWORD = os.environ["API_PASSWORD"]
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
LOOKBACK_MIN = int(os.getenv("LOOKBACK_MIN", "30"))
LAKE = os.getenv("LAKE_ROOT", "lake")
DB_HOST, DB_PORT = os.getenv("DB_HOST", "localhost"), os.getenv("DB_PORT", "5432")
DB_NAME, DB_USER = os.getenv("DB_NAME", "voltgrid_dw"), os.getenv("DB_USER", "voltgrid")
DB_PASSWORD = os.environ["DB_PASSWORD"]
PII_SALT = os.environ["PII_SALT"]

