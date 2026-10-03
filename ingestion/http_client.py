import random, time
import requests
from config.settings import API_BASE_URL, API_USERNAME, API_PASSWORD, MAX_RETRIES
from config.logger import get_logger

logger = get_logger("http")

def login():
    with requests.Session() as s:
        r = request_with_retry(s, "POST", f"{API_BASE_URL}/api/auth/login/",
                               json={"username": API_USERNAME, "password": API_PASSWORD})
    return r.json()["token"]

def make_session(token):
    s = requests.Session()
    s.headers.update({"Authorization": f"Token {token}"})
    return s