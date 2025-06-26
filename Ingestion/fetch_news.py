import json
import os
import time
import requests
from datetime import datetime
from utils.config import GNEWS_API_KEY
import urllib.parse
from utils.logger import get_logger

logger = get_logger("fetch_news")

query = '"AI" AND ("NVIDIA" OR "Microsoft" OR "Apple" OR "Meta" OR "Google" OR "Amazon" OR "Tesla" OR "AMD" OR ' \
        '"Supermicro" OR "Palantir")'

encoded_query = urllib.parse.quote_plus(query)

BASE_URL = "https://gnews.io/api/v4/search"
MAX_RETRIES = 3
DEFAULT_WAIT = 10  # seconds
RETRYABLE_STATUS_CODES = {429, 503}


def retryable_request(url, params):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=(5, 10))

            if response.status_code in RETRYABLE_STATUS_CODES:
                retry_after = response.headers.get('Retry-After')
                wait_time = int(retry_after) if retry_after and retry_after.isdigit() else DEFAULT_WAIT * attempt
                logger.warning(f"Received error {response.status_code} on attempt {attempt}. Retrying in {wait_time}s")

                time.sleep(wait_time)
                continue

            return response

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed on attempt {attempt} - {e}")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(DEFAULT_WAIT * attempt)


def fetch_news():
    logger.info("Fetching news ...")
    params = {
        "q": encoded_query,
        "lang": "en",
        "country": 'us',
        "token": GNEWS_API_KEY,
        "max": 100
    }

    try:
        response = retryable_request(BASE_URL, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.error(f"Request Failed: {e}")
        return

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        logger.error(f"Invalid response {e}")
        return

    if "articles" not in data:
        logger.warning(f'No articles key in {data}')
        return

    articles = data['articles']
    if not articles:
        logger.info('No articles returned')
        return

    os.makedirs('data/raw', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H:%M%S')
    filename = f"data/raw/articles_{timestamp}.json"

    try:
        with open(filename, 'w') as f:
            json.dump(articles, f, indent=2)
        logger.info(f"Saved articles to {filename}")
    except Exception as e:
        logger.error(f'Failed to save articles to {filename}: {e}')


if __name__ == "__main__":
    fetch_news()
