import json
import requests
from datetime import datetime
from utils.config import GNEWS_API_KEY
import urllib.parse
from utils.logger import get_logger

logger = get_logger("fetch_news")

query = '"AI" AND ("NVIDIA" OR "Microsoft" OR "Apple" OR "Meta" OR "Google" OR "Amazon" OR "Tesla" OR "AMD" OR ' \
        '"Supermicro" OR "Palantir")'

encoded_query = urllib.parse.quote_plus(query)

countries = ['us', 'gb', 'de', 'jp', 'cn']

url = f"https://gnews.io/api/v4/search?q={encoded_query}&lang=en&country=us&max=100&token={GNEWS_API_KEY}"

def fetch_news(url):
    logger.info("start fetching news")
    response = requests.get(url)

