# This file fetches market data from CoinGecko for the configured assets and currency.
# Loads assets.json, make one HTTP request with timeouts, classify HTTP errors
# and interpret Retry-After. The collector and jobs decide when to try again.

import json
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

API_URL = 'https://api.coingecko.com/api/v3/coins/markets'

def load_assets():
    # Only collection startup needs this file; saved jobs carry their own configuration.
    assets_path = Path(__file__).resolve().parents[2] / 'datahub' / 'assets.json'
    with assets_path.open(encoding='utf-8') as file:
        assets = json.load(file)
    return tuple(assets['asset_ids']), assets['quote_currency']

# These HTTP responses may be temporary, so the collector allows bounded retries.
RETRYABLE_STATUSES = {
    429,  # Too Many Requests, provider rate-limiting, Retry-After
    500,  # Internal Server Error
    502,  # Bad Gateway
    503,  # Service Unavailable
    504,  # Gateway Timeout
}

# Respect the provider's retry delay, with a minimum wait of five seconds.
def retry_delay(retry_after):
    if not retry_after:
        return 5
    value = retry_after.strip() # just in case we get the value with whitespaces
    if value.isascii() and value.isdecimal():
        return max(5, float(value)) # wait atleast 5 seconds before retrying
    try:
        retry_at = parsedate_to_datetime(value) # in case CG returns a date in retry-after
    except (ValueError, TypeError, OverflowError):
        return 5
    if retry_at.tzinfo is None:
        return 5
    delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
    return max(5, delay)

# Make one request and return the response bytes with the time they were received.
def fetch_markets(session, asset_ids, quote_currency):
    parameters = {'ids': ','.join(asset_ids), 'vs_currency': quote_currency}
    with session.get(
        API_URL,
        params=parameters,
        timeout=(5, 10),  # Connection timeout and read timeout
        allow_redirects=False, # just a safe guard so that any URL redirects are avoided
    ) as response:
        if response.status_code in RETRYABLE_STATUSES:
            raise requests.HTTPError(response=response) # collector.py handles bounded retries 
        if not 200 <= response.status_code < 300: # response not in success codes and retryable codes like 401 authentication failure etc
            raise ValueError(f'CoinGecko returned non-retryable HTTP {response.status_code}.')
        return response.content, datetime.now(timezone.utc)
