# Check that we request the right assets and recognize usable API responses.
# Cover HTTP error classification, basic retry delays, JSON validation, precise
# prices and partial responses using fixed data instead of the live API.

import json
from datetime import timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import requests

from pipeline.ingestion import client, validation
from pipeline.ingestion.client import load_assets

ASSET_IDS, QUOTE_CURRENCY = load_assets()


def response(status=200, body=b'[]'):
    result = MagicMock()
    result.__enter__.return_value = result
    result.status_code = status
    result.headers = {}
    result.content = body
    return result


# Correct request: Sends the configured assets, currency and HTTP timeouts.
def test_fetch_requests_correct_assets_and_currency(raw_response):
    session = MagicMock()
    session.get.return_value = response(body=raw_response)
    raw, observed = client.fetch_markets(session, ASSET_IDS, "usd")
    assert raw == raw_response
    assert observed.tzinfo == timezone.utc
    assert session.get.call_args.kwargs['params'] == {'ids': ','.join(ASSET_IDS), 'vs_currency': 'usd'}
    assert session.get.call_args.kwargs['timeout'] == (5, 10)


# Permanent HTTP error: Rejects a non-retryable response, such as HTTP 401.
def test_permanent_http_error_stops_without_retrying():
    session = MagicMock()
    session.get.return_value = response(status=401)
    with pytest.raises(ValueError, match='non-retryable HTTP 401'):
        client.fetch_markets(session, ASSET_IDS, "usd")
    assert session.get.call_count == 1


# Temporary HTTP errors: Recognizes HTTP 429, 500 and 503 as retryable.
@pytest.mark.parametrize('status', [429, 500, 503])
def test_temporary_http_errors_are_retryable(status):
    session = MagicMock()
    session.get.return_value = response(status=status)
    with pytest.raises(requests.HTTPError) as failure:
        client.fetch_markets(session, ASSET_IDS, "usd")
    assert failure.value.response.status_code == status


# Basic Retry-After: Uses a valid delay in seconds or the five-second fallback for an invalid value.
@pytest.mark.parametrize('header,expected', [('broken', 5), ('12', 12)])
def test_retry_after_seconds_and_invalid_headers(header, expected):
    assert client.retry_delay(header) == expected


# Valid response: Accepts usable records with precise prices, optional nulls and valid timestamps.
def test_valid_data_preserves_precision_nulls_and_timestamps(raw_response):
    accepted, errors = validation.validate_markets(raw_response, ASSET_IDS)
    assert list(accepted) == list(ASSET_IDS)
    assert accepted['ethereum']['current_price'] == Decimal('2500.000000000000000001')
    assert errors == {}


# Invalid documents: Rejects malformed JSON, an unexpected structure and non-standard NaN values.
@pytest.mark.parametrize('body', [b'not JSON', b'{}', b'[{"id":"ethereum","current_price":NaN}]'])
def test_invalid_documents(body):
    with pytest.raises(ValueError):
        validation.validate_markets(body, ASSET_IDS)


# Partial response: Keeps four usable assets when the fifth is missing, duplicated or has an invalid price.
@pytest.mark.parametrize('problem', ['missing', 'duplicate', 'invalid_price'])
def test_partial_response_accepts_other_four_assets(raw_response, problem):
    rows = json.loads(raw_response)
    if problem == 'missing':
        rows.pop()
    elif problem == 'duplicate':
        rows.append(rows[-1])
    else:
        rows[-1]['current_price'] = None
    accepted, errors = validation.validate_markets(json.dumps(rows).encode(), ASSET_IDS)
    assert list(accepted) == list(ASSET_IDS[:-1])
    assert 'tether' in errors


# Zero values: Accepts zero price, market cap and volume rather than treating them as missing.
def test_zero_is_valid_not_missing(raw_response):
    rows = json.loads(raw_response)
    rows[0].update(current_price=0, market_cap=0, total_volume=0)
    assert validation.validate_markets(json.dumps(rows).encode(), ASSET_IDS)[1] == {}
