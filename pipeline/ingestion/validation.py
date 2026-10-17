# This file checks whether the API response contains usable market records.
# Parses JSON with decimal precision, check required prices and timestamps,
# and returns usable assets alongside errors for missing or invalid records.

import json
from datetime import datetime
from decimal import Decimal, InvalidOperation


def reject_constant(value):
    raise ValueError('JSON contains NaN or Infinity.')


# Reject duplicate JSON keys instead of silently keeping only their last value.
def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('JSON contains duplicate object fields.')
        result[key] = value
    return result


# Require a usable price and source timestamp; market cap and volume may be missing.
def validate_asset(asset):
    for field in ('current_price', 'market_cap', 'total_volume'):
        value = asset.get(field)
        if value is None and field != 'current_price':
            continue
        if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
            raise ValueError(f'{field} must be numeric.')
        if value < 0:
            raise ValueError(f'{field} must be nonnegative.')
    timestamp = asset.get('last_updated')
    if not isinstance(timestamp, str):
        raise ValueError('last_updated is required.')
    try:
        parsed = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
    except ValueError:
        raise ValueError('last_updated must be a valid ISO 8601 timestamp.') from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('last_updated must include a timezone.')


# Keep usable assets and report the others separately so one bad record does not discard the rest.
def validate_markets(raw_response, expected_assets):
    try:
        text = raw_response.decode('utf-8')
        markets = json.loads(text, parse_float=Decimal,
                             parse_constant=reject_constant, object_pairs_hook=unique_fields)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, InvalidOperation):
        raise ValueError('Response is not a valid UTF-8 JSON document.') from None
    if not isinstance(markets, list):
        raise ValueError('Expected a JSON array of assets, not an API error object.')
    valid_markets = {}
    errors = {}
    for asset_id in expected_assets:
        matches = [row for row in markets if isinstance(row, dict) and row.get('id') == asset_id]
        if len(matches) != 1:
            errors[asset_id] = 'Missing or duplicate asset.'
            continue
        try:
            validate_asset(matches[0])
        except ValueError as error:
            errors[asset_id] = str(error)
        else:
            valid_markets[asset_id] = matches[0]
    return valid_markets, errors
