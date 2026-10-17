# Check the normalized structure and the rows saved in coingecko_metrics.
# Verify required fields, decimals, UTC timestamps and error logging, then check
# that database saves are all-or-nothing and do not duplicate or replace rows.

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
import simplejson

from pipeline import downstream, storage
from pipeline.ingestion import jobs, validation
from pipeline.ingestion.client import load_assets
from pipeline.ingestion.validation import validate_markets
from pipeline.transformations import markets as transformations
from pipeline.transformations.markets import normalize_markets

ASSET_IDS, QUOTE_CURRENCY = load_assets()


@pytest.fixture
def collection(raw_response):
    markets, _ = validate_markets(raw_response, ASSET_IDS)
    for market in markets.values():
        market['observed_at'] = '2026-10-14T14:00:00+02:00'
    return {'response_id': uuid4(), 'quote_currency': 'usd',
            'ingested_at': datetime(2026, 10, 14, 12, tzinfo=timezone.utc)}, markets


# Normalized structure: Keeps required fields, exact decimals and optional nulls, and converts timestamps to UTC.
def test_flatten_fields_precision_nulls_and_utc(collection):
    metadata, markets = collection
    rows = normalize_markets(metadata, markets)
    assert len(rows) == 5
    eth = next(row for row in rows if row['asset_id'] == 'ethereum')
    assert set(eth) == {'response_id', 'asset_id', 'quote_currency', 'price', 'market_cap',
                       'volume_24h', 'source_updated_at', 'observed_at', 'ingested_at'}
    assert eth['price'] == Decimal('2500.000000000000000001')
    assert eth['response_id'] == str(metadata['response_id'])
    assert eth['quote_currency'] == 'usd'
    assert eth['observed_at'] == eth['ingested_at'] == '2026-10-14T12:00:00Z'
    assert all(row['source_updated_at'].endswith('Z') for row in rows)
    assert next(row for row in rows if row['asset_id'] == 'solana')['market_cap'] is None


# Transformation error: Logs the failure and raises an error instead of returning incomplete output.
def test_transformation_error_is_logged_and_prevents_output(collection, caplog):
    metadata, markets = collection
    del markets['ethereum']['current_price']
    with caplog.at_level(logging.INFO, logger='market_etl.transformations'):
        with pytest.raises(ValueError, match='could not be normalized'):
            normalize_markets(metadata, markets)
    assert 'stage=transformation' in caplog.text and 'failed error=KeyError' in caplog.text


@pytest.fixture
def metrics_collection(db, raw_response):
    connection, _ = db
    storage.acquire_worker(connection)
    job, _ = jobs.get_work(connection, ASSET_IDS, QUOTE_CURRENCY)
    job = jobs.claim_attempt(connection, job['response_id'], uuid4())
    markets, _ = validation.validate_markets(raw_response, ASSET_IDS)
    now = jobs.database_now(connection)
    jobs.finalize_attempt(connection, job, markets, now, now + timedelta(seconds=5))
    saved = downstream.next_finalized_collection(connection, storage.DATASET, 'processed', 'default')
    return connection, saved


# Database types: Stores normalized values as precise numerics and timezone-aware UTC timestamps.
@pytest.mark.integration
def test_metrics_are_typed_precise_and_utc(metrics_collection):
    connection, saved = metrics_collection
    expected = Decimal('2500.000000000000000001')
    rows = downstream.ensure_metrics(connection, saved)
    assert len(rows) == 5
    eth = next(row for row in rows if row['asset_id'] == 'ethereum')
    assert set(eth) == {'response_id', 'asset_id', 'quote_currency', 'price', 'market_cap',
                       'volume_24h', 'source_updated_at', 'observed_at', 'ingested_at'}
    assert eth['response_id'] == saved['response_id']
    assert isinstance(eth['price'], Decimal) and eth['price'] == expected
    for row in rows:
        for field in ('price', 'market_cap', 'volume_24h'):
            assert row[field] is None or isinstance(row[field], Decimal)
        for field in ('source_updated_at', 'observed_at', 'ingested_at'):
            assert isinstance(row[field], datetime) and row[field].utcoffset() == timedelta(0)
    types = connection.execute('SELECT pg_typeof(price)::text, pg_typeof(observed_at)::text FROM coingecko_metrics LIMIT 1').fetchone()
    assert types == ('numeric', 'timestamp with time zone')
    assert next(row for row in rows if row['asset_id'] == 'solana')['market_cap'] is None


# Idempotent normalized saves: Saving again neither duplicates rows nor replaces their original values.
@pytest.mark.integration
def test_metrics_retries_do_not_duplicate_or_replace_saved_rows(metrics_collection):
    connection, saved = metrics_collection
    rows = downstream.ensure_metrics(connection, saved)
    altered = [dict(row, price=Decimal('1')) for row in rows]
    transformations.save_metrics(connection, altered)
    assert transformations.load_metrics(connection, saved['response_id']) == rows
    assert downstream.ensure_metrics(connection, saved) == rows


# Atomic normalized collection: A missing required price rolls back the entire collection insert.
@pytest.mark.integration
def test_invalid_row_rolls_back_entire_normalized_collection(metrics_collection):
    connection, saved = metrics_collection
    markets = simplejson.loads(saved['raw_response'], use_decimal=True)
    rows = normalize_markets(saved, markets)
    rows[-1]['price'] = None
    with pytest.raises(psycopg.IntegrityError):
        transformations.save_metrics(connection, rows)
    assert transformations.load_metrics(connection, saved['response_id']) == []
