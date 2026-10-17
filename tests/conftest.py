# Share test setup so each test can focus on the behaviour it checks.
# Load fixed API data, create isolated schemas in a dedicated test database,
# and provide a controlled worker clock and helpers for creating saved collections.

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv
from psycopg.types.json import Jsonb
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from pipeline import storage
from pipeline.ingestion import jobs, validation
from pipeline.ingestion.client import load_assets

ASSET_IDS, QUOTE_CURRENCY = load_assets()
NOW = datetime(2026, 10, 14, 12, tzinfo=timezone.utc)


def pytest_addoption(parser):
    parser.addoption('--require-integration', action='store_true', help='Fail instead of skipping PostgreSQL tests')


@pytest.fixture
def raw_response():
    return (Path(__file__).parent / 'fixtures' / 'markets.json').read_bytes()


@pytest.fixture
def db(request):
    load_dotenv(Path(__file__).resolve().parents[1] / '.env', override=False)
    url = os.environ.get('TEST_DATABASE_URL')
    if not url:
        if request.config.getoption('--require-integration'):
            pytest.fail('TEST_DATABASE_URL is required for this test run.')
        pytest.skip('Set TEST_DATABASE_URL to run PostgreSQL integration tests.')
    name = conninfo_to_dict(url).get('dbname', '')
    if not name.endswith('_test'):
        pytest.fail('Integration tests require a dedicated database ending in _test.')
    schema = 'test_' + uuid4().hex
    admin = storage.connect(url)
    connection = None
    try:
        admin.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
        test_url = make_conninfo(url, options='-c search_path=' + schema)
        connection = storage.connect(test_url)
        storage.initialize_database(connection)
        yield connection, test_url
    finally:
        if connection is not None:
            connection.close()
        # Only the explicitly created schema in the dedicated test database is deleted.
        admin.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
        admin.close()


@pytest.fixture
def worker(db, monkeypatch):
    connection, url = db
    storage.acquire_worker(connection)
    clock = {'now': jobs.database_now(connection)}
    monkeypatch.setattr(jobs, 'database_now', lambda conn: clock['now'])
    return connection, url, clock


@pytest.fixture
def collections(db, raw_response):
    connection, url = db
    valid, _ = validation.validate_markets(raw_response, ASSET_IDS)
    connection.execute("INSERT INTO collector_schedule VALUES ('coingecko-usd', %s, NULL)", (NOW,))
    created = []
    def create(status='complete', assets=ASSET_IDS):
        response_id = uuid4()
        scheduled = NOW + timedelta(seconds=30 * len(created))
        connection.execute('''INSERT INTO collection_jobs (response_id, dataset_key,
            scheduled_at, requested_assets, quote_currency, next_attempt_at, attempt_count, status)
            VALUES (%s, 'coingecko-usd', %s, %s, 'usd', %s, 1, %s)''',
            (response_id, scheduled, Jsonb(list(ASSET_IDS)), scheduled, status))
        jobs.save_response(connection, response_id, {asset: valid[asset] for asset in assets}, NOW, 1)
        created.append(response_id)
        return response_id
    return connection, create
