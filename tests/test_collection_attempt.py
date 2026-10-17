# Check how one collection attempt connects fetching, validation and saving.
# Make sure only usable records reach finalization and retries request only
# assets that have not already been saved.

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from pipeline.ingestion import collector, jobs
from pipeline.ingestion.client import load_assets

ASSET_IDS, QUOTE_CURRENCY = load_assets()


@pytest.fixture
def job():
    return {'response_id': uuid4(), 'attempt_token': uuid4(), 'attempt_count': 1,
            'scheduled_at': datetime.now(timezone.utc), 'quote_currency': 'usd',
            'requested_assets': list(ASSET_IDS), 'saved_assets': {}, 'status': 'running'}


@pytest.fixture
def attempt_setup(monkeypatch, job):
    connection = MagicMock(closed=False)
    result = dict(job, status='complete', saved_assets={asset: {'observed_at': '2026-10-14T12:00:00Z'}
                                                     for asset in ASSET_IDS})
    save = MagicMock(return_value=(connection, result))
    monkeypatch.setattr(jobs, 'run_with_retry', save)
    return connection, save


# Valid-record handoff: Only usable records are passed to database finalization.
def test_only_valid_records_are_passed_to_finalization(monkeypatch, job, attempt_setup, raw_response):
    connection, save = attempt_setup
    rows = json.loads(raw_response)
    rows[-1]['current_price'] = None
    monkeypatch.setattr(collector, 'fetch_markets', MagicMock(return_value=(json.dumps(rows).encode(), job['scheduled_at'])))
    collector.collect_attempt(MagicMock(), connection, 'unused', job)
    assert set(save.call_args.args[4]) == set(ASSET_IDS[:-1])


# Missing-asset request: If four assets are saved, the next request asks only for the fifth.
def test_retry_requests_only_missing_asset(monkeypatch, job, attempt_setup, raw_response):
    connection, save = attempt_setup
    job['saved_assets'] = {asset: {} for asset in ASSET_IDS[:-1]}
    fetch = MagicMock(return_value=(raw_response, job['scheduled_at']))
    monkeypatch.setattr(collector, 'fetch_markets', fetch)
    session = MagicMock()
    collector.collect_attempt(session, connection, 'unused', job)
    fetch.assert_called_once_with(session, ['tether'], 'usd')
    assert set(save.call_args.args[4]) == {'tether'}
