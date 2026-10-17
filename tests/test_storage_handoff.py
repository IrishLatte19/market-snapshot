# Check that raw data is saved safely and handed to the processor only after commit.
# Cover repeatable merges, per-asset timestamps, bounded database retries,
# transaction rollback and separate ownership for collection and processing.

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pytest

from pipeline import storage, downstream
from pipeline.ingestion import jobs, validation
from pipeline.ingestion.client import load_assets

ASSET_IDS, QUOTE_CURRENCY = load_assets()
OBSERVED_AT = datetime(2026, 10, 14, 12, tzinfo=timezone.utc)


@pytest.fixture
def valid_markets(raw_response):
    return validation.validate_markets(raw_response, ASSET_IDS)[0]


# Idempotent raw saves: Saving again does not create another row or replace an asset already saved.
@pytest.mark.integration
def test_save_is_idempotent_and_does_not_replace_existing_assets(db, valid_markets):
    connection, url = db
    response_id = uuid4()
    jobs.save_response(connection, response_id, valid_markets, OBSERVED_AT, 1)
    jobs.save_response(connection, response_id, valid_markets, OBSERVED_AT, 1)
    assert connection.execute('SELECT count(*) FROM raw_coingecko_responses').fetchone()[0] == 1
    changed = valid_markets['ethereum'].copy()
    changed['current_price'] = 9999
    jobs.save_response(connection, response_id, {'ethereum': changed}, OBSERVED_AT, 2)
    saved = connection.execute('''SELECT raw_response->'ethereum'->>'current_price',
        success, try_count, ingested_at FROM raw_coingecko_responses''').fetchone()
    assert saved[:3] == ('2500.000000000000000001', True, 2)
    assert saved[3].tzinfo is not None


# Partial merges: Adds new assets while retaining earlier data, acquisition timestamps and the highest attempt count.
@pytest.mark.integration
def test_partial_merge_keeps_success_and_per_asset_acquisition_times(db, valid_markets):
    connection, url = db
    response_id = uuid4()
    partial = {asset_id: valid_markets[asset_id] for asset_id in ASSET_IDS[:-1]}
    jobs.save_response(connection, response_id, partial, OBSERVED_AT, 1)
    later = OBSERVED_AT.replace(second=5)
    jobs.save_response(connection, response_id, {'tether': valid_markets['tether']}, later, 2)
    jobs.save_response(connection, response_id, {}, later, 3)
    # An older save replay cannot lower the counter or replace accepted assets.
    jobs.save_response(connection, response_id, partial, OBSERVED_AT, 1)
    row = connection.execute('SELECT raw_response, success, try_count FROM raw_coingecko_responses').fetchone()
    assert set(row[0]) == set(ASSET_IDS)
    assert row[1:] == (True, 3)
    assert datetime.fromisoformat(row[0]['ethereum']['observed_at']) == OBSERVED_AT
    assert datetime.fromisoformat(row[0]['tether']['observed_at']) == later


# Bounded database retries: Eligible temporary errors receive three attempts with the same save arguments.
@pytest.mark.parametrize('error', [psycopg.OperationalError(), psycopg.errors.LockNotAvailable()])
def test_temporary_database_errors_have_bounded_retries(monkeypatch, error):
    connection = MagicMock(closed=False)
    save = MagicMock(side_effect=error)
    sleep = MagicMock()
    monkeypatch.setattr(jobs.time, 'sleep', sleep)
    with pytest.raises(type(error)):
        jobs.run_with_retry(connection, 'test-db', save, uuid4(), {}, OBSERVED_AT, 2)
    assert save.call_count == 3
    assert all(call == save.call_args_list[0] for call in save.call_args_list)
    assert [call.args[0] for call in sleep.call_args_list] == [1, 2]


# Non-retryable database errors: Errors outside the retry policy are raised immediately.
@pytest.mark.parametrize('error', [psycopg.errors.UndefinedTable(), psycopg.errors.CheckViolation()])
def test_errors_outside_database_retry_policy_are_not_retried(monkeypatch, error):
    connection = MagicMock()
    connection.closed = False
    save = MagicMock(side_effect=error)
    sleep = MagicMock()
    monkeypatch.setattr(jobs.time, 'sleep', sleep)
    with pytest.raises(type(error)):
        jobs.run_with_retry(connection, 'test-db', save, uuid4(), {}, OBSERVED_AT, 1)
    save.assert_called_once()
    sleep.assert_not_called()


def start_attempt(connection):
    job, wait = jobs.get_work(connection, ASSET_IDS, "usd")
    assert wait == 0
    return jobs.claim_attempt(connection, job['response_id'], uuid4())


# Atomic finalization: A failure before commit rolls back both market data and job progress.
@pytest.mark.integration
def test_atomic_finalization_rolls_back_payload_and_job_progress(worker, monkeypatch, raw_response):
    connection, url, clock = worker
    job = start_attempt(connection)
    records, _ = validation.validate_markets(raw_response, ASSET_IDS)
    monkeypatch.setattr(jobs, 'advance_schedule', MagicMock(side_effect=RuntimeError('simulated crash before commit')))
    with pytest.raises(RuntimeError, match='simulated crash'):
        jobs.finalize_attempt(connection, job, records, clock['now'], clock['now'] + timedelta(seconds=5))
    restored = jobs.load_job(connection, job['response_id'])
    assert restored['saved_assets'] == {}
    assert restored['status'] == 'running'
    assert restored['attempt_token'] == job['attempt_token']
    assert connection.execute('SELECT cooldown_until FROM collector_schedule').fetchone()[0] is None


# Committed-data handoff: The processor sees committed work, not unfinished or rolled-back finalization.
@pytest.mark.integration
@pytest.mark.parametrize('rollback', [False, True])
def test_only_committed_finalization_is_visible_to_processor(db, raw_response, rollback):
    connection, url = db
    storage.acquire_worker(connection)
    job, _ = jobs.get_work(connection, ASSET_IDS, QUOTE_CURRENCY)
    job = jobs.claim_attempt(connection, job['response_id'], uuid4())
    markets, _ = validation.validate_markets(raw_response, ASSET_IDS)
    now = jobs.database_now(connection)
    with storage.connect(url) as reader:
        assert downstream.next_finalized_collection(reader, storage.DATASET, 'raw', 'default') is None
        try:
            with connection.transaction():
                jobs.finalize_attempt(connection, job, markets, now, now + timedelta(seconds=5))
                assert downstream.next_finalized_collection(reader, storage.DATASET, 'raw', 'default') is None
                if rollback:
                    raise ValueError('Rollback the complete handoff')
        except ValueError:
            if not rollback:
                raise
        saved = downstream.next_finalized_collection(reader, storage.DATASET, 'raw', 'default')
        assert (saved is None) == rollback
        if saved:
            assert saved['response_id'] == job['response_id']


# Independent role ownership: Collector and processor can run together, but competing processors cannot own the same role.
@pytest.mark.integration
def test_collector_and_downstream_can_own_independent_roles_but_not_same_role(db):
    connection, url = db
    storage.acquire_worker(connection)
    with storage.connect(url) as processor:
        storage.acquire_worker(processor, downstream.LOCK_KEY)
        storage.require_owner(connection)
        storage.require_owner(processor, downstream.LOCK_KEY)
        with storage.connect(url) as competing:
            with pytest.raises(RuntimeError, match='Another worker'):
                storage.acquire_worker(competing, downstream.LOCK_KEY)
