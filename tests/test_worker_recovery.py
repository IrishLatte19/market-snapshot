# Check that PostgreSQL keeps collection work consistent across retries and restarts.
# Cover stable job IDs, reserved attempt budgets, partial-data preservation,
# expired retry windows and skipped polling ticks with a controlled clock.

from datetime import timedelta
from uuid import uuid4

import psycopg
import pytest

from pipeline import storage
from pipeline.ingestion import jobs, validation
from pipeline.ingestion.client import load_assets

ASSET_IDS, QUOTE_CURRENCY = load_assets()


def start_attempt(connection):
    job, wait = jobs.get_work(connection, ASSET_IDS, "usd")
    assert wait == 0
    return jobs.claim_attempt(connection, job['response_id'], uuid4())


# Stable job reservation: Repeated scheduling and claim replay do not create duplicates or consume another attempt.
@pytest.mark.integration
def test_unique_job_reservation_and_no_api_budget_reset(worker):
    connection, url, clock = worker
    first, _ = jobs.get_work(connection, ASSET_IDS, "usd")
    second, _ = jobs.get_work(connection, ASSET_IDS, "usd")
    assert first['response_id'] == second['response_id']
    token = uuid4()
    claimed = jobs.claim_attempt(connection, first['response_id'], token)
    replayed = jobs.claim_attempt(connection, first['response_id'], token)
    assert claimed['attempt_count'] == replayed['attempt_count'] == 1
    assert jobs.claim_attempt(connection, first['response_id'], uuid4()) is None
    assert connection.execute('SELECT count(*), max(try_count) FROM raw_coingecko_responses').fetchone() == (1, 1)
    assert connection.execute('SELECT count(*) FROM collection_jobs').fetchone()[0] == 1
    assert connection.execute('SELECT raw_response, success FROM raw_coingecko_responses').fetchone() == ({}, False)
    # Recovery may read one row because PostgreSQL rejects a second active job.
    with pytest.raises(psycopg.errors.UniqueViolation) as failure:
        connection.execute('''INSERT INTO collection_jobs
            (response_id, dataset_key, scheduled_at, requested_assets, quote_currency, next_attempt_at)
            SELECT %s, dataset_key, scheduled_at + interval '30 seconds',
                   requested_assets, quote_currency, next_attempt_at FROM collection_jobs''', (uuid4(),))
    assert failure.value.diag.constraint_name == 'one_active_collection'


# Restart preservation: Reopening the connection preserves saved assets and attempts already consumed.
@pytest.mark.integration
def test_restart_preserves_saved_data_and_attempt_budget(worker, raw_response):
    connection, url, clock = worker
    job = start_attempt(connection)
    records, _ = validation.validate_markets(raw_response, ASSET_IDS[:-1])
    now = clock['now']
    result = jobs.finalize_attempt(connection, job, records, now, now + timedelta(seconds=5))
    assert result['status'] == 'pending'
    clock['now'] += timedelta(seconds=5)
    interrupted = jobs.claim_attempt(connection, job['response_id'], uuid4())
    assert interrupted['attempt_count'] == 2
    connection.close()
    with storage.connect(url) as restarted:
        storage.acquire_worker(restarted)
        clock['now'] += timedelta(seconds=1)
        jobs.recover_jobs(restarted)
        restored = jobs.load_job(restarted, job['response_id'])
        assert restored['status'] == 'pending'
        assert set(restored['saved_assets']) == set(ASSET_IDS[:-1])
        assert restored['attempt_count'] == 2
        assert jobs.claim_attempt(restarted, restored['response_id'], uuid4()) is None
        clock['now'] += timedelta(seconds=5)
        final = jobs.claim_attempt(restarted, restored['response_id'], uuid4())
        assert final['attempt_count'] == 3


# Expiry and skipped ticks: Retains partial data without creating catch-up requests for missed polling times.
@pytest.mark.integration
def test_expired_job_preserves_partial_data_and_skips_missed_ticks(worker, raw_response):
    connection, url, clock = worker
    job = start_attempt(connection)
    records, _ = validation.validate_markets(raw_response, ASSET_IDS[:-1])
    now = clock['now']
    jobs.finalize_attempt(connection, job, records, now, now + timedelta(seconds=5))
    clock['now'] += timedelta(seconds=65)
    jobs.recover_jobs(connection)
    assert jobs.load_job(connection, job['response_id'])['status'] == 'partial'
    queued, wait = jobs.get_work(connection, ASSET_IDS, "usd")
    assert queued is None
    assert wait == pytest.approx(25)
    assert connection.execute('SELECT count(*) FROM collection_jobs').fetchone()[0] == 1


# Three-attempt limit: Stops after three attempts and retains assets saved before later failures.
@pytest.mark.integration
def test_three_attempts_then_partial_success_is_retained(worker, raw_response):
    connection, url, clock = worker
    records, _ = validation.validate_markets(raw_response, ASSET_IDS[:-1])
    for attempt in range(1, 4):
        job = start_attempt(connection)
        assert job['attempt_count'] == attempt
        result = jobs.finalize_attempt(connection, job, records if attempt == 1 else {},
                                       clock['now'], clock['now'] + timedelta(seconds=5))
        clock['now'] += timedelta(seconds=5)
    assert result['status'] == 'partial'
    jobs.recover_jobs(connection)
    assert jobs.load_job(connection, result['response_id'])['status'] == 'partial'
    assert connection.execute('SELECT success, try_count FROM raw_coingecko_responses').fetchone() == (True, 3)


# Retry deadline: Refuses a new attempt once the collection retry window has expired.
@pytest.mark.integration
def test_claim_after_deadline_is_denied(worker):
    connection, url, clock = worker
    queued, _ = jobs.get_work(connection, ASSET_IDS, "usd")
    clock['now'] += timedelta(seconds=30)
    assert jobs.claim_attempt(connection, queued['response_id'], uuid4()) is None
    assert connection.execute('SELECT count(*) FROM raw_coingecko_responses').fetchone()[0] == 0
