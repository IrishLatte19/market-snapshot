# This file focusus on keeping collection progress in PostgreSQL so a restart does not reset the work.
# Manages the 30-second schedule, reserve attempts, merge usable raw assets,
# finalize jobs, recover interrupted collections and retry eligible database errors.

from datetime import timedelta
import logging
import time
from uuid import uuid4

import psycopg
import simplejson
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from pipeline import storage
from pipeline.storage import DATASET, TERMINAL_STATUSES

POLL_INTERVAL_SECONDS = 30
MAX_HTTP_ATTEMPTS = 3
MAX_SAVE_ATTEMPTS = 3
logger = logging.getLogger('market_etl.jobs')


# Add newly usable assets to this collection without replacing assets already saved.
def save_response(connection, response_id, valid_markets, observed_at, try_count):
    payload = {}
    for asset_id, asset in valid_markets.items():
        record = asset.copy()
        record['observed_at'] = observed_at.isoformat()
        payload[asset_id] = record

    with connection.transaction():
        connection.execute('''
            INSERT INTO raw_coingecko_responses
                (response_id, updated_at, raw_response, success, try_count)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (response_id) DO UPDATE SET
                raw_response = EXCLUDED.raw_response || raw_coingecko_responses.raw_response,     -- Existing assets remain, a save retry cannot replace their contents
                success = raw_coingecko_responses.success OR EXCLUDED.success,
                try_count = GREATEST(raw_coingecko_responses.try_count, EXCLUDED.try_count),
                updated_at = GREATEST(raw_coingecko_responses.updated_at, EXCLUDED.updated_at)
        ''', (response_id, observed_at, Jsonb(payload, dumps=simplejson.dumps), bool(payload), try_count))


def database_now(connection):
    return connection.execute('SELECT clock_timestamp()').fetchone()[0]


def load_job(connection, response_id, for_update=False):
    query = '''SELECT j.*, COALESCE(r.raw_response, '{}'::jsonb) AS saved_assets
        FROM collection_jobs j LEFT JOIN raw_coingecko_responses r USING (response_id)
        WHERE j.response_id = %s'''
    if for_update:
        query += ' FOR UPDATE OF j'
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query, (response_id,))
        return cursor.fetchone()


def missing_assets(job):
    return [asset for asset in job['requested_assets'] if asset not in job['saved_assets']]


# Missing assets mean partial success when some data exists, or failure when none exists.
def outcome(job):
    if not missing_assets(job):
        return 'complete'
    return 'partial' if job['saved_assets'] else 'failed'


# Move to the next future polling tick rather than making catch-up requests for missed snapshots.
def advance_schedule(connection, now):
    row = connection.execute('SELECT next_poll_at FROM collector_schedule WHERE dataset_key = %s FOR UPDATE',
                             (DATASET,)).fetchone()
    if row and row[0] <= now:
        skipped = int((now - row[0]).total_seconds() // POLL_INTERVAL_SECONDS) + 1
        next_poll = row[0] + timedelta(seconds=skipped * POLL_INTERVAL_SECONDS)
        connection.execute('UPDATE collector_schedule SET next_poll_at = %s WHERE dataset_key = %s',
                           (next_poll, DATASET))
        logger.info('stage=scheduling skipped_ticks=%s', skipped)


# Resume interrupted work only if time and attempts remain; otherwise keep its data and close it.
def recover_jobs(connection, now=None):
    with connection.transaction():
        storage.require_owner(connection)
        now = now or database_now(connection)
        # The one_active_collection index permits at most one unfinished job.
        row = connection.execute('''SELECT response_id FROM collection_jobs
            WHERE dataset_key = %s AND status IN ('pending', 'running') FOR UPDATE''', (DATASET,)).fetchone()
        if row:
            job = load_job(connection, row[0])
            deadline = job['scheduled_at'] + timedelta(seconds=POLL_INTERVAL_SECONDS)
            if not missing_assets(job) or now >= deadline or job['attempt_count'] >= MAX_HTTP_ATTEMPTS:
                status = outcome(job)
                connection.execute('UPDATE collection_jobs SET status = %s, attempt_token = NULL WHERE response_id = %s',
                                   (status, row[0]))
            elif job['status'] == 'running':
                # A reservation survives crashes, even if HTTP was never sent.
                connection.execute('''UPDATE collection_jobs SET status = 'pending', attempt_token = NULL,
                    next_attempt_at = GREATEST(next_attempt_at, %s) WHERE response_id = %s''',
                    (now + timedelta(seconds=5), row[0]))
            logger.info('stage=recovery collection=%s saved assets retained', row[0])
        # Downtime does not generate a burst of obsolete latest-price requests.
        advance_schedule(connection, now)


# Find the next collection and its wait time while respecting retries, cooldowns and regular ticks.
def get_work(connection, asset_ids, quote_currency, now=None):
    with connection.transaction():
        storage.require_owner(connection)
        now = now or database_now(connection)
        connection.execute('''INSERT INTO collector_schedule(dataset_key, next_poll_at)
            VALUES (%s, %s) ON CONFLICT DO NOTHING''', (DATASET, now))
        next_poll, cooldown = connection.execute('''SELECT next_poll_at, cooldown_until
            FROM collector_schedule WHERE dataset_key = %s FOR UPDATE''', (DATASET,)).fetchone()
        row = connection.execute('''SELECT response_id FROM collection_jobs
            WHERE dataset_key = %s AND status IN ('pending', 'running') FOR UPDATE''', (DATASET,)).fetchone()
        if row:
            job = load_job(connection, row[0])
            if job['status'] == 'running':
                raise RuntimeError('An interrupted running job must be recovered before dispatch.')
            ready_at = max(job['next_attempt_at'], cooldown or job['next_attempt_at'])
            deadline = job['scheduled_at'] + timedelta(seconds=POLL_INTERVAL_SECONDS)
            if now >= deadline or ready_at >= deadline or job['attempt_count'] >= MAX_HTTP_ATTEMPTS:
                job['status'] = outcome(job)
                connection.execute('UPDATE collection_jobs SET status = %s WHERE response_id = %s',
                                   (job['status'], job['response_id']))
                advance_schedule(connection, now)
                return job, 0
            return job, max(0, (ready_at - now).total_seconds())
        if now < next_poll:
            return None, (next_poll - now).total_seconds()
        # A late wake-up may dispatch this tick, but never reconstruct older windows.
        if now >= next_poll + timedelta(seconds=POLL_INTERVAL_SECONDS):
            advance_schedule(connection, now)
            return None, 1
        response_id = uuid4()
        connection.execute('''INSERT INTO collection_jobs
            (response_id, dataset_key, scheduled_at, requested_assets, quote_currency, next_attempt_at)
            VALUES (%s, %s, %s, %s, %s, %s)''',
            (response_id, DATASET, next_poll, Jsonb(list(asset_ids)), quote_currency, now))
        connection.execute('UPDATE collector_schedule SET next_poll_at = %s WHERE dataset_key = %s',
                           (next_poll + timedelta(seconds=POLL_INTERVAL_SECONDS), DATASET))
        return load_job(connection, response_id), 0


# Reserve the attempt before calling the API so a restart cannot reset the retry budget.
def claim_attempt(connection, response_id, attempt_token, now=None):
    with connection.transaction():
        storage.require_owner(connection)
        now = now or database_now(connection)
        job = load_job(connection, response_id, for_update=True)
        deadline = job['scheduled_at'] + timedelta(seconds=POLL_INTERVAL_SECONDS)
        if job['status'] == 'running' and job['attempt_token'] == attempt_token:
            # A lost claim acknowledgement must not admit HTTP after the window expires.
            if now >= deadline:
                connection.execute('UPDATE collection_jobs SET status = %s WHERE response_id = %s',
                                   (outcome(job), response_id))
                return None
            return job  # The reservation committed but its acknowledgement was lost
        cooldown = connection.execute('SELECT cooldown_until FROM collector_schedule WHERE dataset_key = %s',
                                      (DATASET,)).fetchone()[0]
        if (job['status'] != 'pending' or now >= deadline or now < job['next_attempt_at']
                or (cooldown and now < cooldown) or job['attempt_count'] >= MAX_HTTP_ATTEMPTS):
            return None
        attempt = job['attempt_count'] + 1
        connection.execute('''UPDATE collection_jobs SET status = 'running', attempt_count = %s,
            attempt_token = %s, next_attempt_at = %s WHERE response_id = %s''',
            (attempt, attempt_token, now + timedelta(seconds=5), response_id))
        save_response(connection, response_id, {}, now, attempt)
        return load_job(connection, response_id)


# Check the attempt token, then commit the data, cooldown and next job status together.
def finalize_attempt(connection, job, valid_markets, observed_at, cooldown_until, permanent_error=False):
    with connection.transaction():
        storage.require_owner(connection)
        current = load_job(connection, job['response_id'], for_update=True)
        if current['attempt_token'] != job['attempt_token']:
            raise RuntimeError('Stale attempt result rejected; this job has been recovered or reassigned.')
        if current['status'] != 'running':
            return current  # Idempotent replay after an uncertain commit acknowledgement
        save_response(connection, job['response_id'], valid_markets, observed_at, job['attempt_count'])
        connection.execute('''UPDATE collector_schedule
            SET cooldown_until = GREATEST(cooldown_until, %s) WHERE dataset_key = %s''', (cooldown_until, DATASET))
        current = load_job(connection, job['response_id'])
        now = database_now(connection)
        deadline = job['scheduled_at'] + timedelta(seconds=POLL_INTERVAL_SECONDS)
        if (not missing_assets(current) or permanent_error or job['attempt_count'] >= MAX_HTTP_ATTEMPTS
                or now >= deadline or cooldown_until >= deadline):
            status = outcome(current)
        else:
            status = 'pending'
        connection.execute('''UPDATE collection_jobs SET status = %s, next_attempt_at = %s
            WHERE response_id = %s''', (status, cooldown_until, job['response_id']))
        advance_schedule(connection, now)
        return load_job(connection, job['response_id'])


def run_with_retry(connection, database_url, operation, *args):
    """Retry one database operation, never the HTTP request around it."""
    try:
        for attempt in range(1, MAX_SAVE_ATTEMPTS + 1):
            try:
                if connection.closed:
                    connection = storage.connect(database_url)
                    storage.acquire_worker(connection)
                return connection, operation(connection, *args)
            except psycopg.Error as error:
                connection.close()
                code = error.sqlstate
                if code:
                    retryable = code.startswith('08') or code in ('55P03', '57014')
                else:
                    retryable = isinstance(error, psycopg.OperationalError)
                if not retryable or attempt == MAX_SAVE_ATTEMPTS:
                    raise
                logger.warning('stage=storage operation=%s attempt=%s failed error=%s retry_in_seconds=%s',
                               getattr(operation, '__name__', 'database'), attempt, type(error).__name__, attempt)
                time.sleep(attempt)
    except BaseException:
        if not connection.closed:
            connection.close()
        raise
