# This file runs collection through one request path so polling and retries cannot overlap.
# Asks jobs.py for eligible work, fetch unresolved assets, validate the response,
# saves the result and cooldown, and reports logs and metrics for each attempt.

from datetime import datetime, timedelta, timezone
import math
import logging
import time
from uuid import uuid4

import requests

from pipeline.ingestion import jobs
from pipeline import storage
from pipeline.observability import monitoring
from pipeline.ingestion.client import fetch_markets, retry_delay
from pipeline.ingestion.validation import validate_markets

logger = logging.getLogger('market_etl.collector')


# Fetch unresolved assets once, validate the result, and save usable data with the retry cooldown.
def collect_attempt(session, connection, database_url, job):
    valid_markets = {}
    permanent_error = False
    retry_after = None
    missing = jobs.missing_assets(job)
    try:
        body, observed_at = fetch_markets(session, missing, job['quote_currency'])
    except requests.HTTPError as error:
        monitoring.API_REQUESTS.labels(outcome='failure').inc()
        retry_after = error.response.headers.get('Retry-After')
        logger.error('stage=api collection=%s attempt=%s failed HTTP=%s',
                     job['response_id'], job['attempt_count'], error.response.status_code)
    except (requests.ConnectionError, requests.Timeout,
            requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError) as error:
        monitoring.API_REQUESTS.labels(outcome='failure').inc()
        logger.error('stage=api collection=%s attempt=%s failed error=%s',
                     job['response_id'], job['attempt_count'], type(error).__name__)
    except ValueError as error:
        monitoring.API_REQUESTS.labels(outcome='failure').inc()
        logger.error('stage=api collection=%s attempt=%s failed reason=%s',
                     job['response_id'], job['attempt_count'], error)
        permanent_error = True
    else:
        monitoring.API_REQUESTS.labels(outcome='success').inc()
        logger.info('stage=api collection=%s attempt=%s request succeeded', job['response_id'], job['attempt_count'])
        try:
            valid_markets, errors = validate_markets(body, missing)
            if errors:
                logger.warning('stage=validation collection=%s attempt=%s rejected_assets=%s',
                               job['response_id'], job['attempt_count'], errors)
        except ValueError as error:
            logger.error('stage=validation collection=%s attempt=%s failed reason=%s',
                         job['response_id'], job['attempt_count'], error)
    completed_at = datetime.now(timezone.utc)
    if not valid_markets:
        observed_at = completed_at
    delay = retry_delay(retry_after)
    # An effectively unbounded provider delay is persisted, not shortened or overflowed.
    if math.isfinite(delay) and delay < (datetime.max.replace(tzinfo=timezone.utc) - completed_at).total_seconds():
        cooldown_until = completed_at + timedelta(seconds=delay)
    else:
        cooldown_until = datetime.max.replace(tzinfo=timezone.utc)
    connection, result = jobs.run_with_retry(connection, database_url, jobs.finalize_attempt,
        job, valid_markets, observed_at, cooldown_until, permanent_error)
    logger.info('stage=storage collection=%s attempt=%s committed saved_assets=%s status=%s missing_assets=%s',
                job['response_id'], job['attempt_count'], len(result['saved_assets']),
                result['status'], jobs.missing_assets(result))
    monitoring.record_collection(result)
    return connection, result


# Recover saved work, then run eligible attempts one at a time until asked to stop.
def run_collector(session, connection, database_url, asset_ids, quote_currency, once=False, stop=None):
    stop = stop if stop is not None else {'requested': False}
    try:
        storage.acquire_worker(connection)
        connection, _ = jobs.run_with_retry(connection, database_url, jobs.recover_jobs)
        connection, _ = jobs.run_with_retry(connection, database_url, monitoring.restore_gauges)
        monitoring.set_ready(not stop['requested'], connection.info.backend_pid)
        while not stop['requested']:
            connection, work = jobs.run_with_retry(connection, database_url, jobs.get_work, asset_ids, quote_currency)
            monitoring.set_ready(not stop['requested'], connection.info.backend_pid)
            job, wait_seconds = work
            if job and job['status'] in jobs.TERMINAL_STATUSES:
                monitoring.record_collection(job)
                if once:
                    return 1 if job['status'] == 'failed' else 0
                continue
            if wait_seconds > 0 or job is None:
                # Wake at least once per second so a stop request does not wait for a long cooldown.
                time.sleep(min(max(wait_seconds, 0.05), 1))
                continue
            if stop['requested']:
                break
            token = uuid4()
            connection, job = jobs.run_with_retry(connection, database_url, jobs.claim_attempt,
                                                  job['response_id'], token)
            if job is None:
                continue
            if stop['requested']:
                break  # Reservation is durable and will be recovered on restart.
            connection, result = collect_attempt(session, connection, database_url, job)
            if stop['requested']:
                break
            if once and result['status'] in jobs.TERMINAL_STATUSES:
                return 1 if result['status'] == 'failed' else 0
        return 130
    finally:
        monitoring.set_ready(False)
        if not connection.closed:
            connection.close()
