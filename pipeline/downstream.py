# This file process collections that ingestion has finished saving.
# Pick one pending collection, export its raw data, save its normalized metrics,
# then export the processed data. Retry unfinished work without another API fetch.

import logging
from pathlib import Path
import time

import psycopg
from psycopg.rows import dict_row
import simplejson

from pipeline import storage
from pipeline.exports import exporter as exports
from pipeline.observability import monitoring
from pipeline.transformations.markets import build_record
from pipeline.transformations import markets

LOCK_KEY = storage.PROCESSOR_LOCK_KEY
RETRY_SECONDS = 5
logger = logging.getLogger('market_etl.downstream')


def next_finalized_collection(connection, dataset, kind, destination, response_id=None):
    """Return one committed usable snapshot; application paths never change it after finalization."""
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute('''SELECT j.response_id, j.status, j.scheduled_at,
            j.requested_assets, j.quote_currency, r.ingested_at, r.raw_response::text
            FROM collection_jobs j JOIN raw_coingecko_responses r USING (response_id)
            WHERE j.dataset_key = %s AND j.status IN ('complete', 'partial') AND r.success
                AND (%s::uuid IS NULL OR j.response_id = %s::uuid)
                AND NOT EXISTS (
                    SELECT 1 FROM exported_collections e
                    WHERE e.destination = %s AND e.export_kind = %s
                        AND e.response_id = j.response_id)
            ORDER BY j.scheduled_at, j.response_id LIMIT 1''',
            (dataset, response_id, response_id, destination, kind))
        return cursor.fetchone()


# Reuse saved normalized rows after an export failure instead of transforming the collection again.
def ensure_metrics(connection, collection):
    try:
        rows = markets.load_metrics(connection, collection['response_id'])
        if rows:
            return rows
        records = markets.normalize_markets(collection,
            simplejson.loads(collection['raw_response'], use_decimal=True))
        markets.save_metrics(connection, records)
        rows = markets.load_metrics(connection, collection['response_id'])
    except Exception as error:
        monitoring.STAGE_RUNS.labels(stage='transformation', outcome='failure').inc()
        logger.error('stage=transformation collection=%s failed error=%s sqlstate=%s',
                     collection['response_id'], type(error).__name__, getattr(error, 'sqlstate', None))
        raise
    monitoring.STAGE_RUNS.labels(stage='transformation', outcome='success').inc()
    logger.info('stage=transformation collection=%s normalized data committed records=%s',
                collection['response_id'], len(records))
    return rows


# Skip an already exported collection, then build and publish its output while recording the outcome.
def export_stage(connection, kind, data_dir, stop, collection):
    response_id = collection['response_id']
    if stop and stop['requested']:
        return {'appended': 0}
    if exports.is_exported(connection, exports.destination_key(data_dir), kind, response_id):
        return {'appended': 0}
    # A failed transformation is not an attempted processed export.
    saved_metrics = ensure_metrics(connection, collection) if kind == 'processed' else None
    stage = f'{kind}_export'
    try:
        record = build_record(collection, kind, saved_metrics)
        result = exports.publish_record(connection, kind, data_dir, response_id, record, stop)
    except Exception as error:
        monitoring.STAGE_RUNS.labels(stage=stage, outcome='failure').inc()
        # Keep useful context without logging exception text that may contain credentials.
        logger.error('stage=%s collection=%s path=%s failed error=%s errno=%s sqlstate=%s',
            stage, response_id, Path(data_dir) / kind / f'{kind}_data.jsonl',
            type(error).__name__, getattr(error, 'errno', None), getattr(error, 'sqlstate', None))
        raise
    if result['appended']:
        monitoring.STAGE_RUNS.labels(stage=stage, outcome='success').inc()
    return result


def export_once(connection, kind, data_dir, stop=None, response_id=None):
    """Finite CLI export: publish at most one finalized collection of the requested kind."""
    if kind not in ('raw', 'processed'):
        raise ValueError('Export kind must be raw or processed.')
    collection = next_finalized_collection(
        connection, storage.DATASET, kind, exports.destination_key(data_dir), response_id)
    if collection is None:
        return {'appended': 0}
    return export_stage(connection, kind, data_dir, stop, collection)


# Use the same finalized collection for raw export, normalization and processed export, in that order.
def process_once(connection, data_dir, stop):
    storage.require_owner(connection, LOCK_KEY)
    collection = next_finalized_collection(
        connection, storage.DATASET, 'processed', exports.destination_key(data_dir))
    if collection is None:
        return 0
    export_stage(connection, 'raw', data_dir, stop, collection)
    if stop['requested']:
        return 130
    storage.require_owner(connection, LOCK_KEY)
    export_stage(connection, 'processed', data_dir, stop, collection)
    monitoring.refresh_export_backlog(connection, exports.destination_key(data_dir))
    return 130 if stop['requested'] else 0


# Keep processing saved work; notifications wake the loop, but database scans decide what is pending.
def run_downstream(database_url, data_dir, stop, once=False):
    connection = None
    next_metrics_refresh = 0
    try:
        while not stop['requested']:
            try:
                if connection is None or connection.closed:
                    connection = storage.connect(database_url)
                    # Different key: processing never owns or blocks the collector's lock.
                    storage.acquire_worker(connection, LOCK_KEY)
                    # Subscribe before scanning, so a finishing collection cannot fall in a gap.
                    connection.execute('LISTEN collection_ready')
                    next_metrics_refresh = 0
                if time.monotonic() >= next_metrics_refresh:
                    monitoring.refresh_export_backlog(connection, exports.destination_key(data_dir))
                    next_metrics_refresh = time.monotonic() + 5
                status = process_once(connection, data_dir, stop)
                monitoring.set_ready(status == 0 and not stop['requested'], connection.info.backend_pid)
                if once:
                    return status
                if status == 0 and next_finalized_collection(
                        connection, storage.DATASET, 'processed', exports.destination_key(data_dir)):
                    continue  # Process the next collection immediately; never wait to fill a batch.
                if not stop['requested']:
                    # Notifications wake us promptly; timeout scans recover missed signals.
                    list(connection.notifies(timeout=1, stop_after=1))
                continue
            except (psycopg.Error, OSError, ValueError) as error:
                monitoring.set_ready(False)
                monitoring.PENDING_EXPORTS.set(float('nan'))
                logger.error('stage=downstream pass failed error=%s; retry in %s seconds; saved data remains pending',
                             type(error).__name__, RETRY_SECONDS)
                if connection is not None:
                    connection.close()
                if once:
                    return 1
            # Pending work is derived from finalized collections and export records.
            deadline = time.monotonic() + RETRY_SECONDS
            while not stop['requested'] and time.monotonic() < deadline:
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        return 130
    finally:
        monitoring.set_ready(False)
        if connection is not None and not connection.closed:
            connection.close()
