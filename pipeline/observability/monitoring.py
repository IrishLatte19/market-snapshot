# This file shows how the pipeline is doing through Prometheus metrics and health endpoints.
# Tracks API outcomes, collection completeness, freshness and downstream progress; restores gauges
# from PostgreSQL and check readiness without sharing the worker's connection.

from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Lock, Thread

import psycopg
from prometheus_client import CollectorRegistry, Counter, Gauge, CONTENT_TYPE_LATEST, generate_latest

from pipeline import storage

REGISTRY = CollectorRegistry()
STAGE_REGISTRY = CollectorRegistry()
STATE_LOCK = Lock()
STATE = {'ready': False, 'owner_pid': None}

API_REQUESTS = Counter('etl_api_requests_total', 'HTTP attempts by outcome, before data validation.',
                       ['outcome'], registry=REGISTRY)
COLLECTIONS = Counter('etl_collections_total', 'Terminal collection outcomes observed by this process.',
                      ['status'], registry=REGISTRY)
MISSING_ASSETS = Gauge('etl_missing_assets', 'Missing assets in the latest finished collection; NaN when none exists.',
                       registry=REGISTRY)
LAST_COMPLETE = Gauge('etl_last_complete_collection_timestamp_seconds',
                      'Latest complete collection acquisition time, restored from PostgreSQL; zero when none exists.',
                      registry=REGISTRY)
MISSING_ASSETS.set(float('nan'))
STAGE_RUNS = Counter('etl_stage_runs_total', 'Downstream stage attempts by outcome.',
                     ['stage', 'outcome'], registry=STAGE_REGISTRY)
PENDING_EXPORTS = Gauge('etl_pending_exports',
    'Finalized usable collections awaiting processed export for this destination; NaN until queried.',
    registry=STAGE_REGISTRY)
PENDING_EXPORTS.set(float('nan'))

# Show zero-valued series even before the first attempt or completed collection.
for outcome in ('success', 'failure'):
    API_REQUESTS.labels(outcome=outcome)
for status in ('complete', 'partial', 'failed'):
    COLLECTIONS.labels(status=status)
for stage in ('raw_export', 'transformation', 'processed_export'):
    for outcome in ('success', 'failure'):
        STAGE_RUNS.labels(stage=stage, outcome=outcome)


# Read pending processed exports from saved progress, including after a restart.
def refresh_export_backlog(connection, destination):
    pending = connection.execute('''SELECT count(*) FROM collection_jobs j
        JOIN raw_coingecko_responses r USING (response_id)
        WHERE j.dataset_key = %s AND j.status IN ('complete', 'partial') AND r.success
        AND NOT EXISTS (SELECT 1 FROM exported_collections e
            WHERE e.destination = %s AND e.export_kind = 'processed'
            AND e.response_id = j.response_id)''', (storage.DATASET, destination)).fetchone()[0]
    PENDING_EXPORTS.set(pending)


# Use the latest asset acquisition time because missing assets may arrive on a later retry.
def acquisition_time(markets):
    return max(datetime.fromisoformat(asset['observed_at'].replace('Z', '+00:00')).timestamp()
               for asset in markets.values())


# Restore collection freshness and missing-asset counts from saved data without changing counters.
def restore_gauges(connection):
    query = '''SELECT j.requested_assets, COALESCE(r.raw_response, '{}'::jsonb)
        FROM collection_jobs j LEFT JOIN raw_coingecko_responses r USING (response_id)
        WHERE j.dataset_key = %s AND j.status = ANY(%s::text[])
        ORDER BY j.scheduled_at DESC LIMIT 1'''
    latest = connection.execute(query, (storage.DATASET, list(storage.TERMINAL_STATUSES))).fetchone()
    complete = connection.execute(query, (storage.DATASET, ['complete'])).fetchone()
    missing = len(set(latest[0]) - set(latest[1])) if latest else float('nan')
    acquired = acquisition_time(complete[1]) if complete else 0
    MISSING_ASSETS.set(missing)
    LAST_COMPLETE.set(acquired)


# Count finished collections only; pending retries must not look like completed work.
def record_collection(job):
    if job['status'] in storage.TERMINAL_STATUSES:
        COLLECTIONS.labels(status=job['status']).inc()
        MISSING_ASSETS.set(len(set(job['requested_assets']) - set(job['saved_assets'])))
        if job['status'] == 'complete':
            LAST_COMPLETE.set(acquisition_time(job['saved_assets']))


# Update readiness under a lock because the worker and HTTP handlers access it from different threads.
def set_ready(ready, owner_pid=None):
    with STATE_LOCK:
        STATE['ready'] = ready
        STATE['owner_pid'] = owner_pid


# Serve metrics and health checks separately so they remain available while the worker is busy.
def start_server(port, host, database_url, lock_key=storage.COLLECTOR_LOCK_KEY, registry=REGISTRY):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/metrics':
                body, status, content_type = generate_latest(registry), 200, CONTENT_TYPE_LATEST
            elif self.path == '/health':
                body, status, content_type = b'{"status":"alive"}', 200, 'application/json'
            elif self.path == '/ready':
                with STATE_LOCK:
                    ready, pid = STATE['ready'], STATE['owner_pid']
                if ready:
                    try:
                        # Readiness owns this short-lived connection, never the worker's.
                        with psycopg.connect(database_url, autocommit=True, connect_timeout=2,
                                             options='-c statement_timeout=2000') as probe:
                            ready = probe.execute('''SELECT EXISTS (SELECT 1 FROM pg_locks
                                WHERE locktype = 'advisory' AND pid = %s AND classid = %s
                                AND objid = %s AND objsubid = 2 AND granted)''',
                                (pid, storage.LOCK_NAMESPACE, lock_key)).fetchone()[0]
                    except psycopg.Error:
                        ready = False
                body = json.dumps({'status': 'ready' if ready else 'not_ready'}).encode()
                status, content_type = (200 if ready else 503), 'application/json'
            else:
                body, status, content_type = b'{"status":"not_found"}', 404, 'application/json'
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass  # Avoid a console line for every health check and metrics scrape.

    set_ready(False)
    server = ThreadingHTTPServer((host, port), Handler)
    server.daemon_threads = True
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread
