# This file keeps the Database setup in one place: tables, indexes and the notification trigger.
# Opens connections with UTC settings and manage the ownership locks that stop
# two collectors or two processors from running against the same database.

import psycopg

DATASET = 'coingecko-usd'
TERMINAL_STATUSES = ('complete', 'partial', 'failed')
LOCK_NAMESPACE = 742031
COLLECTOR_LOCK_KEY = 1
PROCESSOR_LOCK_KEY = 2


# Use UTC for this connection and limit how long database operations can wait.
def connect(database_url):
    connection = psycopg.connect(database_url, autocommit=True, connect_timeout=5)
    try:
        connection.execute("SET statement_timeout = '10s'")
        connection.execute("SET lock_timeout = '2s'")
        connection.execute("SET TIME ZONE 'UTC'")
        return connection
    except BaseException:
        connection.close()
        raise


# Give one connection ownership of this role so a competing worker cannot start.
def acquire_worker(connection, lock_key=COLLECTOR_LOCK_KEY):
    acquired = connection.execute('SELECT pg_try_advisory_lock(%s, %s)',
                                  (LOCK_NAMESPACE, lock_key)).fetchone()[0]
    if not acquired:
        raise RuntimeError(f'Another worker owns database role {lock_key}; stop that role before starting another worker.')


# Check the current connection still owns its role before allowing more work.
def require_owner(connection, lock_key=COLLECTOR_LOCK_KEY):
    owned = connection.execute('''SELECT EXISTS (
        SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()
        AND classid = %s AND objid = %s AND objsubid = 2 AND granted
    )''', (LOCK_NAMESPACE, lock_key)).fetchone()[0]
    if not owned:
        raise RuntimeError('Worker ownership was lost; reacquire it before continuing.')


def initialize_database(connection):
    """Create all pipeline tables, indexes and the completion notification explicitly."""
    with connection.transaction():
        connection.execute('''
            CREATE TABLE IF NOT EXISTS raw_coingecko_responses (
                response_id UUID PRIMARY KEY,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL,
                raw_response JSONB NOT NULL DEFAULT '{}'::jsonb,
                success BOOLEAN NOT NULL DEFAULT FALSE,
                try_count INTEGER NOT NULL CHECK (try_count BETWEEN 1 AND 3),
                CHECK (jsonb_typeof(raw_response) = 'object'),
                CHECK (success = (raw_response <> '{}'::jsonb))
            );
        ''')
        connection.execute('''
            CREATE TABLE IF NOT EXISTS collector_schedule (
                dataset_key TEXT PRIMARY KEY,
                next_poll_at TIMESTAMPTZ NOT NULL,
                cooldown_until TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS collection_jobs (
                response_id UUID PRIMARY KEY,
                dataset_key TEXT NOT NULL REFERENCES collector_schedule(dataset_key),
                scheduled_at TIMESTAMPTZ NOT NULL,
                requested_assets JSONB NOT NULL,
                quote_currency TEXT NOT NULL,
                next_attempt_at TIMESTAMPTZ NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 3),
                attempt_token UUID,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'complete', 'partial', 'failed')),
                UNIQUE (dataset_key, scheduled_at)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS one_active_collection
                ON collection_jobs(dataset_key) WHERE status IN ('pending', 'running');
            CREATE INDEX IF NOT EXISTS finished_collection_order
                ON collection_jobs(scheduled_at, response_id)
                WHERE status IN ('complete', 'partial');
            CREATE OR REPLACE FUNCTION notify_collection_ready() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.status IN ('complete', 'partial')
                   AND (TG_OP = 'INSERT' OR OLD.status IS DISTINCT FROM NEW.status) THEN
                    PERFORM pg_notify('collection_ready', NEW.response_id::text);
                END IF;
                RETURN NEW;
            END;
            $$;
            CREATE OR REPLACE TRIGGER collection_ready
                AFTER INSERT OR UPDATE OF status ON collection_jobs
                FOR EACH ROW EXECUTE FUNCTION notify_collection_ready();
        ''')
        connection.execute('''CREATE TABLE IF NOT EXISTS coingecko_metrics (
            response_id UUID NOT NULL REFERENCES raw_coingecko_responses(response_id),
            asset_id TEXT NOT NULL,
            quote_currency TEXT NOT NULL,
            price NUMERIC NOT NULL CHECK (price >= 0 AND price < 'Infinity'::numeric),
            market_cap NUMERIC CHECK (market_cap >= 0 AND market_cap < 'Infinity'::numeric),
            volume_24h NUMERIC CHECK (volume_24h >= 0 AND volume_24h < 'Infinity'::numeric),
            source_updated_at TIMESTAMPTZ NOT NULL,
            observed_at TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (response_id, asset_id, quote_currency)
        )''')
        connection.execute('''CREATE TABLE IF NOT EXISTS exported_collections (
            destination TEXT NOT NULL,
            export_kind TEXT NOT NULL CHECK (export_kind IN ('raw', 'processed')),
            response_id UUID NOT NULL REFERENCES raw_coingecko_responses(response_id),
            exported_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (destination, export_kind, response_id)
        )''')
        connection.execute('''CREATE TABLE IF NOT EXISTS export_checkpoints (
            destination TEXT NOT NULL,
            export_kind TEXT NOT NULL CHECK (export_kind IN ('raw', 'processed')),
            committed_bytes BIGINT NOT NULL CHECK (committed_bytes >= 0),
            PRIMARY KEY (destination, export_kind)
        )''')
