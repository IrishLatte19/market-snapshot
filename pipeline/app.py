# This file reads the command-line options and start the requested part of the pipeline.
# It sets up logging, loads database settings, initialize tables and
# starts collection or processing. Also handle shutdown and report failures.

import argparse
import logging
import os
import signal
from pathlib import Path

import psycopg
import requests
from dotenv import load_dotenv

from pipeline.observability import monitoring
from pipeline import storage
from pipeline.storage import connect, initialize_database
from pipeline.ingestion.collector import run_collector
from pipeline.ingestion.client import load_assets
from pipeline.observability.logging_setup import configure_logging
from pipeline.downstream import run_downstream, export_once

logger = logging.getLogger('market_etl.app')
PROJECT_DIR = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description='Collect CoinGecko market data into PostgreSQL.')
    commands = parser.add_mutually_exclusive_group(required=True)
    commands.add_argument('--init-db', action='store_true', help='Create the application schema explicitly')
    commands.add_argument('--once', action='store_true', help='Process the next eligible durable collection, including retries')
    commands.add_argument('--run', action='store_true', help='Run the durable worker on fixed 30-second ticks')
    commands.add_argument('--export-raw', action='store_true', help='Append finished collections to raw JSONL')
    commands.add_argument('--export-processed', action='store_true', help='Append modeled collections to processed JSONL')
    commands.add_argument('--process', action='store_true', help='Transform and export finalized collections automatically')
    commands.add_argument('--process-once', action='store_true', help='Run one bounded downstream pass, without API requests')
    parser.add_argument('--log-dir', type=Path, default=PROJECT_DIR / 'logs', help='Log directory (default: project logs directory)')
    parser.add_argument('--data-dir', type=Path, default=Path(__file__).resolve().parents[1] / 'data',
                        help='Export root directory (default: project data directory)')
    parser.add_argument('--metrics-host', default='127.0.0.1', help='Monitoring bind address for --run or --process')
    parser.add_argument('--metrics-port', type=int, default=8000, help='Monitoring port (default: 8000; use 8001 for a local processor)')
    args = parser.parse_args()
    connection = None
    metrics_server = None
    metrics_thread = None
    stop = {'requested': False}
    # Let the first interrupt request a clean stop; a second interrupt stops waiting.
    def stop_pipeline(signal_number, frame):
        if stop['requested']:
            raise KeyboardInterrupt
        stop['requested'] = True
    previous_handlers = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous_handlers[signum] = signal.signal(signum, stop_pipeline)
    try:
        configure_logging(args.log_dir)
        load_dotenv(PROJECT_DIR / '.env', override=False)
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise ValueError('Set DATABASE_URL in .env or the environment.')
        if args.process or args.process_once:
            if args.process:
                metrics_server, metrics_thread = monitoring.start_server(
                    args.metrics_port, args.metrics_host, database_url,
                    lock_key=storage.PROCESSOR_LOCK_KEY, registry=monitoring.STAGE_REGISTRY)
            return run_downstream(database_url, args.data_dir, stop, once=args.process_once)
        connection = connect(database_url)
        if args.init_db:
            initialize_database(connection)
            logger.info('stage=initialization Database schema is ready.')
            return 0
        if args.export_raw or args.export_processed:
            kind = 'raw' if args.export_raw else 'processed'
            export_once(connection, kind, args.data_dir, stop)
            if stop['requested']:
                return 130
            return 0
        asset_ids, quote_currency = load_assets()
        with requests.Session() as session:
            session.headers.update({'Accept': 'application/json'})
            if args.run:
                metrics_server, metrics_thread = monitoring.start_server(
                    args.metrics_port, args.metrics_host, database_url,
                    lock_key=storage.COLLECTOR_LOCK_KEY, registry=monitoring.REGISTRY)
                logger.info('stage=monitoring Health and metrics endpoints are running.')
            return run_collector(session, connection, database_url, asset_ids=asset_ids,
                                 quote_currency=quote_currency, once=args.once, stop=stop)
    except KeyboardInterrupt:
        logger.info('stage=shutdown Stopped; committed collection data remains saved.')
        return 130
    except (ValueError, RuntimeError) as error:
        logger.error('stage=pipeline stopped reason=%s', error)
        return 1
    # PostgreSQL errors, 42P01 for if the table is undefined i.e. not yet created
    except psycopg.Error as error:
        if error.sqlstate == '42P01': 
            logger.error('stage=storage Database schema is missing; run python fetch.py --init-db.')
        else:
            logger.error('stage=storage failed error=%s SQLSTATE=%s', type(error).__name__, error.sqlstate or 'unknown')
        return 1
    except requests.RequestException as error:
        logger.error('stage=api failed error=%s', type(error).__name__)
        return 1
    except OSError as error:
        logger.error('stage=pipeline operating-system failure error=%s errno=%s; check file permissions or metrics host/port',
                     type(error).__name__, error.errno)
        return 1
    finally:
        if stop['requested']:
            logger.info('stage=shutdown Stop requested; stopped new work and drained or cancelled active work.')
        monitoring.set_ready(False)
        if metrics_server is not None:
            metrics_server.shutdown()
            metrics_server.server_close()
            metrics_thread.join(timeout=2)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
        if connection is not None and not connection.closed:
            connection.close()
        for handler in logging.getLogger('market_etl').handlers[:]:
            logging.getLogger('market_etl').removeHandler(handler)
            handler.close()


if __name__ == '__main__':
    raise SystemExit(main())
