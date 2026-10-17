# This file turns saved market records into the smaller, consistent structure used by consumers.
# Keeps required fields, preserves decimals, converts timestamps to UTC, saves and reads
# normalized rows, and builds raw or processed output records for the exporter.

from datetime import datetime, timezone
from decimal import Decimal
import logging
import simplejson
from psycopg.rows import dict_row

logger = logging.getLogger('market_etl.transformations')


def utc_timestamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return value.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


# Build one flat record per asset in this collection, keeping exact decimals and UTC timestamps.
def normalize_markets(collection, markets):
    logger.info('stage=transformation collection=%s started', collection['response_id'])
    try:
        records = []
        for asset_id, market in sorted(markets.items()):
            records.append({
                'response_id': str(collection['response_id']),
                'asset_id': asset_id,
                'quote_currency': collection['quote_currency'],
                'price': Decimal(market['current_price']),
                'market_cap': Decimal(market['market_cap']) if market.get('market_cap') is not None else None,
                'volume_24h': Decimal(market['total_volume']) if market.get('total_volume') is not None else None,
                'source_updated_at': utc_timestamp(market['last_updated']),
                'observed_at': utc_timestamp(market['observed_at']),
                'ingested_at': utc_timestamp(collection['ingested_at']),
            })
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        logger.error('stage=transformation collection=%s failed error=%s',
                     collection['response_id'], type(error).__name__)
        raise ValueError('Saved collection could not be normalized.') from error
    logger.info('stage=transformation collection=%s succeeded records=%s', collection['response_id'], len(records))
    return records


def build_record(collection, kind, saved_metrics=None):
    """Build one output envelope from a saved snapshot, without database or file access."""
    if kind == 'processed':
        # Processed exports use committed typed rows, never rerun normalization here.
        records = []
        for row in saved_metrics:
            record = dict(row, response_id=str(row['response_id']))
            for field in ('source_updated_at', 'observed_at', 'ingested_at'):
                record[field] = utc_timestamp(row[field])
            records.append(record)
        asset_ids = {row['asset_id'] for row in records}
    else:
        records = simplejson.loads(collection['raw_response'], use_decimal=True)
        asset_ids = set(records)
        for market in records.values():
            for field in ('last_updated', 'observed_at'):
                market[field] = utc_timestamp(market[field])
    return {
        'response_id': str(collection['response_id']),
        'status': collection['status'],
        'quote_currency': collection['quote_currency'],
        'scheduled_at': utc_timestamp(collection['scheduled_at']),
        'ingested_at': utc_timestamp(collection['ingested_at']),
        'missing_asset_ids': sorted(set(collection['requested_assets']) - asset_ids),
        'records': records,
    }


def load_metrics(connection, response_id):
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute('''SELECT response_id, asset_id, quote_currency, price,
            market_cap, volume_24h, source_updated_at, observed_at, ingested_at
            FROM coingecko_metrics WHERE response_id = %s ORDER BY asset_id''', (response_id,))
        return cursor.fetchall()


# Save the whole normalized collection together; retries cannot replace existing observations.
def save_metrics(connection, records):
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.executemany('''INSERT INTO coingecko_metrics
                (response_id, asset_id, quote_currency, price, market_cap, volume_24h,
                 source_updated_at, observed_at, ingested_at)
                VALUES (%(response_id)s, %(asset_id)s, %(quote_currency)s, %(price)s,
                    %(market_cap)s, %(volume_24h)s, %(source_updated_at)s, %(observed_at)s, %(ingested_at)s)
                ON CONFLICT (response_id, asset_id, quote_currency) DO NOTHING''', records)
