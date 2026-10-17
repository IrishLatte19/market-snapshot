# This file apends prepared records to the raw and processed JSONL files.
# Locks each file while writing, track exported IDs and confirmed byte lengths in
# PostgreSQL and removes unconfirmed tails before retrying an interrupted export.

import fcntl
import logging
import os
from pathlib import Path

import simplejson


logger = logging.getLogger('market_etl.exports')


def destination_key(data_dir):
    directory = Path(data_dir).resolve()
    # Host and container paths for the default bind mount identify the same output.
    if directory == Path(__file__).resolve().parents[2] / 'data':
        return 'default'
    return str(directory)


def serialize_record(record):
    return (simplejson.dumps(record, use_decimal=True, allow_nan=False,
        ensure_ascii=False, sort_keys=True, separators=(',', ':')) + '\n').encode('utf-8')


# Flush one complete line to disk, undoing a partial append if the write raises an error.
def append_line(file, line):
    offset = file.tell()
    try:
        if file.write(line) != len(line):
            raise OSError('Incomplete export write.')
        file.flush()
        os.fsync(file.fileno())
    except BaseException:
        # Roll back an ordinary failed write, not a process crash.
        file.seek(offset)
        file.truncate()
        file.flush()
        os.fsync(file.fileno())
        raise


def recover_file(connection, file, destination, kind):
    """Under the file lock, keep only the bytes confirmed in PostgreSQL."""
    file.seek(0, os.SEEK_END)
    size = file.tell()
    committed = load_checkpoint(connection, destination, kind)
    if committed is None:
        if size:
            raise ValueError('Existing export file has no checkpoint; use a fresh export destination.')
        start_checkpoint(connection, destination, kind)
        committed = 0
    if size < committed:
        raise ValueError('Export file is shorter than its checkpoint; restore it or use a fresh export destination.')
    if size > committed:
        file.truncate(committed)
        file.flush()
        os.fsync(file.fileno())
        logger.warning('stage=export kind=%s recovered uncommitted_bytes=%s', kind, size - committed)
    file.seek(0, os.SEEK_END)


def publish_record(connection, kind, data_dir, response_id, record, stop=None):
    """Publish an already-built record; selection and normalization belong to the processor."""
    if kind not in ('raw', 'processed'):
        raise ValueError('Export kind must be raw or processed.')
    path = Path(data_dir).resolve() / kind / f'{kind}_data.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = {'appended': 0}
    destination = destination_key(data_dir)
    with path.open('a+b') as file:
        try:
            fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f'Another exporter owns {path}.') from error
        try:
            recover_file(connection, file, destination, kind)
        except ValueError as error:
            # Recovery errors contain fixed instructions, not database credentials or source bodies.
            logger.error('stage=export kind=%s checkpoint recovery stopped reason=%s', kind, error)
            raise
        # Recheck inside the lock: a finite CLI export may have published this ID meanwhile.
        if not is_exported(connection, destination, kind, response_id) and not (stop and stop['requested']):
            line = serialize_record(record)
            append_line(file, line)
            # On a failed/uncertain DB commit leave the tail for checkpoint recovery, not blind rollback.
            record_export(connection, destination, kind, response_id, file.tell())
            logger.info('stage=export kind=%s collection=%s data saved successfully path=%s', kind, response_id, path)
            counts['appended'] += 1
    logger.info('stage=export kind=%s complete counts=%s destination=%s', kind, counts, path)
    return counts


def is_exported(connection, destination, kind, response_id):
    return connection.execute('''SELECT EXISTS (SELECT 1 FROM exported_collections
        WHERE destination = %s AND export_kind = %s AND response_id = %s)''',
        (destination, kind, response_id)).fetchone()[0]


def load_checkpoint(connection, destination, kind):
    row = connection.execute('''SELECT committed_bytes FROM export_checkpoints
        WHERE destination = %s AND export_kind = %s''', (destination, kind)).fetchone()
    return row[0] if row else None


# Start at zero only after the publisher checks the file is empty and no export history exists.
def start_checkpoint(connection, destination, kind):
    row = connection.execute('''INSERT INTO export_checkpoints (destination, export_kind, committed_bytes)
        SELECT %s, %s, 0 WHERE NOT EXISTS (SELECT 1 FROM exported_collections
            WHERE destination = %s AND export_kind = %s)
        RETURNING committed_bytes''', (destination, kind, destination, kind)).fetchone()
    if row is None:
        raise ValueError('Existing export history has no checkpoint; use a fresh export destination.')


# Commit the exported ID and confirmed file length together so retries can check what was saved.
def record_export(connection, destination, kind, response_id, committed_bytes):
    with connection.transaction():
        connection.execute('''INSERT INTO exported_collections
            (destination, export_kind, response_id) VALUES (%s, %s, %s)''',
            (destination, kind, response_id))
        connection.execute('''UPDATE export_checkpoints SET committed_bytes = %s
            WHERE destination = %s AND export_kind = %s''', (committed_bytes, destination, kind))
