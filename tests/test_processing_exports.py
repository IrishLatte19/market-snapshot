# Check that JSONL exports preserve confirmed data when writes or progress saves fail.
# Cover file locks, append failures, checkpoint recovery, missing or shortened
# files, and exporting the available assets from a partial collection.

import errno
import fcntl
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pytest
import simplejson

from pipeline import downstream
from pipeline.exports import exporter as exports
from pipeline.ingestion.client import load_assets

ASSET_IDS, QUOTE_CURRENCY = load_assets()


def read_records(path):
    return [simplejson.loads(line, use_decimal=True) for line in path.read_bytes().splitlines()]


# Interrupted write: A write that fails halfway through does not damage previously confirmed lines.
def test_partial_write_failure_retains_previous_lines(tmp_path):
    path = tmp_path / 'data.jsonl'
    original = exports.serialize_record({'response_id': 'saved'})
    with path.open('a+b') as file:
        file.write(original)
        file.flush()
        failing = MagicMock(wraps=file)
        def interrupted_write(line):
            file.write(line[:5])
            raise OSError(errno.ENOSPC, 'simulated disk full')
        failing.write.side_effect = interrupted_write
        with pytest.raises(OSError):
            exports.append_line(failing, exports.serialize_record({'response_id': 'new'}))
    assert path.read_bytes() == original


# Exclusive file writer: A second exporter cannot write a file already locked by another exporter.
def test_two_exporters_cannot_write_same_file(tmp_path):
    directory = tmp_path / 'raw'
    directory.mkdir()
    with (directory / 'raw_data.jsonl').open('a+b') as owner:
        fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match='Another exporter'):
            exports.publish_record(MagicMock(), 'raw', tmp_path, uuid4(), {})


# Failed file write: Does not mark the collection exported and allows a later retry to succeed.
@pytest.mark.integration
def test_write_failure_does_not_record_export_and_can_retry(collections, tmp_path, monkeypatch):
    connection, create = collections
    create()
    append = exports.append_line
    monkeypatch.setattr(exports, 'append_line', MagicMock(side_effect=PermissionError('denied')))
    with pytest.raises(PermissionError):
        downstream.export_once(connection, 'raw', tmp_path)
    assert connection.execute('SELECT count(*) FROM exported_collections').fetchone()[0] == 0
    monkeypatch.setattr(exports, 'append_line', append)
    assert downstream.export_once(connection, 'raw', tmp_path)['appended'] == 1


# Failed progress commit: Retry removes the unconfirmed append and writes it once, for raw and processed exports.
@pytest.mark.integration
@pytest.mark.parametrize('kind', ['raw', 'processed'])
def test_failed_progress_commit_rolls_back_marker_and_checkpoint_and_retry_removes_duplicate(collections, tmp_path, monkeypatch, kind):
    connection, create = collections
    create()
    record_export = exports.record_export
    def failed_commit(conn, *args):
        with conn.transaction():
            record_export(conn, *args)
            raise psycopg.OperationalError('Failure before committing progress')
    monkeypatch.setattr(exports, 'record_export', failed_commit)
    with pytest.raises(psycopg.OperationalError):
        downstream.export_once(connection, kind, tmp_path)
    path = tmp_path / kind / f'{kind}_data.jsonl'
    before = path.read_bytes()
    destination = exports.destination_key(tmp_path)
    assert before and exports.load_checkpoint(connection, destination, kind) == 0
    assert connection.execute('SELECT count(*) FROM exported_collections').fetchone()[0] == 0
    monkeypatch.setattr(exports, 'record_export', record_export)
    if kind == 'processed':
        monkeypatch.setattr(downstream.markets, 'normalize_markets', MagicMock(side_effect=AssertionError('Metrics already saved')))
    assert downstream.export_once(connection, kind, tmp_path) == {'appended': 1}
    assert path.read_bytes() == before
    assert len(read_records(path)) == 1
    assert exports.load_checkpoint(connection, destination, kind) == len(before)


# Incomplete tail recovery: Removes bytes beyond the checkpoint while preserving confirmed lines.
@pytest.mark.integration
def test_partial_tail_is_removed_without_scanning_or_changing_confirmed_lines(collections, tmp_path):
    connection, create = collections
    create()
    downstream.export_once(connection, 'raw', tmp_path)
    path = tmp_path / 'raw/raw_data.jsonl'
    before = path.read_bytes()
    with path.open('ab') as file:
        file.write(b'{"incomplete":')
    create()
    assert downstream.export_once(connection, 'raw', tmp_path) == {'appended': 1}
    assert path.read_bytes().startswith(before)
    assert len(read_records(path)) == 2
    assert exports.load_checkpoint(connection, exports.destination_key(tmp_path), 'raw') == path.stat().st_size


# Missing or shortened file: Reports an error when the file is smaller than its recorded checkpoint.
@pytest.mark.integration
@pytest.mark.parametrize('missing', [False, True])
def test_short_or_missing_file_is_not_silently_accepted(collections, tmp_path, missing):
    connection, create = collections
    first = create()
    downstream.export_once(connection, 'raw', tmp_path)
    path = tmp_path / 'raw/raw_data.jsonl'
    if missing:
        path.unlink()
    else:
        path.write_bytes(b'broken')
    create()
    with pytest.raises(ValueError, match='shorter than its checkpoint'):
        downstream.export_once(connection, 'raw', tmp_path)
    assert exports.is_exported(connection, exports.destination_key(tmp_path), 'raw', first)


# Partial collection output: Exports the four available assets with partial status and the missing asset identified.
@pytest.mark.integration
def test_partial_collection_exports_only_its_four_saved_assets(collections, db, tmp_path):
    connection, create = collections
    _, url = db
    identity = create('partial', ASSET_IDS[:-1])
    assert downstream.run_downstream(url, tmp_path, {'requested': False}, once=True) == 0
    raw = read_records(tmp_path / 'raw/raw_data.jsonl')[0]
    processed = read_records(tmp_path / 'processed/processed_data.jsonl')[0]
    for output in (raw, processed):
        assert output['response_id'] == str(identity)
        assert output['status'] == 'partial' and output['missing_asset_ids'] == ['tether']
        assert len(output['records']) == 4
    assert connection.execute('SELECT count(*) FROM coingecko_metrics').fetchone()[0] == 4
