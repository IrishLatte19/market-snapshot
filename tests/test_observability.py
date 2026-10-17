# Check that logs and metrics describe what happened rather than just what was attempted.
# Verify file and console logs, the six metric families, and collection/stage
# outcomes, freshness and backlog when processing succeeds or fails.

import logging
import re
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import psycopg
import pytest
import requests

from pipeline import downstream
from pipeline.exports import exporter as exports
from pipeline.ingestion import collector, jobs, validation
from pipeline.ingestion.client import load_assets
from pipeline.observability import monitoring
from pipeline.observability.logging_setup import configure_logging

ASSET_IDS, QUOTE_CURRENCY = load_assets()


@pytest.fixture
def log_file(tmp_path):
    configure_logging(tmp_path)
    yield tmp_path / 'etl.log'
    logger = logging.getLogger('market_etl')
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()


# Log format and append: Writes UTC-stamped messages to the console and appends to etl.log.
def test_logs_append_with_utc_timestamp_and_console_output(log_file, capsys):
    configure_logging(log_file.parent)
    logger = logging.getLogger('market_etl.test')
    logger.info('stage=test first event')
    configure_logging(log_file.parent)
    logger.error('stage=test second event')
    lines = log_file.read_text().splitlines()
    assert len(lines) == 2
    assert re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z INFO ', lines[0])
    assert 'ERROR' in lines[1]
    assert 'stage=test first event' in capsys.readouterr().out


# Ingestion logs: Records API success or network failure, collection identity and committed-storage information.
@pytest.mark.parametrize('outcome', ['success', 'network_failure'])
def test_ingestion_logs_api_validation_and_committed_storage(log_file, raw_response, monkeypatch, outcome):
    identity = uuid4()
    now = datetime.now(timezone.utc)
    job = {'response_id': identity, 'attempt_token': uuid4(), 'attempt_count': 1,
           'quote_currency': 'usd', 'requested_assets': ['ethereum'], 'saved_assets': {}, 'status': 'running'}
    fetch = MagicMock(return_value=(raw_response if outcome == 'success' else b'not JSON', now))
    if outcome == 'network_failure':
        fetch.side_effect = requests.Timeout('sensitive request details must not be logged')
    markets = validation.validate_markets(raw_response, ['ethereum'])[0] if outcome == 'success' else {}
    saved = {asset: dict(record, observed_at=now.isoformat()) for asset, record in markets.items()}
    result = dict(job, status='complete' if markets else 'failed', saved_assets=saved)
    monkeypatch.setattr(collector, 'fetch_markets', fetch)
    monkeypatch.setattr(jobs, 'run_with_retry', lambda connection, *args: (connection, result))
    collector.collect_attempt(MagicMock(), MagicMock(), 'unused', job)
    text = log_file.read_text()
    assert str(identity) in text
    assert 'stage=storage' in text and 'committed saved_assets=' in text
    if outcome == 'network_failure':
        assert 'stage=api' in text and 'failed error=Timeout' in text
        assert 'sensitive request details' not in text
    else:
        assert 'request succeeded' in text


# Export failure logs: Identifies the failed stage and collection without including sensitive error details.
def test_export_failure_logs_collection_stage_and_safe_context(log_file, monkeypatch):
    kind = 'processed'
    identity = uuid4()
    error = PermissionError(13, 'private credentials must not be logged')
    monkeypatch.setattr(exports, 'is_exported', MagicMock(return_value=False))
    monkeypatch.setattr(downstream, 'build_record', MagicMock(return_value={}))
    monkeypatch.setattr(downstream.exports, 'publish_record', MagicMock(side_effect=error))
    with pytest.raises(PermissionError):
        downstream.export_stage(MagicMock(), kind, log_file.parent, {'requested': False}, {'response_id': identity})
    text = log_file.read_text()
    assert f'collection={identity}' in text
    assert f'stage={kind}_export' in text
    assert f'{kind}/{kind}_data.jsonl' in text
    assert 'error=PermissionError errno=13' in text
    assert 'private credentials' not in text


def value(name, labels=None):
    return monitoring.REGISTRY.get_sample_value(name, labels or {})


# Six questions only: The collector and processor expose exactly the chosen metric families.
def test_metric_registries_cover_only_the_six_questions():
    assert {metric.name for metric in monitoring.REGISTRY.collect()} == {
        'etl_api_requests', 'etl_collections', 'etl_missing_assets',
        'etl_last_complete_collection_timestamp_seconds',
    }
    assert {metric.name for metric in monitoring.STAGE_REGISTRY.collect()} == {
        'etl_stage_runs', 'etl_pending_exports',
    }


@pytest.fixture
def attempt(monkeypatch, raw_response):
    connection = MagicMock(closed=False)
    job = {'response_id': uuid4(), 'attempt_token': uuid4(), 'attempt_count': 1,
           'quote_currency': 'usd', 'requested_assets': list(ASSET_IDS), 'saved_assets': {},
           'status': 'running'}
    result = dict(job, status='complete', saved_assets={asset: {'observed_at': '2026-10-14T12:00:00Z'}
                                                     for asset in ASSET_IDS})
    fetch = MagicMock(return_value=(raw_response, datetime.now(timezone.utc)))
    finalize = MagicMock(return_value=result)
    monkeypatch.setattr(collector, 'fetch_markets', fetch)
    monkeypatch.setattr(jobs, 'finalize_attempt', finalize)
    return connection, job, result, fetch, finalize


# Successful-attempt metrics: Updates requests, completion, missing assets and freshness.
def test_success_records_http_outcome_and_committed_completion(attempt):
    connection, job, result, fetch, finalize = attempt
    requests_before = value('etl_api_requests_total', {'outcome': 'success'})
    complete_before = value('etl_collections_total', {'status': 'complete'})
    collector.collect_attempt(MagicMock(), connection, 'unused', job)
    assert value('etl_api_requests_total', {'outcome': 'success'}) == requests_before + 1
    assert value('etl_collections_total', {'status': 'complete'}) == complete_before + 1
    assert value('etl_missing_assets') == 0
    assert value('etl_last_complete_collection_timestamp_seconds') == datetime(2026, 10, 14, 12, tzinfo=timezone.utc).timestamp()


# Failed-request metrics: Counts a failure once without advancing successful-collection freshness.
def test_failed_http_is_measured_once_without_advancing_freshness(attempt):
    failure = requests.Timeout()
    connection, job, result, fetch, finalize = attempt
    fetch.side_effect = failure
    result.update(status='failed', saved_assets={})
    failed_before = value('etl_api_requests_total', {'outcome': 'failure'})
    freshness_before = value('etl_last_complete_collection_timestamp_seconds')
    collector.collect_attempt(MagicMock(), connection, 'unused', job)
    assert value('etl_api_requests_total', {'outcome': 'failure'}) == failed_before + 1
    assert value('etl_last_complete_collection_timestamp_seconds') == freshness_before
    assert value('etl_missing_assets') == 5


# HTTP success versus usable data: Distinguishes a successful response from a complete, usable collection.
@pytest.mark.parametrize('body', [None, b'not JSON'])
def test_partial_or_invalid_data_is_not_a_transport_failure(attempt, body):
    connection, job, result, fetch, finalize = attempt
    result.update(status='partial', saved_assets={asset: {} for asset in ASSET_IDS[:-1]})
    if body is not None:
        fetch.return_value = (body, datetime.now(timezone.utc))
        result.update(status='failed', saved_assets={})
    success_before = value('etl_api_requests_total', {'outcome': 'success'})
    freshness_before = value('etl_last_complete_collection_timestamp_seconds')
    partial_before = value('etl_collections_total', {'status': result['status']})
    collector.collect_attempt(MagicMock(), connection, 'unused', job)
    assert value('etl_api_requests_total', {'outcome': 'success'}) == success_before + 1
    assert value('etl_collections_total', {'status': result['status']}) == partial_before + 1
    assert value('etl_last_complete_collection_timestamp_seconds') == freshness_before
    assert value('etl_missing_assets') == (5 if body is not None else 1)


# Failed-save metrics: Does not report completion or advance freshness when database saving fails.
def test_failed_save_does_not_report_complete_collection(attempt):
    connection, job, result, fetch, finalize = attempt
    finalize.side_effect = psycopg.errors.CheckViolation('permanent save failure')
    freshness_before = value('etl_last_complete_collection_timestamp_seconds')
    complete_before = value('etl_collections_total', {'status': 'complete'})
    with pytest.raises(psycopg.errors.CheckViolation):
        collector.collect_attempt(MagicMock(), connection, 'unused', job)
    assert value('etl_collections_total', {'status': 'complete'}) == complete_before
    assert value('etl_last_complete_collection_timestamp_seconds') == freshness_before


# Stage failures: Count the failing step once, not a later export that never started.
@pytest.mark.parametrize('stage', ['raw_export', 'transformation', 'processed_export'])
def test_downstream_failure_identifies_only_the_failing_stage(stage, monkeypatch, tmp_path):
    stages = ('raw_export', 'transformation', 'processed_export')
    def failures():
        return {name: monitoring.STAGE_REGISTRY.get_sample_value(
            'etl_stage_runs_total', {'stage': name, 'outcome': 'failure'}) for name in stages}
    monkeypatch.setattr(exports, 'is_exported', MagicMock(return_value=False))
    monkeypatch.setattr(downstream.markets, 'load_metrics', MagicMock(return_value=[]))
    normalize = MagicMock(return_value=[])
    publish = MagicMock(return_value={'appended': 1})
    if stage == 'transformation':
        normalize.side_effect = ValueError('Bad source record')
    else:
        publish.side_effect = ValueError('Output unavailable')
    monkeypatch.setattr(downstream.markets, 'normalize_markets', normalize)
    monkeypatch.setattr(downstream.markets, 'save_metrics', MagicMock())
    monkeypatch.setattr(downstream, 'build_record', MagicMock(return_value={}))
    monkeypatch.setattr(exports, 'publish_record', publish)
    before = failures()
    kind = 'raw' if stage == 'raw_export' else 'processed'
    with pytest.raises(ValueError):
        downstream.export_stage(MagicMock(), kind, tmp_path, {'requested': False},
                                {'response_id': uuid4(), 'raw_response': '{}'})
    assert failures() == {name: count + int(name == stage) for name, count in before.items()}
    if stage == 'transformation':
        publish.assert_not_called()


# Export backlog: Count complete and partial collections until their processed output is saved.
@pytest.mark.integration
def test_export_backlog_tracks_unfinished_processed_outputs(collections, tmp_path):
    connection, create = collections
    create()
    create('partial', ASSET_IDS[:-1])
    create('failed', ())
    destination = exports.destination_key(tmp_path)
    monitoring.refresh_export_backlog(connection, destination)
    assert monitoring.STAGE_REGISTRY.get_sample_value('etl_pending_exports') == 2
    before = {stage: monitoring.STAGE_REGISTRY.get_sample_value(
        'etl_stage_runs_total', {'stage': stage, 'outcome': 'success'})
        for stage in ('raw_export', 'transformation', 'processed_export')}
    downstream.export_once(connection, 'raw', tmp_path)
    monitoring.refresh_export_backlog(connection, destination)
    assert monitoring.STAGE_REGISTRY.get_sample_value('etl_pending_exports') == 2
    downstream.export_once(connection, 'processed', tmp_path)
    monitoring.refresh_export_backlog(connection, destination)
    assert monitoring.STAGE_REGISTRY.get_sample_value('etl_pending_exports') == 1
    for stage, count in before.items():
        assert monitoring.STAGE_REGISTRY.get_sample_value(
            'etl_stage_runs_total', {'stage': stage, 'outcome': 'success'}) == count + 1
