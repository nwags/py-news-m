import pandas as pd

from py_news.config import load_config
from py_news.models import RECONCILIATION_DISCREPANCY_COLUMNS, RECONCILIATION_EVENT_COLUMNS
from py_news.pipelines.refdata_refresh import run_refdata_refresh
from py_news.storage.paths import normalized_artifact_path


def test_refdata_refresh_materializes_reserved_reconciliation_artifacts(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)

    events_path = normalized_artifact_path(config, "reconciliation_events")
    discrepancies_path = normalized_artifact_path(config, "reconciliation_discrepancies")
    assert events_path.exists()
    assert discrepancies_path.exists()

    events = pd.read_parquet(events_path)
    discrepancies = pd.read_parquet(discrepancies_path)
    assert list(events.columns) == RECONCILIATION_EVENT_COLUMNS
    assert list(discrepancies.columns) == RECONCILIATION_DISCREPANCY_COLUMNS
