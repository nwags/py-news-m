import pandas as pd

from py_news.config import load_config
from py_news.models import AUGMENTATION_ARTIFACT_COLUMNS, AUGMENTATION_EVENT_COLUMNS, AUGMENTATION_RUN_COLUMNS
from py_news.pipelines.refdata_refresh import run_refdata_refresh
from py_news.storage.paths import normalized_artifact_path


def test_refdata_refresh_materializes_reserved_augmentation_artifacts(tmp_path):
    config = load_config(project_root=tmp_path, cache_root=tmp_path / ".news_cache")
    run_refdata_refresh(config)

    runs_path = normalized_artifact_path(config, "augmentation_runs")
    events_path = normalized_artifact_path(config, "augmentation_events")
    artifacts_path = normalized_artifact_path(config, "augmentation_artifacts")
    assert runs_path.exists()
    assert events_path.exists()
    assert artifacts_path.exists()

    runs = pd.read_parquet(runs_path)
    events = pd.read_parquet(events_path)
    artifacts = pd.read_parquet(artifacts_path)
    assert list(runs.columns) == AUGMENTATION_RUN_COLUMNS
    assert list(events.columns) == AUGMENTATION_EVENT_COLUMNS
    assert list(artifacts.columns) == AUGMENTATION_ARTIFACT_COLUMNS
