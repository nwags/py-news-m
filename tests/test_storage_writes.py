import json

import pandas as pd

from py_news.storage.writes import upsert_parquet, write_json, write_text


def test_write_text_and_json_create_parent_dirs(tmp_path):
    text_path = tmp_path / "a" / "b" / "c.txt"
    json_path = tmp_path / "x" / "y" / "z.json"

    write_text(text_path, "hello")
    write_json(json_path, {"k": "v"})

    assert text_path.read_text(encoding="utf-8") == "hello"
    assert json.loads(json_path.read_text(encoding="utf-8")) == {"k": "v"}


def test_upsert_parquet_is_idempotent(tmp_path):
    path = tmp_path / "articles.parquet"

    upsert_parquet(path, rows=[{"article_id": "a1", "title": "old"}], dedupe_keys=["article_id"])
    upsert_parquet(path, rows=[{"article_id": "a1", "title": "new"}], dedupe_keys=["article_id"])

    df = pd.read_parquet(path)
    assert len(df) == 1
    assert df.iloc[0]["article_id"] == "a1"
    assert df.iloc[0]["title"] == "new"
