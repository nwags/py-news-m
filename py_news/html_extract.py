"""Modest HTML-to-text extraction helpers for content fetch."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

_WS_RE = re.compile(r"\s+")


def extract_visible_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag_name in ("script", "style", "noscript"):
        for node in soup.find_all(tag_name):
            node.decompose()

    body = soup.body if soup.body is not None else soup
    text = body.get_text(" ", strip=True)
    return _WS_RE.sub(" ", text).strip()
