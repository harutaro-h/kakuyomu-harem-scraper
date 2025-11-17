#!/usr/bin/env python3
"""
Kakuyomu harem tag scraper.

This script enumerates Kakuyomu works tagged with "ハーレム" and filters them by:
- first episode posted on or after MIN_START_DATE (default: 2025-04-01, overridable)
- review star (point) count of at least 3,000
- total character count of at least 50,000
- tag includes "ハーレム"
- notice includes "性描写あり"

The results are written to `kakuyomu_harem_filtered.csv` in UTF-8 with BOM.
"""

from __future__ import annotations
import csv
import re
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# 設定
# ============================================================

BASE_URL = "https://kakuyomu.jp/tags/ハーレム"
DEFAULT_START_DATE = "2025-04-01"

_min = os.getenv("KAKUYOMU_MIN_START_DATE", DEFAULT_START_DATE)
try:
    MIN_START_DATE = datetime.fromisoformat(_min)
except Exception:
    MIN_START_DATE = datetime(2025, 4, 1)

MIN_STAR_COUNT = 3000
MIN_CHARACTER_COUNT = 50_000

REQUEST_DELAY_SECONDS = 2.0
LISTING_MAX_PAGES = 80
MAX_EPISODE_PAGES_PER_WORK = 50

OUTPUT_CSV = Path("kakuyomu_harem_filtered.csv")

LISTING_CARD_SELECTOR = "div.widget-workCard"
CARD_LINK_SELECTOR = "h3 a[href^='/works/'], a[href^='/works/']"

CARD_TAG_SELECTORS = (
    ".widget-workCard-tag",
    ".tag",
    "[data-testid='tag']",
)

STAR_SELECTORS = (
    "span.widget-workCard-reviewPoints",
    "span.widget-workCard-reviewPoint",
    "span.widget-workCard-reviewCount",
    "span.reviewPoints",
    "span.reviewPoint",
    "span.reviewCount",
    "[data-testid='review-point']",
    "[data-testid='review-count']",
)

CHAR_COUNT_SELECTORS = (
    "span.widget-workCard-charCount",
    "span.charCount",
    "[data-testid='char-count']",
)

DETAIL_STAR_SELECTORS = (
    "span.widget-workHeader-reviewPoints",
    "span.widget-workHeader-reviewPoint",
    "span.reviewPoints",
    "span.reviewPoint",
    "span.reviewCount",
    "[data-testid='review-point']",
    "[data-testid='review-count']",
)

DETAIL_CHAR_SELECTORS = (
    "span.widget-workHeader-charCount",
    "span.widget-workInfo-charCount",
    "span.charCount",
    "[data-testid='char-count']",
)

DETAIL_TAG_SELECTORS = (
    "[data-testid='tag']",
    ".widget-workTag-item",
    ".work-tag",
    ".tag",
)

NOTICE_SELECTORS = (
    ".workHeader-notice",
    ".widget-workHeader-notice",
    ".workNotice",
    "[data-testid='notice']",
    ".notice",
)

TEXTUAL_CHAR_LABELS = ("総文字数", "文字数")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ============================================================
# データモデル
# ============================================================

@dataclass
class WorkRecord:
    title: str
    url: str
    stars: int
    total_chars: int
    first_episode_date: datetime
    tags: Sequence[str]
    notice_sexual: bool

    def meets(self) -> bool:
        return (
            self.stars >= MIN_STAR_COUNT
            and self.total_chars >= MIN_CHARACTER_COUNT
            and self.first_episode_date >= MIN_START_DATE
            and any(tag == "ハーレム" for tag in self.tags)
            and self.notice_sexual
        )

    def as_csv_row(self) -> List[str]:
        return [
            self.title,
            self.url,
            str(self.stars),
            str(self.total_chars),
            self.first_episode_date.strftime("%Y-%m-%d"),
            " ".join(self.tags),
            "True" if self.notice_sexual else "False",
        ]

# ============================================================
# HTTP クライアント
# ============================================================

class KakuyomuClient:
    def __init__(self, delay_seconds: float = REQUEST_DELAY_SECONDS) -> None:
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers["User-Agent"] = USER_AGENT
        self.delay_seconds = delay_seconds

    def fetch_html(self, url: str) -> str:
        resp = self.session.get(url, timeout=20)
        resp.raise_for_status()
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        return resp.text

# ============================================================
# HTML Utility
# ============================================================

def build_listing_url(page: int) -> str:
    u = urlsplit(BASE_URL)
    q = dict(parse_qsl(u.query))
    q.setdefault("sort", "popular")
    q["page"] = str(page)
    return urlunsplit((u.scheme, u.netloc, u.path, urlencode(q), u.fragment))

def clean_number(s: str) -> Optional[int]:
    digits = re.sub(r"[^\d]", "", s or "")
    return int(digits) if digits else None

def collect_texts(soup: BeautifulSoup | Tag, selectors: Sequence[str]) -> List[str]:
    values: List[str] = []
    for sel in selectors:
        for el in soup.select(sel):
            t = el.get_text(strip=True)
            if t:
                values.append(t)
    return values

def extract_number(soup: BeautifulSoup | Tag, selectors: Sequence[str]) -> Optional[int]:
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            n = clean_number(el.get_text(strip=True))
            if n is not None:
                return n
    return None

def extract_tags(soup: BeautifulSoup | Tag) -> List[str]:
    tags = collect_texts(soup, CARD_TAG_SELECTORS)
    if not tags:
        tags = collect_texts(soup, DETAIL_TAG_SELECTORS)
    uniq = []
    for t in tags:
        if t not in uniq:
            uniq.append(t)
    return uniq

def extract_total_chars_from_textlabel(soup: BeautifulSoup) -> Optional[int]:
    for label in TEXTUAL_CHAR_LABELS:
        dt = soup.find("dt", string=lambda v: isinstance(v, str) and label in v)
        if dt:
            dd = dt.find_next("dd")
            if dd:
                n = clean_number(dd.get_text(strip=True))
                if n is not None:
                    return n
    return None

def detect_notice_sexual(soup: BeautifulSoup) -> bool:
    for sel in NOTICE_SELECTORS:
        for el in soup.select(sel):
            if "性描写あり" in el.get_text(" ", strip=True):
                return True
    return "性描写あり" in soup.get_text(" ", strip=True)

def parse_card(card: Tag):
    link = card.select_one(CARD_LINK_SELECTOR)
    if not link or not link.get("href"):
        raise ValueError("Missing work link")
    title = link.get_text(strip=True)
    url = urljoin("https://kakuyomu.jp", link["href"])
    stars = extract_number(card, STAR_SELECTORS)
    chars = extract_number(card, CHAR_COUNT_SELECTORS)
    tags = extract_tags(card)
    return title, url, stars, chars, tags

def parse_detail(html: str):
    soup = BeautifulSoup(html, "lxml")
    stars = extract_number(soup, DETAIL_STAR_SELECTORS)
    chars = extract_number(soup, DETAIL_CHAR_SELECTORS)
    if chars is None:
        chars = extract_total_chars_from_textlabel(soup)
    tags = extract_tags(soup)
    notice = detect_notice_sexual(soup)
    return stars, chars, tags, notice

def collect_episode_datetimes(soup: BeautifulSoup) -> List[datetime]:
    datetimes = []
    for tag in soup.select(
        "section[id*='episode'] time[datetime], "
        "article time[datetime], "
        "li time[datetime], "
        "time[datetime]"
    ):
        raw = (tag.get("datetime") or "")[:10]
        try:
            datetimes.append(datetime.fromisoformat(raw))
        except Exception:
            pass
    return datetimes

def find_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    next_link = soup.select_one("a[rel='next']")
    if next_link and next_link.get("href"):
        return urljoin("https://kakuyomu.jp", next_link["href"])

    alt = soup.select_one(".pager__item--next a, .pagination-next a")
    if alt and alt.get("href"):
        return urljoin("https://kakuyomu.jp", alt["href"])

    for a in soup.select("a"):
        if a.get_text(strip=True) in ("次へ", "次", "Next") and a.get("href"):
            return urljoin("https://kakuyomu.jp", a["href"])

    return None

def fetch_first_episode_date(client: KakuyomuClient, episodes_url: str) -> Optional[datetime]:
    visited = set()
    url = episodes_url
    page_count = 0
    all_dates: List[datetime] = []
    min_seen = None

    while url and url not in visited and page_count < MAX_EPISODE_PAGES_PER_WORK:
        visited.add(url)
        html = client.fetch_html(url)
        soup = BeautifulSoup(html, "lxml")
        dates = collect_episode_datetimes(soup)
        all_dates.extend(dates)

        if dates:
            page_min = min(dates)
            if min_seen is None or page_min < min_seen:
                min_seen = page_min

        if min_seen is not None and min_seen < MIN_START_DATE:
            break

        url = find_next_page_url(soup)
        page_count += 1

    return min(all_dates) if all_dates else None


# ============================================================
# スクレイピングメイン
# ============================================================

def scrape_harem_works(
    client: Optional[KakuyomuClient] = None,
    listing_max_pages: int = LISTING_MAX_PAGES,
) -> List[WorkRecord]:

    client = client or KakuyomuClient()
    records: List[WorkRecord] = []

    for page in range(1, listing_max_pages + 1):
        listing_url = build_listing_url(page)
        try:
            listing_html = client.fetch_html(listing_url)
        except Exception as exc:
            print(f"[ERROR] Listing fetch failed {listing_url}: {exc}")
            break

        soup = BeautifulSoup(listing_html, "lxml")
        cards = soup.select(LISTING_CARD_SELECTOR)

        if not cards:
            cards = [a.parent for a in soup.select("h3 a[href^='/works/']")]
        if not cards:
            break

        for card in cards:
            try:
                title, url, stars_c, chars_c, tags = parse_card(card)
            except ValueError:
                continue

            # 一覧段階 (前処理フィルタ)
            if stars_c is not None and stars_c < MIN_STAR_COUNT:
                continue
            if chars_c is not None and chars_c < MIN_CHARACTER_COUNT:
                continue

            try:
                detail_html = client.fetch_html(url)
            except Exception as exc:
                print(f"[ERROR] Failed detail fetch {url}: {exc}")
                continue

            detail_stars, detail_chars, detail_tags, notice = parse_detail(detail_html)

            stars = detail_stars if detail_stars is not None else stars_c
            chars = detail_chars if detail_chars is not None else chars_c
            tags = detail_tags or tags

            # 詳細段階のフィルタ
            if stars is None or stars < MIN_STAR_COUNT:
                continue
            if chars is None or chars < MIN_CHARACTER_COUNT:
                continue
            if not notice:
                continue

            # /episodes 全ページをたどって初回日時を取得
            episodes_url = url.rstrip("/") + "/episodes"
            try:
                first_dt = fetch_first_episode_date(client, episodes_url)
            except Exception as exc:
                print(f"[ERROR] Episode fetch error {episodes_url}: {exc}")
                continue

            if first_dt is None or first_dt < MIN_START_DATE:
                continue

            record = WorkRecord(
                title=title,
                url=url,
                stars=stars,
                total_chars=chars,
                first_episode_date=first_dt,
                tags=tags,
                notice_sexual=notice,
            )
            records.append(record)

    return records


# ============================================================
# CSV 出力
# ============================================================

def write_csv(records: Sequence[WorkRecord], path: Path) -> None:
    path.parent.mkdir(exist_ok=True, parents=True)
    headers = [
        "title",
        "url",
        "stars",
        "total_chars",
        "first_episode_date",
        "tags",
        "notice_sexual",
    ]

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for r in records:
            writer.writerow(r.as_csv_row())


# ============================================================
# メイン
# ============================================================

def main() -> None:
    records = scrape_harem_works()
    write_csv(records, OUTPUT_CSV)
    print(f"Works scanned (valid & included) : {len(records)}")
    print(f"Output written to: {OUTPUT_CSV.resolve()}")


# ============================================================
# Mock テスト（オフライン検証用）
# ============================================================

class MockKakuyomuClient(KakuyomuClient):
    def __init__(self, responses: Mapping[str, str]):
        super().__init__(delay_seconds=0)
        self.responses = dict(responses)

    def fetch_html(self, url: str) -> str:
        if url not in self.responses:
            raise requests.HTTPError(f"Mock missing for {url}")
        return self.responses[url]


def _build_mock_html() -> Mapping[str, str]:
    """オフライン検証用 HTML セット"""

    listing_page = """
    <html><body>
      <div class="widget-workCard">
        <h3><a href="/works/111">Hero and Harem</a></h3>
        <span class="widget-workCard-reviewPoints">3500</span>
        <span class="widget-workCard-charCount">60000</span>
        <span class="widget-workCard-tag">ハーレム</span>
      </div>
      <div class="widget-workCard">
        <h3><a href="/works/222">No Notice</a></h3>
        <span class="widget-workCard-reviewPoints">3500</span>
        <span class="widget-workCard-charCount">60000</span>
        <span class="widget-workCard-tag">ハーレム</span>
      </div>
      <a rel="next" href="/tags/ハーレム?sort=popular&page=2">次</a>
    </body></html>
    """

    empty_listing = "<html><body>no more</body></html>"

    detail_111 = """
    <html><body>
      <span class="widget-workHeader-reviewPoints">3600</span>
      <span class="widget-workHeader-charCount">62000</span>
      <div data-testid="tag">ハーレム</div>
      <div class="workHeader-notice">性描写あり</div>
    </body></html>
    """

    detail_222 = """
    <html><body>
      <span class="widget-workHeader-reviewPoints">3600</span>
      <span class="widget-workHeader-charCount">62000</span>
      <div data-testid="tag">ハーレム</div>
      <div class="workHeader-notice">全年齢</div>
    </body></html>
    """

    episodes_111 = """
    <html><body>
      <section id="episodes"><time datetime="2025-04-15">2025-04-15</time></section>
    </body></html>
    """

    episodes_222 = """
    <html><body>
      <section id="episodes"><time datetime="2024-03-01">2024-03-01</time></section>
    </body></html>
    """

    return {
        build_listing_url(1): listing_page,
        build_listing_url(2): empty_listing,
        "https://kakuyomu.jp/works/111": detail_111,
        "https://kakuyomu.jp/works/222": detail_222,
        "https://kakuyomu.jp/works/111/episodes": episodes_111,
        "https://kakuyomu.jp/works/222/episodes": episodes_222,
    }


def run_mock_tests() -> None:
    print("Running mock tests…")
    mock = MockKakuyomuClient(_build_mock_html())
    recs = scrape_harem_works(mock, listing_max_pages=2)

    assert len(recs) == 1, f"Expected 1 record, got {len(recs)}"
    r = recs[0]
    assert r.title == "Hero and Harem"
    assert r.notice_sexual
    assert r.stars >= MIN_STAR_COUNT
    assert r.total_chars >= MIN_CHARACTER_COUNT
    assert r.first_episode_date >= MIN_START_DATE
    assert "ハーレム" in r.tags
    print("Mock tests passed!")


if __name__ == "__main__":
    import sys
    if "--run-mock-tests" in sys.argv:
        run_mock_tests()
    else:
        main()
