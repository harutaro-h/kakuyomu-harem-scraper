#!/usr/bin/env python3
"""Kakuyomu harem tag scraper.
This script enumerates Kakuyomu works tagged with "ハーレム" and filters them by:
- first episode posted on or after 2025-04-01
- review star (point) count of at least 3,000
- total character count of at least 50,000
The results are written to ``kakuyomu_harem_filtered.csv`` in UTF-8 with BOM so the
file can be opened in spreadsheet software without mojibake.
"""
from __future__ import annotations
import csv
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
BASE_URL = "https://kakuyomu.jp/tags/ハーレム"
MIN_START_DATE = datetime(2025, 4, 1)
MIN_STAR_COUNT = 3000
MIN_CHARACTER_COUNT = 50_000
# ★ 負荷軽減のためリクエスト間隔を 2 秒に伸ばす
REQUEST_DELAY_SECONDS = 2.0
# ★ 一覧ページの上限を控えめに（必要なら増やす）
LISTING_MAX_PAGES = 80
# ★ 1作品あたり /episodes を何ページまで見るかの上限
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
TEXTUAL_CHAR_LABELS = ("総文字数", "文字数")
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
@dataclass
class WorkRecord:
    """Represents a Kakuyomu work and the data required for filtering."""
    title: str
    url: str
    stars: Optional[int]
    total_chars: Optional[int]
    first_episode_date: Optional[datetime]
    tags: Sequence[str]
    def meets_requirements(self) -> bool:
        return (
            isinstance(self.stars, int)
            and self.stars >= MIN_STAR_COUNT
            and isinstance(self.total_chars, int)
            and self.total_chars >= MIN_CHARACTER_COUNT
            and isinstance(self.first_episode_date, datetime)
            and self.first_episode_date >= MIN_START_DATE
            and any(tag == "ハーレム" for tag in self.tags)
        )
    def as_csv_row(self) -> List[str]:
        return [
            self.title,
            self.url,
            str(self.stars or ""),
            str(self.total_chars or ""),
            self.first_episode_date.strftime("%Y-%m-%d") if self.first_episode_date else "",
            " ".join(self.tags),
        ]
class KakuyomuClient:
    """HTTP client with retry and polite throttling."""
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
        response = self.session.get(url, timeout=20)
        response.raise_for_status()
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        return response.text
        

def is_not_found_error(error: requests.HTTPError) -> bool:
    response = getattr(error, "response", None)
    return bool(response and response.status_code == 404)
def build_listing_url(page: int) -> str:
    """Return the harem tag listing URL for a specific page."""
    base_parts = urlsplit(BASE_URL)
    query = dict(parse_qsl(base_parts.query))
    query.setdefault("sort", "popular")
    query["page"] = str(page)
    new_query = urlencode(query, doseq=True)
    return urlunsplit((base_parts.scheme, base_parts.netloc, base_parts.path, new_query, base_parts.fragment))
def clean_number(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    return int(digits) if digits else None
def collect_texts(container: BeautifulSoup | Tag, selectors: Sequence[str]) -> List[str]:
    values: List[str] = []
    for selector in selectors:
        for element in container.select(selector):
            text = element.get_text(strip=True)
            if text:
                values.append(text)
    return values
def extract_number_from_selectors(container: BeautifulSoup | Tag, selectors: Sequence[str]) -> Optional[int]:
    for selector in selectors:
        element = container.select_one(selector)
        if element:
            number = clean_number(element.get_text(strip=True))
            if number is not None:
                return number
    return None
def extract_tags(container: BeautifulSoup | Tag) -> List[str]:
    tags = collect_texts(container, CARD_TAG_SELECTORS)
    if not tags:
        tags = collect_texts(container, DETAIL_TAG_SELECTORS)
    seen: List[str] = []
    for tag in tags:
        if tag not in seen:
            seen.append(tag)
    return seen
def extract_characters_from_textual_labels(soup: BeautifulSoup) -> Optional[int]:
    for label in TEXTUAL_CHAR_LABELS:
        dt = soup.find("dt", string=lambda value: isinstance(value, str) and label in value)
        if dt:
            dd = dt.find_next("dd")
            if dd:
                number = clean_number(dd.get_text(strip=True))
                if number is not None:
                    return number
    return None
def parse_card(card: Tag) -> Tuple[str, str, Optional[int], Optional[int], List[str]]:
    link = card.select_one(CARD_LINK_SELECTOR)
    if not link or not link.get("href"):
        raise ValueError("Work card missing link")
    title = link.get_text(strip=True)
    url = urljoin("https://kakuyomu.jp", link["href"])
    stars = extract_number_from_selectors(card, STAR_SELECTORS)
    char_count = extract_number_from_selectors(card, CHAR_COUNT_SELECTORS)
    tags = extract_tags(card)
    return title, url, stars, char_count, tags
def parse_detail_page(html: str) -> Tuple[Optional[int], Optional[int], List[str]]:
    soup = BeautifulSoup(html, "lxml")
    stars = extract_number_from_selectors(soup, DETAIL_STAR_SELECTORS)
    char_count = extract_number_from_selectors(soup, DETAIL_CHAR_SELECTORS)
    if char_count is None:
        char_count = extract_characters_from_textual_labels(soup)
    tags = extract_tags(soup)
    return stars, char_count, tags
def collect_episode_datetimes(html: str) -> List[datetime]:
    soup = BeautifulSoup(html, "lxml")
    datetimes: List[datetime] = []
    for time_tag in soup.select("time[datetime]"):
        raw = (time_tag.get("datetime") or "")[:10]
        try:
            datetimes.append(datetime.fromisoformat(raw))
        except ValueError:
            continue
    return datetimes
def find_next_page_url(soup: BeautifulSoup) -> Optional[str]:
    next_link = soup.select_one("a[rel='next']")
    if next_link and next_link.get("href"):
        return urljoin("https://kakuyomu.jp", next_link["href"])
    alt_link = soup.select_one(".pager__item--next a, .pagination-next a")
    if alt_link and alt_link.get("href"):
        return urljoin("https://kakuyomu.jp", alt_link["href"])
    for anchor in soup.select("a"):
        if anchor.get_text(strip=True) in ("次へ", "次", "Next") and anchor.get("href"):
            return urljoin("https://kakuyomu.jp", anchor["href"])
    return None
def fetch_first_episode_date(client: KakuyomuClient, episodes_url: str) -> Optional[datetime]:
    visited: set[str] = set()
    current_url = episodes_url
    page_count = 0
    all_dates: List[datetime] = []
    min_seen: Optional[datetime] = None

    while (
        current_url
        and current_url not in visited
        and page_count < MAX_EPISODE_PAGES_PER_WORK
    ):
        visited.add(current_url)
        try:
            html = client.fetch_html(current_url)
        except requests.HTTPError as error:
            if is_not_found_error(error):
                break
            raise
        soup = BeautifulSoup(html, "lxml")

        page_dates = collect_episode_datetimes(html)
        all_dates.extend(page_dates)
        if page_dates:
            page_min = min(page_dates)
            if min_seen is None or page_min < min_seen:
                min_seen = page_min

        # ★ すでに MIN_START_DATE より古い話が見つかっていれば、これ以上巡回しない
        if min_seen is not None and min_seen < MIN_START_DATE:
            break

        current_url = find_next_page_url(soup)
        page_count += 1

    return min(all_dates) if all_dates else None
def scrape_harem_works() -> List[WorkRecord]:
    client = KakuyomuClient()
    records: List[WorkRecord] = []
    for page in range(1, LISTING_MAX_PAGES + 1):
        listing_url = build_listing_url(page)
        listing_html = client.fetch_html(listing_url)
        soup = BeautifulSoup(listing_html, "lxml")
        cards = soup.select(LISTING_CARD_SELECTOR)
        if not cards:
            # fallback: some pages wrap cards differently
            cards = [anchor.parent for anchor in soup.select("h3 a[href^='/works/']")]
        if not cards:
            break
        for card in cards:
            try:
                title, url, stars, char_count, tags = parse_card(card)
            except ValueError:
                continue

            # ★ 一覧カード時点で明らかに条件未満なら詳細/episodesを見に行かない
            if stars is not None and stars < MIN_STAR_COUNT:
                continue
            if char_count is not None and char_count < MIN_CHARACTER_COUNT:
                continue
            try:
                detail_html = client.fetch_html(url)
            except requests.HTTPError as error:
                if is_not_found_error(error):
                    continue
                raise
            detail_stars, detail_chars, detail_tags = parse_detail_page(detail_html)
            if detail_stars is not None:
                stars = detail_stars
            if detail_chars is not None:
                char_count = detail_chars
            if detail_tags:
                tags = detail_tags

            # ★ 詳細を見ても条件未満なら episodes に行かない
            if stars is None or stars < MIN_STAR_COUNT:
                continue
            if char_count is None or char_count < MIN_CHARACTER_COUNT:
                continue

            episodes_url = url.rstrip("/") + "/episodes"
            try:
                first_episode_date = fetch_first_episode_date(client, episodes_url)
            except requests.HTTPError as error:
                if is_not_found_error(error):
                    continue
                raise

            record = WorkRecord(
                title=title,
                url=url,
                stars=stars,
                total_chars=char_count,
                first_episode_date=first_episode_date,
                tags=tags,
            )
            records.append(record)
        # continue until the site stops returning cards
    return records
def filter_records(records: Iterable[WorkRecord]) -> List[WorkRecord]:
    return [record for record in records if record.meets_requirements()]
def write_csv(records: Sequence[WorkRecord], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    headers = ["title", "url", "stars", "total_chars", "first_episode_date", "tags"]
    with destination.open("w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        for record in records:
            writer.writerow(record.as_csv_row())
def main() -> None:
    all_records = scrape_harem_works()
    filtered_records = filter_records(all_records)
    write_csv(filtered_records, OUTPUT_CSV)
    print(f"Total works scanned: {len(all_records)}")
    print(f"Works meeting criteria: {len(filtered_records)}")
    print(f"Output written to: {OUTPUT_CSV.resolve()}")
if __name__ == "__main__":
    main()
