# kakuyomu_harem_scraper.py
# -*- coding: utf-8 -*-
import os, re, csv, time
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl, urljoin

from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ===== 設定 =====
BASE_URL = "https://kakuyomu.jp/tags/ハーレム?sort=popular"

ALL_CSV_NAME       = "kakuyomu_harem_all.csv"        # 全件（監査用）
FILTERED_CSV_NAME  = "kakuyomu_harem_filtered.csv"   # 条件一致のみ（これが最終成果物）
ARTIFACT_DIR_NAME  = "artifacts"
MISMATCH_CSV_NAME  = "mismatch.csv"

MIN_STARS = 3000
MIN_CHARS = 50000
# 最低公開日。環境変数 KAKUYOMU_MIN_DATE (YYYY-MM-DD) で上書きできる。
# 直近作品も拾いたい場合は 2020-01-01 等の過去日付に調整する。
_MIN_DATE_ENV_VAR = "KAKUYOMU_MIN_DATE"
_DEFAULT_MIN_DATE = "2018-01-01"
_min_date_raw = os.environ.get(_MIN_DATE_ENV_VAR, _DEFAULT_MIN_DATE)
try:
    MIN_DATE = datetime.fromisoformat(_min_date_raw)
except ValueError:
    raise ValueError(
        f"Invalid date for {_MIN_DATE_ENV_VAR}: '{_min_date_raw}'. Use YYYY-MM-DD format."
    )

MAX_PAGES      = 80   # 一覧側の上限（必要なら増やす）
PAGE_SLEEP     = 1.0
DETAIL_SLEEP   = 0.8
EPAGE_SLEEP    = 0.6  # 目次ページ間の待機
# =================


# ---------- 共通ユーティリティ ----------
def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,768")
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"--user-agent={ua}")
    chrome_path = os.environ.get("CHROME_PATH") or os.environ.get("CHROME_BIN")
    if chrome_path:
        opts.binary_location = chrome_path
    return webdriver.Chrome(options=opts)

def build_page_url(base: str, page: int) -> str:
    u = urlsplit(base)
    q = dict(parse_qsl(u.query))
    q["page"] = str(page)
    new_q = urlencode(q, doseq=True)
    return urlunsplit((u.scheme, u.netloc, u.path, new_q, u.fragment))

def parse_number(s: Optional[str]) -> Optional[int]:
    if not s: return None
    m = re.sub(r"[^\d]", "", s)
    return int(m) if m.isdigit() else None

def collect_tags(container: BeautifulSoup) -> str:
    tags = []
    for sel in [".widget-workCard-tag", ".tag", "[data-testid='tag']"]:
        for t in container.select(sel):
            s = t.get_text(strip=True)
            if s: tags.append(s)
    return " ".join(tags)

def detect_sexual_notice(detail_soup: BeautifulSoup) -> bool:
    for sel in [".workHeader-notice", ".notice", "[data-testid='notice']"]:
        bloc = detail_soup.select(sel)
        if bloc:
            text = " ".join(el.get_text(" ", strip=True) for el in bloc)
            if "性描写あり" in text:
                return True
    return "性描写あり" in detail_soup.get_text(" ", strip=True)

def pick_first_date_from_html(html: str) -> List[datetime]:
    soup = BeautifulSoup(html, "lxml")
    dts = []
    for t in soup.select("time[datetime]"):
        val = (t.get("datetime") or "")[:10]
        try:
            dts.append(datetime.fromisoformat(val))
        except:
            pass
    return dts
# ----------------------------------------


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_html(driver, url: str) -> str:
    driver.get(url)
    time.sleep(DETAIL_SLEEP)
    return driver.page_source


def card_select_all(html: str) -> Tuple[BeautifulSoup, List]:
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.widget-workCard")
    if not cards:
        cards = [a.parent for a in soup.select("h3 a[href^='/works/']")]
    return soup, cards

def card_title_and_url(card) -> Optional[tuple]:
    a = card.select_one("h3 a[href^='/works/']") or card.select_one("a[href^='/works/']")
    if not a: return None
    return a.get_text(strip=True), "https://kakuyomu.jp" + a["href"]

def card_stars(card) -> Optional[int]:
    for sel in ["span.widget-workCard-reviewCount", "span.reviewCount", "[data-testid='review-count']"]:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            return parse_number(el.get_text(strip=True))
    return None

def card_chars(card) -> Optional[int]:
    for sel in ["span.widget-workCard-charCount", "span.charCount", "[data-testid='char-count']"]:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            return parse_number(el.get_text(strip=True))
    return None

def detail_fallbacks_if_missing(stars, chars, detail_soup):
    if chars is None:
        for sel in ["span.charCount", "[data-testid='char-count']"]:
            el = detail_soup.select_one(sel)
            if el and el.get_text(strip=True):
                chars = parse_number(el.get_text(strip=True))
                break
    return stars, chars


def crawl_all_episode_pages(driver, episodes_url: str, artifact_dir: Path, work_id: str) -> List[datetime]:
    """
    目次（/episodes）をページ送りして、全ページの <time datetime> を集約。
    次ページ判定は rel=next / .pager__item--next / 「次へ」リンク などを順に探す。
    """
    all_dates: List[datetime] = []
    seen_urls = set()
    page_idx = 1
    url = episodes_url

    while url and url not in seen_urls and page_idx <= 200:  # 安全上限
        seen_urls.add(url)
        html = fetch_html(driver, url)
        (artifact_dir / f"episodes_{work_id}_{page_idx}.html").write_text(html, encoding="utf-8")
        soup = BeautifulSoup(html, "lxml")

        # そのページの日時を追加
        all_dates.extend(pick_first_date_from_html(html))

        # 次ページ探索
        next_href = None
        # 1) rel=next
        a = soup.select_one("a[rel='next']")
        if a and a.get("href"): next_href = a["href"]
        # 2) クラス系
        if not next_href:
            b = soup.select_one(".pager__item--next a, .pagination-next a")
            if b and b.get("href"): next_href = b["href"]
        # 3) 文言でのフォールバック
        if not next_href:
            for c in soup.select("a"):
                txt = c.get_text(strip=True)
                if txt in ("次へ", "次", "Next") and c.get("href"):
                    next_href = c["href"]; break

        if next_href:
            url = urljoin("https://kakuyomu.jp", next_href)
            page_idx += 1
            time.sleep(EPAGE_SLEEP)
        else:
            break

    return all_dates


def main():
    driver = get_driver()

    # 出力先の絶対パス
    workspace      = Path.cwd()
    ARTIFACT_PATH  = workspace / ARTIFACT_DIR_NAME
    ARTIFACT_PATH.mkdir(exist_ok=True, parents=True)
    ALL_PATH       = workspace / ALL_CSV_NAME
    FILTERED_PATH  = workspace / FILTERED_CSV_NAME
    MISMATCH_PATH  = ARTIFACT_PATH / MISMATCH_CSV_NAME

    all_rows: List[dict] = []
    mismatch_rows: List[dict] = []
    last_page = 0

    for page in range(1, MAX_PAGES + 1):
        page_url = build_page_url(BASE_URL, page)
        html = fetch_html(driver, page_url)
        (ARTIFACT_PATH / f"page_{page}.html").write_text(html, encoding="utf-8")
        driver.save_screenshot(str(ARTIFACT_PATH / f"page_{page}.png"))

        soup, cards = card_select_all(html)
        if not cards:
            print(f"page {page}: no cards -> stop")
            break

        print(f"page {page}: cards={len(cards)}")
        last_page = page

        for idx, card in enumerate(cards, 1):
            tit_url = card_title_and_url(card)
            if not tit_url: continue
            title, work_url = tit_url

            stars = card_stars(card)
            chars = card_chars(card)
            tags_list = collect_tags(card)
            has_harem = "ハーレム" in (tags_list or "")

            # 作品詳細
            detail_html = fetch_html(driver, work_url)
            detail_soup = BeautifulSoup(detail_html, "lxml")
            notice = detect_sexual_notice(detail_soup)
            tags_detail = collect_tags(detail_soup)

            # 目次（全ページ巡回して最古日を初回公開日に）
            episodes_url = work_url + "/episodes"
            # work_id は /works/xxxxxxxxxxxx の末尾を想定
            work_id = work_url.rstrip("/").split("/")[-1]
            try:
                all_dates = crawl_all_episode_pages(driver, episodes_url, ARTIFACT_PATH, work_id)
            except Exception:
                # /episodes が無い等 → 詳細ページ内の <time> で代用
                all_dates = pick_first_date_from_html(detail_html)

            first_date = min(all_dates) if all_dates else None

            # 欠損フォールバック
            stars, chars = detail_fallbacks_if_missing(stars, chars, detail_soup)

            meets = (
                isinstance(stars, int) and stars >= MIN_STARS and
                isinstance(chars, int) and chars >= MIN_CHARS and
                bool(notice) and bool(has_harem) and
                (first_date is not None and first_date >= MIN_DATE)
            )

            row = {
                "title": title,
                "url": work_url,
                "stars": stars if stars is not None else "",
                "total_chars": chars if chars is not None else "",
                "notice_sexual": notice,
                "first_episode_date": first_date.strftime("%Y-%m-%d") if first_date else "",
                "tags": tags_list,
                "tags_detail": tags_detail,
                "meets_conditions": meets
            }
            all_rows.append(row)

            # 不完全な行は mismatch 候補として保存
            if (row["stars"] == "" or row["total_chars"] == "" or not row["notice_sexual"] or "ハーレム" not in str(row["tags"]) or row["first_episode_date"] == ""):
                mismatch_rows.append(row)

        time.sleep(PAGE_SLEEP)

    driver.quit()

    # --- CSV出力 ---
    headers = ["title","url","stars","total_chars","notice_sexual","first_episode_date","tags","tags_detail","meets_conditions"]

    # 全件（監査）
    with open(ALL_PATH, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers); w.writeheader(); w.writerows(all_rows)

    # 条件一致のみ（最終成果物）
    filtered = [r for r in all_rows if r["meets_conditions"]]
    with open(FILTERED_PATH, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers); w.writeheader(); w.writerows(filtered)

    # mismatch
    if mismatch_rows:
        with open(MISMATCH_PATH, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=headers); w.writeheader(); w.writerows(mismatch_rows)

    # サマリ
    (ARTIFACT_PATH / "scrape_summary.txt").write_text(
        f"pages_processed={last_page}\nrows_all={len(all_rows)}\nrows_filtered={len(filtered)}\n",
        encoding="utf-8"
    )
    print(f"Saved: ALL={ALL_PATH} ({len(all_rows)})  FILTERED={FILTERED_PATH} ({len(filtered)})  mismatch={len(mismatch_rows)}")


if __name__ == "__main__":
    main()

