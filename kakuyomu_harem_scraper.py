# kakuyomu_harem_scraper.py
# -*- coding: utf-8 -*-
"""
Kakuyomu タグ「ハーレム」を人気順でページ送り (?page=2,3,...) しながら全巡回。
各作品について:
- ★数
- 総文字数
- 注意書き「性描写あり」
- 初回公開日（目次の最古 <time datetime>）
- タグ（一覧/詳細の両方）
を取得し、以下の AND 条件でフィルタした結果を CSV に出力（0件でもヘッダ付き）:

  初回公開日 >= 2025-04-01
  ★ >= 3000
  「性描写あり」
  文字数 >= 50000
  タグに「ハーレム」を含む（複合タグも可）

さらに artifacts/ に HTML・スクショ・不整合候補を保存（Actions の Artifacts でDL可）。
"""

import os, re, csv, time
from pathlib import Path
from datetime import datetime
from typing import Optional, List
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl

from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ===== 設定 =====
BASE_URL = "https://kakuyomu.jp/tags/ハーレム?sort=popular"
OUT_CSV = "kakuyomu_harem_filtered.csv"
ARTIFACT_DIR = Path("artifacts"); ARTIFACT_DIR.mkdir(exist_ok=True)
MISMATCH_CSV = ARTIFACT_DIR / "mismatch.csv"

MIN_STARS = 3000
MIN_CHARS = 50000
MIN_DATE = datetime(2025, 4, 1)

MAX_PAGES = 50          # 安全上限（必要に応じて増やす）
PAGE_SLEEP = 1.0        # ページ間の待機
DETAIL_SLEEP = 0.8      # 作品詳細取得の待機
# =================


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
    """BASE_URL に page パラメータを付加/上書き"""
    u = urlsplit(base)
    q = dict(parse_qsl(u.query))
    q["page"] = str(page)
    new_q = urlencode(q, doseq=True)
    return urlunsplit((u.scheme, u.netloc, u.path, new_q, u.fragment))

def parse_number(s: Optional[str]) -> Optional[int]:
    if not s: return None
    m = re.sub(r"[^\d]", "", s)
    return int(m) if m.isdigit() else None

def pick_first_date_from_html(html: str) -> Optional[datetime]:
    soup = BeautifulSoup(html, "lxml")
    times = [t.get("datetime", "")[:10] for t in soup.select("time[datetime]")]
    dates: List[datetime] = []
    for d in times:
        try: dates.append(datetime.fromisoformat(d))
        except: pass
    return min(dates) if dates else None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_html(driver, url: str) -> str:
    driver.get(url)
    time.sleep(DETAIL_SLEEP)
    return driver.page_source

def card_select_all(html: str):
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

def collect_tags(container) -> str:
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
            if "性描写あり" in text: return True
    return "性描写あり" in detail_soup.get_text(" ", strip=True)

def detail_fallbacks_if_missing(stars, chars, detail_soup):
    """一覧で欠損した場合、詳細ページ側でも拾えるものを拾う（強化用・必要最低限）"""
    # 星は詳細ページで露出が弱い場合もあるので、そのまま None を許容
    # 文字数は詳細ページに合計が出ることが多い
    if chars is None:
        for sel in ["span.charCount", "[data-testid='char-count']"]:
            el = detail_soup.select_one(sel)
            if el and el.get_text(strip=True):
                chars = parse_number(el.get_text(strip=True))
                break
    return stars, chars

def main():
    driver = get_driver()
    all_rows = []
    mismatch_rows = []

    for page in range(1, MAX_PAGES + 1):
        page_url = build_page_url(BASE_URL, page)
        html = fetch_html(driver, page_url)
        (ARTIFACT_DIR / f"page_{page}.html").write_text(html, encoding="utf-8")
        driver.save_screenshot(str(ARTIFACT_DIR / f"page_{page}.png"))
        soup, cards = card_select_all(html)

        if not cards:
            print(f"page {page}: no cards -> stop")
            break

        print(f"page {page}: cards={len(cards)}")

        # 各カード処理
        low_star_tail = 0
        for i, card in enumerate(cards, 1):
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
            # 目次（/episodes 存在しない場合はトップの <time> 群で代用）
            episodes_url = work_url + "/episodes"
            try:
                toc_html = fetch_html(driver, episodes_url)
            except Exception:
                toc_html = detail_html
            first_date = pick_first_date_from_html(toc_html)

            # 欠損フォールバック
            stars, chars = detail_fallbacks_if_missing(stars, chars, detail_soup)
            tags_detail = collect_tags(detail_soup)

            # 厳密判定（欠損は False）
            meets = (
                isinstance(stars, int) and stars >= MIN_STARS and
                isinstance(chars, int) and chars >= MIN_CHARS and
                bool(notice) and bool(has_harem) and
                (first_date is not None and first_date >= MIN_DATE)
            )

            all_rows.append({
                "title": title,
                "url": work_url,
                "stars": stars if stars is not None else "",
                "total_chars": chars if chars is not None else "",
                "notice_sexual": notice,
                "first_episode_date": first_date.strftime("%Y-%m-%d") if first_date else "",
                "tags": tags_list,
                "tags_detail": tags_detail,
                "meets_conditions": meets
            })

            # 星の低い領域が続いたら早期打ち切りのヒント（任意）
            if isinstance(stars, int) and stars < MIN_STARS:
                low_star_tail += 1
            else:
                low_star_tail = 0

        # 末尾に★<3000 が大量に続くページが現れたら打ち切る（任意）
        if low_star_tail >= max(10, len(cards)//2):
            print(f"page {page}: low-star tail detected -> early stop")
            break

        time.sleep(PAGE_SLEEP)

    driver.quit()

    # CSV（必ず出力）
headers = ["title","url","stars","total_chars","notice_sexual","first_episode_date","tags","tags_detail","meets_conditions"]

# 👇 追加ここから
OUT_CSV = Path.cwd() / OUT_CSV
ARTIFACT_DIR = Path.cwd() / ARTIFACT_DIR
# 👆 追加ここまで

with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=headers)
    w.writeheader()
    w.writerows(all_rows)

# “疑いあり”だけまとめ（欠損/タグ欠落/日付なし等）
for r in all_rows:
    if (r["stars"] == "" or r["total_chars"] == "" or not r["notice_sexual"] or "ハーレム" not in str(r["tags"]) or r["first_episode_date"] == ""):
        mismatch_rows.append(r)
if mismatch_rows:
    with open(ARTIFACT_DIR / "mismatch.csv", "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(mismatch_rows)

# サマリ
(ARTIFACT_DIR / "scrape_summary.txt").write_text(
    f"pages_processed={page}\nrows={len(all_rows)}\n", encoding="utf-8"
)
print(f"Saved: {OUT_CSV} rows={len(all_rows)} mismatches={len(mismatch_rows)}")

