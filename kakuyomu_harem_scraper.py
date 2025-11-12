# kakuyomu_harem_scraper.py
# -*- coding: utf-8 -*-
"""
Kakuyomu「ハーレム」タグを人気順で無限スクロール巡回し、
各作品の ★数 / 文字数 / 注意書き(性描写あり) / 初回公開日 / タグ を取得。
下記条件で AND フィルタし、CSV を必ず出力（0件でもヘッダ付き）。
- 初回公開日 >= 2025-04-01
- ★ >= 3000
- 性描写あり
- 文字数 >= 50,000
- タグに「ハーレム」を含む（複合タグ内も可）

加えて、デバッグ用の証跡（HTML・スクリーンショット・不整合一覧）を artifacts/ に保存。
"""

import os
import re
import csv
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List

from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ===== 設定 =====
BASE_URL = "https://kakuyomu.jp/tags/ハーレム?sort=popular"  # 人気順
OUT_CSV = "kakuyomu_harem_filtered.csv"
MISMATCH_CSV = "artifacts/mismatch.csv"
ARTIFACT_DIR = Path("artifacts")
ARTIFACT_DIR.mkdir(exist_ok=True)

MIN_STARS = 3000
MIN_CHARS = 50000
MIN_DATE = datetime(2025, 4, 1)
MAX_SCROLLS = 60                 # 無限スクロール上限
SCROLL_PAUSE = 1.0               # スクロール間待機
EXTRA_SETTLE_WAIT = 2.0          # 終端での余韻待機

# ===============


def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,768")
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"--user-agent={ua}")
    # GitHub Actions の setup-chrome 用フォールバック
    chrome_path = os.environ.get("CHROME_PATH") or os.environ.get("CHROME_BIN")
    if chrome_path:
        opts.binary_location = chrome_path
    return webdriver.Chrome(options=opts)


def parse_number(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.sub(r"[^\d]", "", s)
    return int(m) if m.isdigit() else None


def extract_first_date_from_toc(html: str) -> Optional[datetime]:
    soup = BeautifulSoup(html, "lxml")
    times = [t.get("datetime", "")[:10] for t in soup.select("time[datetime]")]
    dates: List[datetime] = []
    for d in times:
        try:
            dates.append(datetime.fromisoformat(d))
        except Exception:
            pass
    return min(dates) if dates else None


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_html(driver, url: str) -> str:
    driver.get(url)
    # 描画完了待ち
    WebDriverWait(driver, 15).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )
    time.sleep(1.0)
    return driver.page_source


def save_artifact(driver, name: str):
    # 画面とHTMLを保存
    driver.save_screenshot(str(ARTIFACT_DIR / f"{name}.png"))
    (ARTIFACT_DIR / f"{name}.html").write_text(driver.page_source, encoding="utf-8")


def find_cards_html(html: str):
    """複数パターンで一覧カードを拾う（将来のDOM変更に備える）"""
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.widget-workCard")
    if not cards:
        # 代替（タイトルリンクからカード要素を逆引き）
        cards = [a.parent for a in soup.select("h3 a[href^='/works/']")]
    return cards, soup


def pick_star(card) -> Optional[int]:
    for sel in [
        "span.widget-workCard-reviewCount",
        "span.reviewCount",
        "[data-testid='review-count']",
    ]:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            return parse_number(el.get_text(strip=True))
    return None


def pick_chars(card) -> Optional[int]:
    for sel in [
        "span.widget-workCard-charCount",
        "span.charCount",
        "[data-testid='char-count']",
    ]:
        el = card.select_one(sel)
        if el and el.get_text(strip=True):
            return parse_number(el.get_text(strip=True))
    return None


def pick_tags(container) -> str:
    tags_texts = []
    for sel in [".widget-workCard-tag", ".tag", "[data-testid='tag']"]:
        for t in container.select(sel):
            s = t.get_text(strip=True)
            if s:
                tags_texts.append(s)
    return " ".join(tags_texts)


def detect_sexual_notice(soup_detail: BeautifulSoup) -> bool:
    # まずは注意書きっぽい領域を優先
    for sel in [".workHeader-notice", ".notice", "[data-testid='notice']"]:
        bloc = soup_detail.select(sel)
        if bloc:
            text = " ".join(el.get_text(" ", strip=True) for el in bloc)
            if "性描写あり" in text:
                return True
    # フォールバック：全文
    return "性描写あり" in soup_detail.get_text(" ", strip=True)


def main():
    driver = get_driver()
    print("Open:", BASE_URL)
    driver.get(BASE_URL)

    # 初期ロード待ち & 証跡
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
    )
    save_artifact(driver, "1_initial")

    # 一覧のカードが出るまで待つ
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.widget-workCard, h3 a[href^='/works/']")
            )
        )
    except Exception:
        print("WARN: 初期カード検出に失敗")
    # 無限スクロール
    last_h = 0
    same_cnt = 0
    for _ in range(MAX_SCROLLS):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            same_cnt += 1
            if same_cnt >= 3:
                break
        else:
            same_cnt = 0
            last_h = h

    time.sleep(EXTRA_SETTLE_WAIT)  # 終端の余韻待ち
    save_artifact(driver, "2_after_scroll")

    cards, soup = find_cards_html(driver.page_source)
    print(f"cards detected: {len(cards)}")

    rows = []
    mismatches = []

    for i, card in enumerate(cards, 1):
        a = card.select_one("h3 a[href^='/works/']") or card.select_one("a[href^='/works/']")
        if not a:
            continue
        title = a.get_text(strip=True)
        work_url = "https://kakuyomu.jp" + a["href"]

        stars = pick_star(card)
        total_chars = pick_chars(card)
        tags_text = pick_tags(card)
        has_harem = "ハーレム" in (tags_text or "")

        # 作品トップ → 注意書き
        html_detail = get_html(driver, work_url)
        soup_detail = BeautifulSoup(html_detail, "lxml")
        notice = detect_sexual_notice(soup_detail)

        # 目次（/episodes が無い場合はトップの <time> 群で代用）
        toc_url = work_url + "/episodes"
        try:
            html_toc = get_html(driver, toc_url)
        except Exception:
            html_toc = html_detail
        first_date = extract_first_date_from_toc(html_toc)

        # 厳密判定（欠損は即 False）
        meets = (
            isinstance(stars, int) and stars >= MIN_STARS and
            isinstance(total_chars, int) and total_chars >= MIN_CHARS and
            bool(notice) and bool(has_harem) and
            (first_date is not None and first_date >= MIN_DATE)
        )

        # 作品トップ側のタグでも補助確認（安全網）
        # ※ false positive を避けるため meets 計算は上の has_harem のみで実施
        tags_detail = pick_tags(soup_detail)
        # ログ
        print(f"[{i}] {title} ★{stars}  {total_chars}字  first={first_date}  sex={notice}  tagOK={has_harem}  meets={meets}")

        rows.append({
            "title": title,
            "url": work_url,
            "stars": stars if stars is not None else "",
            "total_chars": total_chars if total_chars is not None else "",
            "notice_sexual": notice,
            "first_episode_date": first_date.strftime("%Y-%m-%d") if first_date else "",
            "tags": tags_text,
            "tags_detail": tags_detail,
            "meets_conditions": meets,
            "raw_review_selector_hit": stars is not None,
            "raw_chars_selector_hit": total_chars is not None
        })

        # 再評価ロジックでの期待値とズレたら記録（星/文字/タグ/日付/注意書き）
        expected = meets
        # ここでは expected==meets なので、将来 他経路 併設時の差分検出に備えて行だけ残す
        if stars is None or total_chars is None or not has_harem or not notice or (first_date is None):
            mismatches.append({
                "title": title, "url": work_url,
                "stars": stars, "total_chars": total_chars, "notice_sexual": notice,
                "first_episode_date": first_date.strftime("%Y-%m-%d") if first_date else "",
                "tags": tags_text, "tags_detail": tags_detail
            })

    # まとめ出力（0件でも必ずCSVを作る）
    headers = ["title","url","stars","total_chars","notice_sexual","first_episode_date","tags","tags_detail","meets_conditions","raw_review_selector_hit","raw_chars_selector_hit"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # 不整合っぽい行を別CSVに
    if mismatches:
        with open(MISMATCH_CSV, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(mismatches[0].keys()))
            w.writeheader()
            for r in mismatches:
                w.writerow(r)

    # 収集サマリ
    (ARTIFACT_DIR / "scrape_summary.txt").write_text(
        f"cards_detected={len(cards)}\nrows={len(rows)}\ncsv={OUT_CSV}\n",
        encoding="utf-8"
    )

    print(f"\nSaved CSV: {OUT_CSV} (rows={len(rows)})")
    if mismatches:
        print(f"Mismatch rows: {len(mismatches)} -> {MISMATCH_CSV}")

    driver.quit()


if __name__ == "__main__":
    main()
