# kakuyomu_harem_scraper.py
# -*- coding: utf-8 -*-
"""
Kakuyomu「ハーレム」タグ作品一覧を評価順で走査し、
条件（初回公開日 >= 2025/04/01、★ >=3000、性描写あり、文字数 >=50,000）で絞り込み、
結果をCSVで保存する。
"""

import time
import re
import csv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# ---------- 設定 ----------
BASE_URL = "https://kakuyomu.jp/tags/ハーレム?sort=popular"
OUTPUT_CSV = "kakuyomu_harem_filtered.csv"
MIN_STARS = 3000
MIN_CHARS = 50000
MIN_DATE = datetime(2025, 4, 1)
# --------------------------

def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    return driver

def scroll_to_bottom(driver, pause_time=2, max_scrolls=50):
    """無限スクロールを★3000未満になるまで or max_scrolls回"""
    last_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(pause_time)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def parse_number(text):
    return int(re.sub(r"[^\d]", "", text))

def get_first_episode_date(soup):
    # 目次内から「YYYY年MM月DD日 公開」を取得
    date_tag = soup.find("time", {"datetime": True})
    if date_tag and date_tag.get("datetime"):
        return datetime.fromisoformat(date_tag["datetime"].split("T")[0])
    return None

def main():
    driver = get_driver()
    print("アクセス中:", BASE_URL)
    driver.get(BASE_URL)
    scroll_to_bottom(driver)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    cards = soup.select("div.widget-workCard")

    results = []
    print(f"{len(cards)}件の作品カードを検出しました。")

    for card in cards:
        title_tag = card.select_one("h3.widget-workCard-title a")
        if not title_tag:
            continue
        title = title_tag.text.strip()
        url = "https://kakuyomu.jp" + title_tag["href"]
        stars_tag = card.select_one("span.widget-workCard-reviewCount")
        stars = parse_number(stars_tag.text) if stars_tag else 0
        tags_text = " ".join(t.text for t in card.select(".widget-workCard-tag"))
        has_harem_tag = "ハーレム" in tags_text
        char_tag = card.select_one("span.widget-workCard-charCount")
        total_chars = parse_number(char_tag.text) if char_tag else 0

        # 各作品ページへ遷移して詳細確認
        driver.get(url)
        time.sleep(1.5)
        soup_detail = BeautifulSoup(driver.page_source, "html.parser")

        notice = "性描写あり" in soup_detail.text
        date = get_first_episode_date(soup_detail)

        meets_all = (
            stars >= MIN_STARS and
            notice and
            has_harem_tag and
            total_chars >= MIN_CHARS and
            date and date >= MIN_DATE
        )

        results.append({
            "title": title,
            "url": url,
            "stars": stars,
            "total_chars": total_chars,
            "notice_sexual": notice,
            "first_episode_date": date.strftime("%Y-%m-%d") if date else "",
            "tags": tags_text,
            "meets_conditions": meets_all
        })
        print(f"→ {title} [{stars}★ / {total_chars}字 / {date}]")

    driver.quit()

    # CSV保存
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ CSV出力完了: {OUTPUT_CSV}")
    filtered = [r for r in results if r["meets_conditions"]]
    print(f"条件一致作品: {len(filtered)}件")

if __name__ == "__main__":
    main()
