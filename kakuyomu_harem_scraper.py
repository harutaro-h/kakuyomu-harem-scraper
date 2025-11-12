# kakuyomu_harem_scraper.py
# -*- coding: utf-8 -*-
import os, time, re, csv, sys
from datetime import datetime
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

BASE_URL = "https://kakuyomu.jp/tags/ハーレム?sort=popular"
OUT_CSV = "kakuyomu_harem_filtered.csv"
OUT_DIR = Path("artifacts")               # 証跡保存用
OUT_DIR.mkdir(exist_ok=True)
MIN_STARS = 3000
MIN_CHARS = 50000
MIN_DATE = datetime(2025, 4, 1)

def get_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,768")
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"--user-agent={ua}")
    # setup-chrome 用のフォールバック
    if os.environ.get("CHROME_PATH"):
        opts.binary_location = os.environ["CHROME_PATH"]
    return webdriver.Chrome(options=opts)

def parse_number(s):
    m = re.sub(r"[^\d]", "", s or "")
    return int(m) if m.isdigit() else 0

def extract_first_date_from_toc(html: str):
    soup = BeautifulSoup(html, "lxml")
    times = [t.get("datetime", "")[:10] for t in soup.select("time[datetime]")]
    dates = []
    for d in times:
        try:
            dates.append(datetime.fromisoformat(d))
        except Exception:
            pass
    return min(dates) if dates else None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_html(driver, url):
    driver.get(url)
    time.sleep(1.2)
    return driver.page_source

def safe_text(el):
    return el.get_text(strip=True) if el else ""

def find_cards_html(html):
    """複数パターンでカードを抽出"""
    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.widget-workCard")
    if not cards:
        # 代替候補（将来のクラス変更に備える）
        cards = [a.parent for a in soup.select("h3 a[href^='/works/']")]
    return cards, soup

def main():
    driver = get_driver()
    print("Open:", BASE_URL)
    driver.get(BASE_URL)

    # 初期ロード待ち & 証跡
    WebDriverWait(driver, 15).until(lambda d: d.execute_script("return document.readyState") == "complete")
    driver.save_screenshot(str(OUT_DIR / "1_initial.png"))
    (OUT_DIR / "1_initial.html").write_text(driver.page_source, encoding="utf-8")

    # 無限スクロール（一定回数 or 高さが伸びなくなるまで）
    last_h, same_cnt = 0, 0
    for _ in range(40):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.0)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last_h:
            same_cnt += 1
            if same_cnt >= 3:
                break
        else:
            same_cnt = 0
            last_h = h

    driver.save_screenshot(str(OUT_DIR / "2_after_scroll.png"))
    (OUT_DIR / "2_after_scroll.html").write_text(driver.page_source, encoding="utf-8")

    cards, soup = find_cards_html(driver.page_source)
    print(f"cards detected: {len(cards)}")
    rows = []

    for i, card in enumerate(cards, 1):
        a = card.select_one("h3 a[href^='/works/']") or card.select_one("a[href^='/works/']")
        if not a:
            continue
        title = safe_text(a)
        work_url = "https://kakuyomu.jp" + a["href"]

        stars = parse_number(safe_text(card.select_one("span.widget-workCard-reviewCount")))
        total_chars = parse_number(safe_text(card.select_one("span.widget-workCard-charCount")))
        tags_text = " ".join(t.get_text(strip=True) for t in card.select(".widget-workCard-tag, .tag"))
        has_harem = "ハーレム" in tags_text

        # 作品トップ
        html = get_html(driver, work_url)
        soup_detail = BeautifulSoup(html, "lxml")
        notice = ("性描写あり" in soup_detail.get_text(" ", strip=True))

        # 目次（/episodes がなくてもトップにtimeが並ぶことがある）
        toc_url = work_url + "/episodes"
        try:
            toc_html = get_html(driver, toc_url)
        except Exception:
            toc_html = html
        first_date = extract_first_date_from_toc(toc_html)

        meets = all([
            has_harem,
            notice,
            stars >= MIN_STARS,
            total_chars >= MIN_CHARS,
            (first_date and first_date >= MIN_DATE)
        ])

        rows.append({
            "title": title,
            "url": work_url,
            "stars": stars,
            "total_chars": total_chars,
            "notice_sexual": notice,
            "first_episode_date": first_date.strftime("%Y-%m-%d") if first_date else "",
            "tags": tags_text,
            "meets_conditions": meets
        })
        print(f"[{i}] {title}  ★{stars}  {total_chars}字  first={first_date}  ok={meets}")

    driver.quit()

    # 必ずCSVを出力（0件でも）
    headers = ["title","url","stars","total_chars","notice_sexual","first_episode_date","tags","meets_conditions"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    # 走査メモ
    with open(OUT_DIR/"scrape_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"cards_detected={len(cards)}\nrows={len(rows)}\n")
        f.write(f"csv={OUT_CSV}\n")

    print(f"Saved CSV: {OUT_CSV}  (rows={len(rows)})")

if __name__ == "__main__":
    main()
