# kakuyomu_harem_scraper.py
# -*- coding: utf-8 -*-
import os, time, re, csv
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

BASE_URL = "https://kakuyomu.jp/tags/ハーレム?sort=popular"
OUTPUT_CSV = "kakuyomu_harem_filtered.csv"
MIN_STARS = 3000
MIN_CHARS = 50000
MIN_DATE = datetime(2025, 4, 1)

def get_driver():
    opts = Options()
    # CIで安定するフラグ群
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1366,768")
    ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    opts.add_argument(f"--user-agent={ua}")
    # 必要時のみ CHROME_PATH を使う
    chrome_path = os.environ.get("CHROME_PATH") or os.environ.get("CHROME_BIN")
    if chrome_path:
        opts.binary_location = chrome_path
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
    time.sleep(1.5)
    return driver.page_source

def main():
    driver = get_driver()
    print("Open:", BASE_URL)
    driver.get(BASE_URL)

    # ある程度スクロール（タグ人気順の無限ロードを想定）
    last = 0
    for _ in range(30):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.2)
        h = driver.execute_script("return document.body.scrollHeight;")
        if h == last:
            break
        last = h

    soup = BeautifulSoup(driver.page_source, "lxml")
    cards = soup.select("div.widget-workCard")
    print("cards:", len(cards))

    rows = []
    for i, card in enumerate(cards, 1):
        a = card.select_one("h3.widget-workCard-title a")
        if not a: 
            continue
        title = a.get_text(strip=True)
        work_url = "https://kakuyomu.jp" + a["href"]

        stars = parse_number((card.select_one("span.widget-workCard-reviewCount") or {}).get_text() if card.select_one("span.widget-workCard-reviewCount") else "")
        tags_text = " ".join(t.get_text(strip=True) for t in card.select(".widget-workCard-tag"))
        has_harem = "ハーレム" in tags_text
        total_chars = parse_number((card.select_one("span.widget-workCard-charCount") or {}).get_text() if card.select_one("span.widget-workCard-charCount") else "")

        # 作品トップで注意書きと目次の初回日を確定
        html = get_html(driver, work_url)
        soup_detail = BeautifulSoup(html, "lxml")
        notice = ("性描写あり" in soup_detail.get_text(" ", strip=True))

        # 目次URL（作品トップと同一ページ内にある場合もあるが、念のため /episodes にも対応）
        toc_url = work_url + "/episodes" if not work_url.endswith("/episodes") else work_url
        toc_html = get_html(driver, toc_url)
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

        # ★が3000未満のカードが増えてきたら早期終了（任意）
        # if stars < MIN_STARS: break

    driver.quit()

    if rows:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            import csv
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print("Saved:", OUTPUT_CSV)
    else:
        print("No rows scraped.")

if __name__ == "__main__":
    main()

