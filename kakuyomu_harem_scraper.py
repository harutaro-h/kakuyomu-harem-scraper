import requests
from bs4 import BeautifulSoup
import time
import csv
import re
import sys
from datetime import datetime
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ==========================================
# 設定・定数
# ==========================================

# 検索条件
TARGET_TAG_SEARCH = "ハーレム"
TARGET_START_DATE = datetime(2025, 4, 1) # この日付以降に開始
MIN_STARS = 3000
MIN_CHARS = 50000

# URL関連
BASE_URL = "https://kakuyomu.jp"
SEARCH_URL = f"{BASE_URL}/tags/{TARGET_TAG_SEARCH}"

# 負荷対策設定
SLEEP_TIME = 2.0  # 1リクエストあたりの待機時間(秒)
MAX_LISTING_PAGES = 80  # 検索結果の最大巡回ページ数
MAX_EPISODE_PAGES = 50  # 1作品あたりのエピソード最大巡回ページ数
TIMEOUT = 15

# 出力ファイル名
OUTPUT_FILENAME = "kakuyomu_harem_filtered.csv"

# User-Agent (Github Actions上で動くことを明示しつつ、社内ツールであることを記載)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 (KADOKAWA_Internal_Tool_Test/GitHubActions)"
}

# ==========================================
# セッションの作成 (リトライ機能付き)
# ==========================================
def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

# ==========================================
# ヘルパー関数
# ==========================================

def fetch_soup(session, url):
    """
    URLを取得してBeautifulSoupオブジェクトを返す。
    必ずsleepを挟む。
    """
    print(f"Fetch: {url}")
    try:
        time.sleep(SLEEP_TIME)
        response = session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        return BeautifulSoup(response.content, "lxml")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None

def parse_int(text):
    """文字列からカンマを除去して整数にする"""
    if not text:
        return 0
    try:
        # "12,345文字" -> "12345"
        num_str = re.sub(r'[^\d]', '', text)
        return int(num_str) if num_str else 0
    except ValueError:
        return 0

def get_iso_date(text):
    """YYYY-MM-DD形式の文字列をdatetimeオブジェクトに変換"""
    try:
        # datetime="2025-11-19T18:00:00Z" のような形式を想定
        return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None

# ==========================================
# スクレイピングロジック
# ==========================================

def process_work(session, work_url, listing_stars, listing_chars):
    """
    作品詳細ページを処理し、条件を満たすか判定する。
    戻り値: 条件を満たせば辞書データ、満たさなければNone
    """
    # 1. 作品詳細ページの取得
    soup = fetch_soup(session, work_url)
    if not soup:
        return None

    # タイトル取得
    title_tag = soup.select_one('h1#workTitle, h1') 
    title = title_tag.get_text(strip=True) if title_tag else "No Title"

    # 星数取得（詳細ページで再確認）
    stars = listing_stars # デフォルトは一覧の値
    points_elm = soup.select_one('#workPoints') or soup.select_one('.js-stars-count')
    if points_elm:
        stars = parse_int(points_elm.get_text())

    # 文字数取得
    total_chars = listing_chars
    chars_elm = soup.select_one('#workTotalCharacterCount')
    if chars_elm:
        total_chars = parse_int(chars_elm.get_text())

    # 基本フィルタ（星数・文字数）
    if stars < MIN_STARS or total_chars < MIN_CHARS:
        print(f"  -> Skip: Stars({stars}) or Chars({total_chars}) below threshold.")
        return None

    # タグ取得
    tags = []
    tag_links = soup.select('[itemprop="keywords"] a, #tagList a, .TagList-module__tag___ a')
    for link in tag_links:
        tags.append(link.get_text(strip=True))
    
    if TARGET_TAG_SEARCH not in tags:
        print(f"  -> Skip: '{TARGET_TAG_SEARCH}' tag missing.")
        return None

    # 注意書き「性描写あり」チェック
    notice_sexual = False
    notice_elms = soup.select('#workHeader-inner, .work-header-notice, .Notice-module__area, [aria-label="性描写あり"]')
    header_text = " ".join([e.get_text() for e in notice_elms])
    aria_labels = [e.get('aria-label', '') for e in soup.select('[aria-label]')]
    
    if "性描写あり" in header_text or "性描写あり" in aria_labels:
        notice_sexual = True

    if not notice_sexual:
        print("  -> Skip: No sexual content notice.")
        return None

    # 2. 初回エピソード投稿日の取得と判定
    first_date = get_first_episode_date(session, work_url)
    
    if not first_date:
        print("  -> Skip: Could not determine start date.")
        return None
    
    if first_date < TARGET_START_DATE:
        print(f"  -> Skip: Started on {first_date.date()} (Before {TARGET_START_DATE.date()})")
        return None

    # 全ての条件をクリア
    return {
        'title': title,
        'url': work_url,
        'stars': stars,
        'total_chars': total_chars,
        'first_episode_date': first_date.strftime('%Y-%m-%d'),
        'tags': " ".join(tags),
        'notice_sexual': "True"
    }

def get_first_episode_date(session, work_url):
    """
    エピソード一覧を巡回して、最も古い投稿日時を取得する。
    ターゲット日付より古いものが見つかった時点で早期終了する。
    """
    episodes_url = f"{work_url}/episodes"
    current_url = episodes_url
    min_date = None
    page_count = 0

    while current_url and page_count < MAX_EPISODE_PAGES:
        page_count += 1
        soup = fetch_soup(session, current_url)
        if not soup:
            break

        # datetime属性を持つtimeタグを探す
        time_tags = soup.select('li.widget-episode .widget-episode-date time, .EpisodeList-module__date___ time')
        
        if not time_tags:
            break

        found_dates = []
        for tm in time_tags:
            dt_str = tm.get('datetime')
            if dt_str:
                dt = get_iso_date(dt_str)
                if dt:
                    found_dates.append(dt)

        if found_dates:
            current_page_min = min(found_dates)
            
            # もしこのページの中に、ターゲット日付より前の日付があれば対象外確定
            if current_page_min < TARGET_START_DATE:
                return current_page_min 

            if min_date is None or current_page_min < min_date:
                min_date = current_page_min

        # 次のページへ
        next_link = soup.select_one('a[rel="next"], .pager-next a')
        if next_link:
            next_href = next_link.get('href')
            current_url = urljoin(work_url, next_href)
        else:
            current_url = None

    return min_date

# ==========================================
# メイン処理
# ==========================================

def main():
    session = create_session()
    results = []
    total_scanned = 0
    
    print(f"Start scraping: {SEARCH_URL}")
    print(f"Conditions: >= {TARGET_START_DATE.date()}, Stars >= {MIN_STARS}, Chars >= {MIN_CHARS}, Tag='ハーレム', Notice='性描写あり'")

    for page in range(1, MAX_LISTING_PAGES + 1):
        target_url = f"{SEARCH_URL}?sort=popular&page={page}"
        
        soup = fetch_soup(session, target_url)
        if not soup:
            print("Failed to retrieve listing page. Stopping.")
            break

        work_cards = soup.select('.widget-workCard')
        if not work_cards:
            work_cards = soup.select('[class*="WorkCard-module__card"]')

        if not work_cards:
            print(f"No works found on page {page}. Ending search.")
            break

        print(f"Processing page {page} ({len(work_cards)} works)...")

        for card in work_cards:
            total_scanned += 1
            
            link_tag = card.select_one('a[href^="/works/"]')
            if not link_tag:
                continue
            
            relative_link = link_tag.get('href')
            if not re.match(r'^/works/\d+$', relative_link):
                title_link = card.select_one('.widget-workCard-title a')
                if title_link:
                    relative_link = title_link.get('href')
                else:
                    continue

            work_url = urljoin(BASE_URL, relative_link)

            # 一覧での簡易フィルタリング
            stars_text = "0"
            star_elm = card.select_one('.widget-workCard-reviewPoints, [class*="Star-module__count"]')
            if star_elm:
                stars_text = star_elm.get_text()
            listing_stars = parse_int(stars_text)

            chars_text = "0"
            char_elm = card.select_one('.widget-workCard-characterCount, [class*="Meta-module__characterCount"]')
            if char_elm:
                chars_text = char_elm.get_text()
            listing_chars = parse_int(chars_text)

            if listing_stars < (MIN_STARS * 0.8): 
                continue
            if listing_chars < (MIN_CHARS * 0.8):
                continue

            print(f"Checking: {work_url} (ListStars: {listing_stars})")
            
            try:
                work_data = process_work(session, work_url, listing_stars, listing_chars)
                if work_data:
                    print(f"  [MATCH] Found: {work_data['title']}")
                    results.append(work_data)
            except Exception as e:
                print(f"Error processing work {work_url}: {e}")
                continue

    if results:
        print(f"\nWriting {len(results)} works to {OUTPUT_FILENAME}...")
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8-sig', newline='') as f:
            fieldnames = ['title', 'url', 'stars', 'total_chars', 'first_episode_date', 'tags', 'notice_sexual']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print("Done.")
    else:
        print("\nNo works matched the criteria.")
        # ファイルが作られないとArtifactアップロードでエラーになる可能性があるため空ファイルを作成
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8-sig', newline='') as f:
            pass
    
    print(f"Total scanned (listing): {total_scanned}")

if __name__ == "__main__":
    main()
