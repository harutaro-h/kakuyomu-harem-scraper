import requests
from bs4 import BeautifulSoup
import time
import csv
import re
import sys
from datetime import datetime
from urllib.parse import urljoin, quote
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
# 重要: 日本語タグをURLエンコードする
SEARCH_URL = f"{BASE_URL}/tags/{quote(TARGET_TAG_SEARCH)}"

# 負荷対策設定
SLEEP_TIME = 2.0
MAX_LISTING_PAGES = 80
MAX_EPISODE_PAGES = 50
TIMEOUT = 15

OUTPUT_FILENAME = "kakuyomu_harem_filtered.csv"

# User-Agent
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 (KADOKAWA_Internal_Tool_Test/GitHubActions)"
}

# ==========================================
# セッション作成
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
    if not text: return 0
    try:
        num_str = re.sub(r'[^\d]', '', text)
        return int(num_str) if num_str else 0
    except ValueError:
        return 0

def get_iso_date(text):
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None

# ==========================================
# スクレイピングロジック
# ==========================================

def process_work(session, work_url, listing_stars, listing_chars):
    soup = fetch_soup(session, work_url)
    if not soup: return None

    # タイトル
    title_tag = soup.select_one('h1#workTitle, h1') 
    title = title_tag.get_text(strip=True) if title_tag else "No Title"

    # 星数（詳細ページ優先）
    stars = listing_stars
    points_elm = soup.select_one('#workPoints, .js-stars-count')
    if points_elm:
        stars = parse_int(points_elm.get_text())

    # 文字数
    total_chars = listing_chars
    chars_elm = soup.select_one('#workTotalCharacterCount')
    if chars_elm:
        total_chars = parse_int(chars_elm.get_text())

    # フィルタ
    if stars < MIN_STARS or total_chars < MIN_CHARS:
        # print(f"  -> Skip: Stars({stars}) or Chars({total_chars}) below threshold.") # ログ削減
        return None

    # タグ
    tags = []
    tag_links = soup.select('[itemprop="keywords"] a, #tagList a, .TagList-module__tag___ a')
    for link in tag_links:
        tags.append(link.get_text(strip=True))
    
    if TARGET_TAG_SEARCH not in tags:
        # print(f"  -> Skip: Tag '{TARGET_TAG_SEARCH}' missing in details.")
        return None

    # 性描写ありチェック
    notice_sexual = False
    # 複数のセレクタで注意書きを探す
    notice_elms = soup.select('#workHeader-inner, .work-header-notice, .Notice-module__area, [aria-label="性描写あり"]')
    full_text = " ".join([e.get_text() for e in notice_elms])
    aria_labels = [e.get('aria-label', '') for e in soup.select('[aria-label]')]
    
    if "性描写あり" in full_text or "性描写あり" in aria_labels:
        notice_sexual = True

    if not notice_sexual:
        # print("  -> Skip: No sexual content notice.")
        return None

    # 初回エピソード日時
    first_date = get_first_episode_date(session, work_url)
    if not first_date: return None
    
    if first_date < TARGET_START_DATE:
        return None

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
    episodes_url = f"{work_url}/episodes"
    current_url = episodes_url
    min_date = None
    page_count = 0

    while current_url and page_count < MAX_EPISODE_PAGES:
        page_count += 1
        soup = fetch_soup(session, current_url)
        if not soup: break

        time_tags = soup.select('li.widget-episode .widget-episode-date time, .EpisodeList-module__date___ time')
        if not time_tags: break

        found_dates = []
        for tm in time_tags:
            dt_str = tm.get('datetime')
            if dt_str:
                dt = get_iso_date(dt_str)
                if dt: found_dates.append(dt)

        if found_dates:
            current_page_min = min(found_dates)
            if current_page_min < TARGET_START_DATE:
                return current_page_min 
            if min_date is None or current_page_min < min_date:
                min_date = current_page_min

        next_link = soup.select_one('a[rel="next"], .pager-next a')
        if next_link:
            current_url = urljoin(work_url, next_link.get('href'))
        else:
            current_url = None

    return min_date

# ==========================================
# メイン
# ==========================================
def main():
    session = create_session()
    results = []
    total_scanned = 0
    
    print(f"Start scraping: {SEARCH_URL}")
    
    for page in range(1, MAX_LISTING_PAGES + 1):
        target_url = f"{SEARCH_URL}?sort=popular&page={page}"
        soup = fetch_soup(session, target_url)
        if not soup: break

        # デバッグ: ページタイトルを表示して、正しくアクセスできているか確認
        page_title = soup.title.get_text(strip=True) if soup.title else "No Title"
        print(f"Page {page} Title: {page_title}")

        # セレクタを強化: 最新のPartialWorkCardや、汎用的なWidgetにも対応
        work_cards = soup.select('.widget-workCard')
        if not work_cards:
            work_cards = soup.select('[class*="WorkCard-module__card"]')
        if not work_cards:
            work_cards = soup.select('[class*="PartialWorkCard-module__card"]')
        
        if not work_cards:
            print(f"No works found on page {page}. Dump: Title={page_title}")
            # もしかするとメインカラム取得失敗かもしれないので、メインエリアのリンクを直接探すフォールバック
            main_col = soup.select_one('.widget-mainColumn, #main, main')
            if main_col:
                # メインエリア内の /works/ リンクを持つ要素を親ごと取得する簡易ロジック
                links = main_col.select('a[href^="/works/"]')
                if links:
                    print(f"Fallback: Found {len(links)} potential links in main column.")
                    # ここでは処理が複雑になるため、次のループでカードが見つからなかったと判断して終了するが、
                    # 通常は上のセレクタでヒットするはず。
            break

        print(f"Processing page {page} ({len(work_cards)} works found)...")

        for card in work_cards:
            total_scanned += 1
            
            # リンク取得
            link_tag = card.select_one('a[href^="/works/"]')
            if not link_tag: continue
            
            relative_link = link_tag.get('href')
            # /episodes や /reviews を除外
            if not re.match(r'^/works/\d+$', relative_link):
                # タイトルクラス内のリンクを優先で探す
                title_link = card.select_one('[class*="title"] a') or card.select_one('h3 a')
                if title_link:
                    relative_link = title_link.get('href')
                else:
                    continue
            
            # 最終確認
            if not re.match(r'^/works/\d+$', relative_link):
                continue

            work_url = urljoin(BASE_URL, relative_link)

            # 一覧情報の取得 (セレクタを複数パターン用意)
            stars_text = "0"
            # ReviewPoints, Star-count, etc.
            star_elm = card.select_one('.widget-workCard-reviewPoints, [class*="Star-module__count"], [class*="ReviewPoints-module__points"]')
            if star_elm: stars_text = star_elm.get_text()
            listing_stars = parse_int(stars_text)

            chars_text = "0"
            char_elm = card.select_one('.widget-workCard-characterCount, [class*="Meta-module__characterCount"]')
            if char_elm: chars_text = char_elm.get_text()
            listing_chars = parse_int(chars_text)

            # 足切り
            if listing_stars < (MIN_STARS * 0.8): continue
            if listing_chars < (MIN_CHARS * 0.8): continue

            print(f"Checking: {work_url} (ListStars: {listing_stars})")
            
            try:
                work_data = process_work(session, work_url, listing_stars, listing_chars)
                if work_data:
                    print(f"  [MATCH] Found: {work_data['title']}")
                    results.append(work_data)
            except Exception as e:
                print(f"Error processing {work_url}: {e}")
                continue

    if results:
        print(f"\nWriting {len(results)} works to {OUTPUT_FILENAME}...")
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8-sig', newline='') as f:
            fieldnames = ['title', 'url', 'stars', 'total_chars', 'first_episode_date', 'tags', 'notice_sexual']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
    else:
        print("\nNo works matched the criteria (Outputting empty file).")
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8-sig', newline='') as f:
            pass

    print(f"Done. Total listing scanned: {total_scanned}")

if __name__ == "__main__":
    main()
