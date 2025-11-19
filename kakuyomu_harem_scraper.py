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

TARGET_TAG_SEARCH = "ハーレム"
TARGET_START_DATE = datetime(2025, 4, 1)
MIN_STARS = 3000
MIN_CHARS = 50000

BASE_URL = "https://kakuyomu.jp"
SEARCH_URL = f"{BASE_URL}/tags/{quote(TARGET_TAG_SEARCH)}"

SLEEP_TIME = 2.0
MAX_LISTING_PAGES = 80
MAX_EPISODE_PAGES = 50
TIMEOUT = 15

OUTPUT_FILENAME = "kakuyomu_harem_filtered.csv"

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

def get_work_listing_info(soup):
    """
    一覧ページから作品情報の候補を抽出する（クラス名に依存しないロバストな実装）
    戻り値: {url: {stars: int, chars: int}} の辞書
    """
    works_map = {}
    
    # メインコンテンツエリアを特定（サイドバーのリンクを拾わないようにするため）
    main_area = soup.select_one('main, div[role="main"], .widget-mainColumn, #main')
    if not main_area:
        main_area = soup # 見つからなければ全体から探す

    # hrefが "/works/数字" で終わるリンクを全て取得
    links = main_area.find_all('a', href=re.compile(r'^/works/\d+$'))
    
    for link in links:
        href = link.get('href')
        url = urljoin(BASE_URL, href)
        
        # 重複チェック（既に処理済みならスキップ）
        if url in works_map:
            continue

        # リンクの親要素を辿って、作品情報のコンテナ（カード）と思われる要素を探す
        # "文字" というテキストが含まれている最も近い親要素（div, article, li）を探す
        container = None
        curr = link.parent
        for _ in range(4): # 4階層まで遡る
            if not curr: break
            if curr.name in ['div', 'article', 'li'] and "文字" in curr.get_text():
                container = curr
                break
            curr = curr.parent
        
        if not container:
            # コンテナが見つからない場合は、リンク周辺の情報が取れないのでスキップまたはデフォルト値
            # ここでは安全のため、詳細ページ取得対象候補として登録（数値0）
            works_map[url] = {'stars': 0, 'chars': 0}
            continue

        text_content = container.get_text()

        # 星数を正規表現で探す (例: ★1,234 / ☆ 1234 / 1234)
        # "★"や"☆"の近くにある数字、または ReviewPoints などのクラスを持つ要素
        stars = 0
        # パターン1: クラス名指定での取得試行
        star_elm = container.select_one('[class*="ReviewPoints"], [class*="Star-module__count"], .js-stars-count')
        if star_elm:
            stars = parse_int(star_elm.get_text())
        else:
            # パターン2: テキスト解析 (★ 1,234)
            star_match = re.search(r'[★☆]\s*([\d,]+)', text_content)
            if star_match:
                stars = parse_int(star_match.group(1))

        # 文字数を正規表現で探す (例: 12,345文字)
        chars = 0
        char_match = re.search(r'([\d,]+)文字', text_content)
        if char_match:
            chars = parse_int(char_match.group(1))

        works_map[url] = {'stars': stars, 'chars': chars}

    return works_map

def process_work_details(session, work_url, listing_stars, listing_chars):
    soup = fetch_soup(session, work_url)
    if not soup: return None

    # タイトル
    title_tag = soup.select_one('h1#workTitle, h1') 
    title = title_tag.get_text(strip=True) if title_tag else "No Title"

    # 星数・文字数（詳細ページで再取得を試みる）
    stars = listing_stars
    points_elm = soup.select_one('#workPoints, .js-stars-count')
    if points_elm: stars = parse_int(points_elm.get_text())

    total_chars = listing_chars
    chars_elm = soup.select_one('#workTotalCharacterCount')
    if chars_elm: total_chars = parse_int(chars_elm.get_text())

    # フィルタ
    if stars < MIN_STARS or total_chars < MIN_CHARS:
        return None

    # タグ
    tags = []
    tag_links = soup.select('[itemprop="keywords"] a, #tagList a, [class*="TagList"] a')
    for link in tag_links:
        tags.append(link.get_text(strip=True))
    
    if TARGET_TAG_SEARCH not in tags:
        return None

    # 性描写ありチェック
    notice_sexual = False
    notice_elms = soup.select('#workHeader-inner, .work-header-notice, [class*="Notice"], [aria-label="性描写あり"]')
    full_text = " ".join([e.get_text() for e in notice_elms]) + " " + " ".join([e.get('aria-label', '') for e in soup.select('[aria-label]')])
    
    if "性描写あり" in full_text:
        notice_sexual = True

    if not notice_sexual:
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

        time_tags = soup.select('li.widget-episode time, [class*="EpisodeList"] time')
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

        next_link = soup.select_one('a[rel="next"], [class*="pager-next"] a')
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

        # ページタイトル確認
        page_title = soup.title.get_text(strip=True) if soup.title else "No Title"
        print(f"Page {page} Title: {page_title}")

        # 新しいロジックで作品リストを取得
        works_info_map = get_work_listing_info(soup)
        
        if not works_info_map:
            print(f"No works found on page {page}. (Link detection failed)")
            break

        print(f"Processing page {page} ({len(works_info_map)} works found)...")

        for work_url, info in works_info_map.items():
            total_scanned += 1
            
            # 一覧での簡易フィルタ
            # 数値が取得できていない(0)場合は、念のため詳細ページを見に行く（取りこぼし防止）
            # 数値が明確に取得できていて、かつ基準未満ならスキップ
            if info['stars'] > 0 and info['stars'] < (MIN_STARS * 0.8):
                continue
            if info['chars'] > 0 and info['chars'] < (MIN_CHARS * 0.8):
                continue
            
            print(f"Checking: {work_url} (Est. Stars: {info['stars']}, Chars: {info['chars']})")
            
            try:
                work_data = process_work_details(session, work_url, info['stars'], info['chars'])
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
