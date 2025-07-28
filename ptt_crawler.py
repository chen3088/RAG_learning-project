import csv
import os
import random
import re
import time
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BOARD_NAME = "stock"
CONFIG = {
    "BOARD_NAME": BOARD_NAME,
    "BOARD_URL": f"https://www.ptt.cc/bbs/{BOARD_NAME}/index.html",
    "BASE_URL": "https://www.ptt.cc",
    "HEADERS": {"User-Agent": "Mozilla/5.0"},
    "ENCODING": "utf-8",
    "DEFAULT_THRESHOLD": 40,
    "SLEEP_TIME_RANGE": (0.1, 0.3),
    "PAGE_SLEEP_TIME_RANGE": (0.2, 0.5),
    "DATA_DIR": os.path.join(os.getcwd(), BOARD_NAME),
}

# Ensure data directory exists
os.makedirs(CONFIG["DATA_DIR"], exist_ok=True)

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def safe_request(url: str, headers: Optional[Dict[str, str]] = None) -> Optional[requests.Response]:
    """Make an HTTP GET request with basic error handling."""
    try:
        headers = headers or CONFIG["HEADERS"]
        res = requests.get(url, headers=headers)
        res.encoding = CONFIG["ENCODING"]
        return res
    except Exception as err:
        print(f"⚠️ Request failed for {url}: {err}")
        return None


def random_sleep(range_tuple: Optional[tuple] = None) -> None:
    """Sleep for a random duration within the given range."""
    sleep_range = range_tuple or CONFIG["SLEEP_TIME_RANGE"]
    time.sleep(round(random.uniform(*sleep_range), 2))


def safe_write_csv(data: Dict[str, str], filename: str, *, mode: str = "a", fieldnames: List[str] | None = None) -> bool:
    """Write a single row or multiple rows to a CSV file safely."""
    try:
        with open(filename, mode=mode, encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if f.tell() == 0:
                writer.writeheader()
            if isinstance(data, list):
                writer.writerows(data)
            else:
                writer.writerow(data)
        return True
    except Exception as err:
        print(f"❌ Error writing to {filename}: {err}")
        return False


def get_data_path(filename: str) -> str:
    """Return the absolute path for files stored in the board directory."""
    path = os.path.join(CONFIG["DATA_DIR"], filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path

# ---------------------------------------------------------------------------
# Crawling functions
# ---------------------------------------------------------------------------

def get_latest_index(board_url: str | None = None) -> Optional[int]:
    """Return the latest page index for the configured board."""
    board_url = board_url or CONFIG["BOARD_URL"]
    res = safe_request(board_url)
    if not res:
        return None
    soup = BeautifulSoup(res.text, "html.parser")
    prev = soup.select_one("div.btn-group-paging a.btn.wide:nth-child(2)")
    if prev and (m := re.search(r"index(\d+)\.html", prev["href"])):
        return int(m.group(1)) + 1
    return None


def crawl_page(index: int) -> List[Dict[str, str]]:
    """Crawl a single page and return a list of article metadata."""
    url = f"{CONFIG['BASE_URL']}/bbs/{BOARD_NAME}/index{index}.html"
    print(f"📄 抓取頁面: {url}")
    res = safe_request(url)
    if not res:
        return []
    soup = BeautifulSoup(res.text, "html.parser")
    records = []
    for div in soup.select("div.r-ent"):
        title_tag = div.select_one("div.title > a")
        date_tag = div.select_one("div.meta > div.date")
        nrec_tag = div.select_one("div.nrec")
        if title_tag and date_tag:
            records.append({
                "title": title_tag.text.strip(),
                "date": date_tag.text.strip(),
                "link": f"{CONFIG['BASE_URL']}{title_tag['href']}",
                "nrec": nrec_tag.text.strip() if nrec_tag else "0",
            })
    return records


def crawl_posts(num_pages: int = 5) -> pd.DataFrame:
    """Main entry to crawl multiple pages of posts."""
    latest_index = get_latest_index()
    if latest_index is None:
        print("❌ 無法取得最新頁碼")
        return pd.DataFrame()
    all_articles: List[Dict[str, str]] = []
    for page_index in range(latest_index, latest_index - num_pages, -1):
        all_articles.extend(crawl_page(page_index))
        random_sleep(CONFIG["PAGE_SLEEP_TIME_RANGE"])
    return pd.DataFrame(all_articles)


def process_recommendations(df: pd.DataFrame, threshold: int | None = None) -> pd.DataFrame:
    """Filter posts with recommendations greater than the given threshold."""
    threshold = threshold or CONFIG["DEFAULT_THRESHOLD"]
    numeric = pd.to_numeric(df["nrec"].replace({"爆": "100", "X": "-1"}), errors="coerce").fillna(0)
    return df[numeric >= threshold]


def process_and_save_data(df: pd.DataFrame, *, threshold: int | None = None, filename: str | None = None) -> pd.DataFrame:
    """Filter posts by recommendations and save to CSV."""
    threshold = threshold or CONFIG["DEFAULT_THRESHOLD"]
    if filename is None:
        filename = f"{CONFIG['BOARD_NAME']}_above_{threshold}_rec.csv"
    filtered_df = process_recommendations(df, threshold)
    output_path = get_data_path(filename)
    filtered_df.to_csv(output_path, encoding="utf-8-sig", index=False)
    print(f"✅ Successfully saved {len(filtered_df)} records with {threshold}+ recommendations to {output_path}")
    return filtered_df


def visualize_recommendations(df: pd.DataFrame, threshold: int | None = None) -> None:
    """Display simple statistics and histogram of recommendation counts."""
    import matplotlib.pyplot as plt
    threshold = threshold or CONFIG["DEFAULT_THRESHOLD"]
    print(f"Posts with {threshold}+ recommendations: {len(df)}")
    plt.figure(figsize=(10, 6))
    plt.hist(df["nrec"], bins=30, edgecolor="black", alpha=0.7)
    plt.title(f"Distribution of Posts with {threshold}+ Recommendations")
    plt.xlabel("Number of Recommendations")
    plt.ylabel("Frequency")
    plt.grid(True, alpha=0.3)
    plt.show()

# ---------------------------------------------------------------------------
# Post content functions
# ---------------------------------------------------------------------------

def get_structured_content(url: str) -> Dict[str, str]:
    """Return structured content for a given post URL."""
    res = safe_request(url)
    if not res:
        return {}
    soup = BeautifulSoup(res.text, "html.parser")
    main = soup.select_one("div#main-content")
    if not main:
        return {}
    for tag in main.find_all(["div", "span"], recursive=False):
        tag.extract()
    text = main.get_text(separator="\n").strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    urls = [a["href"] for a in main.find_all("a", href=True)]
    title = ""
    source = ""
    body_lines: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if "標題" in line:
            title = re.sub(r"^.*?標題[:,：]\s*", "", line).strip()
        elif "作者" in line:
            source = re.sub(r"^.*?作者[:,：]\s*", "", line).strip()
        else:
            body_lines.append(line)
        i += 1
    clean_content: List[str] = []
    content_started = False
    for line in body_lines:
        if not content_started:
            if "看板" in line or "時間" in line:
                continue
            content_started = True
        clean_content.append(line)
    return {
        "title": title,
        "source": source,
        "urls": urls,
        "content": "\n".join(clean_content).strip(),
    }


def crawl_ptt_post_content(filtered_data: pd.DataFrame, *, output_file: str | None = None) -> None:
    """Crawl each post in `filtered_data` and append structured content to CSV."""
    if output_file is None:
        output_file = get_data_path(f"{CONFIG['BOARD_NAME']}_content.csv")
    existing_links: set[str] = set()
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
            existing_links = set(existing_df["link"].dropna())
            print(f"📁 已存在資料筆數：{len(existing_links)}")
        except Exception as err:
            print(f"⚠️ 無法讀取 {output_file}，錯誤：{err}")
    to_crawl = filtered_data[~filtered_data["link"].isin(existing_links)]
    print(f"🚀 準備抓取新連結筆數：{len(to_crawl)}")
    fieldnames = ["title", "date", "link", "nrec", "source", "content", "urls"]
    for _, row in to_crawl.iterrows():
        structured = get_structured_content(row["link"])
        record = {
            "title": row["title"],
            "date": row["date"],
            "link": row["link"],
            "nrec": row["nrec"],
            "source": structured.get("source", ""),
            "content": structured.get("content", ""),
            "urls": "|".join(structured.get("urls", [])),
        }
        safe_write_csv(record, output_file, fieldnames=fieldnames)
        random_sleep()
    print("✅ 全部內文抓取完成！")

# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"📂 Data will be saved in: {CONFIG['DATA_DIR']}")
    data = crawl_posts(num_pages=10)
    raw_file = f"{CONFIG['BOARD_NAME']}_raw.csv"
    data.to_csv(get_data_path(raw_file), encoding="utf-8-sig", index=False)
    print(f"✅ Raw data saved to {raw_file}")
    filtered = process_and_save_data(data)
    crawl_ptt_post_content(filtered)
