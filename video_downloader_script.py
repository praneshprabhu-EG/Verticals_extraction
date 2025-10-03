import pandas as pd
import os
import yt_dlp
import threading
from concurrent.futures import ThreadPoolExecutor
import time

# Global variables
INPUT_FILE = 'input_main.xlsx'
OUTPUT_DIR = 'downloads'
PROCESSED_FILE = 'processed_urls.txt'
MAX_WORKERS = 2

# Create the output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Load processed URLs
if os.path.exists(PROCESSED_FILE):
    with open(PROCESSED_FILE, 'r') as f:
        processed = set(line.strip() for line in f if line.strip())
else:
    processed = set()

# Read URLs from the Excel file
try:
    with pd.ExcelFile(INPUT_FILE, engine='openpyxl') as xls:
        df = pd.read_excel(xls)
    urls = [url.strip() for url in df.iloc[:, 0] if pd.notna(url) and isinstance(url, str) and url.strip() and url.strip() not in processed]
except FileNotFoundError:
    print(f"Error: {INPUT_FILE} not found.")
    exit(1)
except Exception as e:
    print(f"Error reading {INPUT_FILE}: {e}")
    exit(1)


def appendProcessedUrl(url):
    with threading.Lock():
        with open(PROCESSED_FILE, 'a') as f:
            f.write(url + '\n')


def downloadVideo(url):
    max_retries = 3
    retry_delay = 60
    for attempt in range(max_retries):
        try:
            print(f"Processing URL: {url} (Attempt {attempt + 1}/{max_retries})")
            ydl_opts = {
                'format': 'bv*+ba/b',
                'outtmpl': os.path.join(OUTPUT_DIR, '%(channel)s/%(title)s.%(ext)s'),
                'merge_output_format': 'mp4',
                'noplaylist': False,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=True)
                channel_name = info.get('channel') or info.get('uploader') or 'Unknown_Channel'
                channel_dir = os.path.join(OUTPUT_DIR, channel_name)
                if not os.path.exists(channel_dir):
                    os.makedirs(channel_dir)
            print(f"Completed: {url}")
            appendProcessedUrl(url)
            return
        except yt_dlp.utils.DownloadError as e:
            print(f"Download error for {url}: {e}")
            if "rate-limited" in str(e).lower() or "429" in str(e).lower():
                print(f"Rate limit detected. Waiting {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            return
        except Exception as e:
            print(f"Unexpected error for {url}: {e}")
            return


# Download videos concurrently
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(downloadVideo, urls)

print("All downloads completed.")