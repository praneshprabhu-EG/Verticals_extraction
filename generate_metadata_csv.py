
import os
import json
import csv
import googleapiclient.discovery
import isodate

# Replace with your actual YouTube Data API key
API_KEY = 'AIzaSyC7GOOp6MedGQ68sB8ooIoX8_nS1-WrA30'

# Folder to process
ROOT_FOLDER = 'ChannelScrappedDataOnly'
JSON_FILE = 'overall_video_ids_array.json'
CSV_FILE = 'shorts_report.csv'

def build_youtube_client():
    """Initialize YouTube API client."""
    return googleapiclient.discovery.build('youtube', 'v3', developerKey=API_KEY)

def fetch_video_metadata(youtube, video_ids):
    """Fetches title and duration for a list of video IDs, batched in groups of 50."""
    metadata = {}
    for i in range(0, len(video_ids), 50):
        batch_ids = ','.join(video_ids[i:i+50])
        try:
            response = youtube.videos().list(
                part='snippet,contentDetails',
                id=batch_ids
            ).execute()
            for item in response.get('items', []):
                vid = item['id']
                title = item['snippet']['title']
                duration_iso = item['contentDetails']['duration']
                duration_seconds = int(isodate.parse_duration(duration_iso).total_seconds())
                metadata[vid] = {'title': title, 'duration_seconds': duration_seconds}
        except Exception as e:
            print(f"Error fetching batch {i}-{i+50}: {e}")
    return metadata

def process_subfolder(subfolder_path, youtube):
    """Processes a subfolder: reads JSON, fetches metadata, and writes CSV with total_shorts in second row."""
    json_path = os.path.join(subfolder_path, JSON_FILE)
    csv_path = os.path.join(subfolder_path, CSV_FILE)
    
    # Get channel name from subfolder name
    channel_name = os.path.basename(subfolder_path)
    
    if not os.path.exists(json_path):
        print(f"Skipping {subfolder_path}: No {JSON_FILE} found")
        return
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            video_ids = json.load(f)
            if not isinstance(video_ids, list) or not video_ids:
                print(f"Skipping {subfolder_path}: Invalid or empty video IDs")
                return
            total_shorts = len(video_ids)  # Count total Shorts
    except Exception as e:
        print(f"Error reading {json_path}: {e}")
        return
    
    # Fetch metadata
    metadata = fetch_video_metadata(youtube, video_ids)
    
    # Write to CSV with channel_name, title, duration_seconds, and total_shorts (in second row only)
    try:
        with open(csv_path, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['channel_name', 'title', 'duration_seconds', 'total_shorts'])
            writer.writeheader()
            for index, vid in enumerate(video_ids, start=1):
                row = {
                    'channel_name': channel_name,
                    'title': metadata.get(vid, {'title': 'Unknown'})['title'],
                    'duration_seconds': metadata.get(vid, {'duration_seconds': 0})['duration_seconds'],
                    'total_shorts': str(total_shorts) if index == 2 else ''  # Write total_shorts only in second row
                }
                writer.writerow(row)
        print(f"Processed {subfolder_path}: Wrote {len(video_ids)} entries to {CSV_FILE} (Total Shorts: {total_shorts})")
    except Exception as e:
        print(f"Error writing {csv_path}: {e}")

def main():
    """Main function to process all subfolders in ROOT_FOLDER."""
    youtube = build_youtube_client()
    
    for subfolder in os.listdir(ROOT_FOLDER):
        subfolder_path = os.path.join(ROOT_FOLDER, subfolder)
        if os.path.isdir(subfolder_path):
            process_subfolder(subfolder_path, youtube)

if __name__ == '__main__':
    main()
