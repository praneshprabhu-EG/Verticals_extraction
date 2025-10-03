import pandas as pd
import requests
import json
import re
import time
import os
import shutil
import traceback
import yt_dlp
from cacher import cacher

@cacher
def getYotubePageResultUsingContinuationToken(channel_videos_url, continuation_token):
    url = "https://www.youtube.com/youtubei/v1/browse?prettyPrint=false"

    payload = json.dumps({
        "context": {
            "client": {
                "hl": "en",
                "gl": "IN",
                "remoteHost": "2405:201:600b:da:b856:4bba:18f5:166a",
                "deviceMake": "",
                "deviceModel": "",
                "visitorData": "CgtZR3dqZW9VbndNVSiLlKG3BjIKCgJJThIEGgAgLw%3D%3D",
                "userAgent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36,gzip(gfe)",
                "clientName": "WEB",
                "clientVersion": "2.20240913.01.00",
                "osName": "Windows",
                "osVersion": "10.0",
                "originalUrl": channel_videos_url,
                "platform": "DESKTOP",
                "clientFormFactor": "UNKNOWN_FORM_FACTOR",
                "configInfo": {
                    "appInstallData": "CIuUobcGEOHssAUQzdewBRCQw7EFELDusAUQ9KuwBRDRr7EFEOyosQUQrsGxBRDwx7EFEN3o_hIQt-r-EhCNzLAFEOvo_hIQ9MCxBRDH5rAFEOW5sQUQzN-uBRDlyrAFEKaSsQUQyL-xBRCinbEFEMaksQUQ2qixBRDZya8FEKaTsQUQydewBRDi1K4FEIXDsQUQ782wBRDf7bAFEKrYsAUQvbauBRCigbAFEL2ZsAUQh8OxBRDT4a8FEJT-sAUQvL6xBRCMzbEFELfvrwUQjZSxBRDQjbAFEJaVsAUQ0sSxBRDX6a8FEOPRsAUQiOOvBRCmmrAFEL2KsAUQm86xBRDerbEFEOuZsQUQgcOxBRCd0LAFEJ2msAUQ7qKvBRDJ5rAFEPirsQUQ4rWxBRComrAFEJLLsQUQgsawBRDJ968FEMT1sAUQiqGxBRDbvrEFEJSJsQUQ6sOvBRCQzLEFEJKusQUQs8r_EhDW3bAFEKiSsQUQlqOxBRD0x7EFEOjMsQUQ9quwBRDbr68FEJ7GsQUQieiuBRCEtrAFEO25sQUQmZixBRCJp7EFEIWnsQUQiIewBRCctP8SEPjGsQUQmsawBRD9x7EFEMrJ_xIqIENBTVNFaFVKb0wyd0ROSGtCdlB0EFF2UDFBNGRCdz09"
                },
                "userInterfaceTheme": "USER_INTERFACE_THEME_LIGHT",
                "timeZone": "Asia/Calcutta",
                "browserName": "Chrome",
                "browserVersion": "128.0.0.0",
                "acceptHeader": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "deviceExperimentId": "ChxOelF4TlRJMU9ERTVPVFk0TlRReU1qazJNZz09EIuUobcGGIuUobcG",
                "screenWidthPoints": 833,
                "screenHeightPoints": 773,
                "screenPixelDensity": 1,
                "screenDensityFloat": 1,
                "utcOffsetMinutes": 330,
                "connectionType": "CONN_CELLULAR_4G",
                "memoryTotalKbytes": "8000000",
                "mainAppWebInfo": {
                    "graftUrl": channel_videos_url,
                    "pwaInstallabilityStatus": "PWA_INSTALLABILITY_STATUS_UNKNOWN",
                    "webDisplayMode": "WEB_DISPLAY_MODE_BROWSER",
                    "isWebNativeShareAvailable": True
                }
            },
            "user": {
                "lockedSafetyMode": False
            },
            "request": {
                "useSsl": True,
                "internalExperimentFlags": [],
                "consistencyTokenJars": []
            },
            "clickTracking": {
                "clickTrackingParams": "CCcQ8eIEIhMI6drMnt_HiAMVt4NjBh0pYg4K"
            },
            "adSignalsInfo": {
                "params": [
                    {"key": "dt", "value": "1726499339448"},
                    {"key": "flash", "value": "0"},
                    {"key": "frm", "value": "0"},
                    {"key": "u_tz", "value": "330"},
                    {"key": "u_his", "value": "2"},
                    {"key": "u_h", "value": "900"},
                    {"key": "u_w", "value": "1440"},
                    {"key": "u_ah", "value": "860"},
                    {"key": "u_aw", "value": "1440"},
                    {"key": "u_cd", "value": "24"},
                    {"key": "bc", "value": "31"},
                    {"key": "bih", "value": "773"},
                    {"key": "biw", "value": "816"},
                    {"key": "brdim", "value": "0,0,0,0,1440,0,1440,860,833,773"},
                    {"key": "vis", "value": "1"},
                    {"key": "wgl", "value": "true"},
                    {"key": "ca_type", "value": "image"}
                ]
            }
        },
        "continuation": continuation_token
    })
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'cookie': 'GPS=1; YSC=yeBffjRQOOE; VISITOR_INFO1_LIVE=YGwjeoUnwMU; VISITOR_PRIVACY_METADATA=CgJJThIEGgAgLw%3D%3D; PREF=tz=Asia.Calcutta; SIDCC=AKEyXzXOCkNZEj1NcF223BoeBvOCdwBTEPR4-kLA-YZh6e1leQq_B8W5CxVhoU0DWCPUaW44; __Secure-1PSIDCC=AKEyXzXuO2Yz6aH2fpDD9-Qu2AqM-p8bb-V6uSpUOy-J924Yc3vhvxJ3ky0kul0jbxIkMVj7; __Secure-3PSIDCC=AKEyXzUAenI0dx6ALh9jBpO8PumOkkgJymf0DE5cDGMi5FfK1wIYKbGbtIGbwNohPe57py1g8w',
        'origin': 'https://www.youtube.com',
        'priority': 'u=1, i',
        'referer': channel_videos_url,
        'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
        'sec-ch-ua-arch': '"x86"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-form-factors': '"Desktop"',
        'sec-ch-ua-full-version': '"128.0.6613.138"',
        'sec-ch-ua-full-version-list': '"Chromium";v="128.0.6613.138", "Not;A=Brand";v="24.0.0.0", "Google Chrome";v="128.0.6613.138"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"Windows"',
        'sec-ch-ua-platform-version': '"10.0.0"',
        'sec-ch-ua-wow64': '?0',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'same-origin',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
        'x-goog-visitor-id': 'CgtZR3dqZW9VbndNVSiLlKG3BjIKCgJJThIEGgAgLw%3D%3D',
        'x-youtube-bootstrap-logged-in': 'false',
        'x-youtube-client-name': '1',
        'x-youtube-client-version': '2.20240913.01.00'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    return response.text

@cacher
def getPageHTML(url):
    resp = requests.get(url)
    return resp.text

def downloadForSingleChannel(channel_videos_url):
    overall_video_ids_array, continuation_token, total_request_count = [], '', 0

    # Get first page data
    html = getPageHTML(channel_videos_url)[2]
    yt_initial_data = re.search(r'\<script\s+nonce\=\"\S+?\"\>var ytInitialData = (.*)\<\/script\>', html).group(1)
    yt_initial_data = yt_initial_data.split(';</script>')[0]
    yt_initial_data = json.loads(yt_initial_data)

    for item in yt_initial_data['contents']['twoColumnBrowseResultsRenderer']['tabs'][1]['tabRenderer']['content']['richGridRenderer']['contents']:
        if item.get('richItemRenderer'):
            overall_video_ids_array.append(item['richItemRenderer']['content']['videoRenderer']['videoId'])
        elif item.get('continuationItemRenderer'):
            continuation_token = item['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
            break

    print('   | Videos captured in current request %s:' % total_request_count, len(overall_video_ids_array), end='\r')

    try:
        # Do API calls to get subsequent videos
        token_already_seen = {continuation_token}
        max_error_retries, curr_error_count = 6, 0
        while True:
            total_request_count += 1
            try:
                current_request_videos = []
                another_request_data = getYotubePageResultUsingContinuationToken(channel_videos_url, continuation_token)[2]
                another_request_data = json.loads(another_request_data)
                for item in another_request_data['onResponseReceivedActions'][0]['appendContinuationItemsAction']['continuationItems']:
                    if item.get('richItemRenderer'):
                        current_request_videos.append(item['richItemRenderer']['content']['videoRenderer']['videoId'])
                    elif item.get('continuationItemRenderer'):
                        continuation_token = item['continuationItemRenderer']['continuationEndpoint']['continuationCommand']['token']
                        break
                # Reset error count after success case
                curr_error_count = 0
            except:
                traceback.print_exc()
                curr_error_count += 1
                if curr_error_count >= max_error_retries:
                    raise Exception("Max error retries exceeded")
                else:
                    print("-"*25, "\nError came. Trying again after 5 seconds.\n", "-"*25, sep='')
                    time.sleep(5)
                    continue

            print('   | Videos captured in current request %s:' % total_request_count, len(current_request_videos), end='\r')
            overall_video_ids_array.extend(current_request_videos)

            if continuation_token in token_already_seen:
                break
            else:
                token_already_seen.add(continuation_token)

            time.sleep(2)
    finally:
        print('\nTotal Shorts:', len(set(overall_video_ids_array)))
        return overall_video_ids_array

def downloadForSingleChannelV2(channel_videos_url):
    def get_channel_id(channel_url):
        ydl_opts = {
            'quiet': True,
            'force_generic_extractor': False,
            'extract_flat': True,
            'skip_download': True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(channel_url, download=False)
            return info.get('channel_id') or info.get('id')

    # Clean the URL to get base channel URL
    base_url = channel_videos_url.split('/shorts')[0]
    channel_id = get_channel_id(base_url)
    
    if not channel_id:
        raise Exception("Could not get channel ID")
    
    # Try multiple approaches to get Shorts
    ydl_opts = {
        'quiet': True,
        'extract_flat': True,
        'skip_download': True,
        'match_filter': lambda info: '/shorts/' in info.get('url', ''),
    }
    
    short_urls = [
        f"https://www.youtube.com/{channel_id}/shorts",
        f"https://www.youtube.com/@{channel_videos_url.split('@')[-1].split('/')[0]}/shorts",
        f"https://www.youtube.com/channel/{channel_id}/shorts"
    ]
    
    overall_video_ids_array = []
    
    for url in short_urls:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if info and 'entries' in info:
                    for entry in info['entries']:
                        if entry.get('id') and len(entry['id']) == 11:
                            overall_video_ids_array.append(entry['id'])
                    if overall_video_ids_array:
                        break
        except Exception as e:
            print(f"   | Attempt with {url} failed: {str(e)}")
            continue
    
    if not overall_video_ids_array:
        print("   | Could not fetch Shorts directly, trying search method...")
        search_url = f"https://www.youtube.com/{channel_id}/search?query=shorts"
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(search_url, download=False)
                if info and 'entries' in info:
                    for entry in info['entries']:
                        if entry.get('id') and len(entry['id']) == 11:
                            overall_video_ids_array.append(entry['id'])
        except Exception as e:
            print(f"   | Search method also failed: {str(e)}")
            return downloadForSingleChannel(channel_videos_url)
    
    print("   | Found %s Shorts video IDs." % len(overall_video_ids_array))
    return overall_video_ids_array

def main():
    dump_folder = "ChannelScrappedDataOnly"
    XLSX_FILE = 'working_sheet.xlsx'
    
    try:
        print(f"Attempting to read XLSX file: {XLSX_FILE}")
        df = pd.read_excel(XLSX_FILE)
        print(f"XLSX file read successfully. Columns: {df.columns.tolist()}")
        
        channel_videos_url_array = []
        for url in df.iloc[:, 0]:
            if isinstance(url, str):
                # Normalize URL to include /shorts if not present
                if not url.endswith('/shorts'):
                    url = url.rstrip('/') + '/shorts'
                channel_videos_url_array.append(url)
        print(f"Found {len(channel_videos_url_array)} valid YouTube channel URLs")
        
        if os.path.exists("cache") or os.path.exists("overall_video_ids_array.json"):
            raise Exception("Previous downloaded data not properly relocated")
        
        results = []
        for channel_videos_url in channel_videos_url_array:
            try:
                channel_handle = re.match(r'https\:\/\/www\.youtube\.com\/\@(\S+?)\/shorts', channel_videos_url).group(1)
                final_dump_folder = f"{dump_folder}/{channel_handle}"
                
                if os.path.exists(final_dump_folder):
                    print(f">> Skipping channel as its already done: {channel_handle}")
                    continue
                else:
                    print(f"\n>> Processing channel: {channel_handle}")
                
                video_ids = downloadForSingleChannelV2(channel_videos_url)
                total_shorts = len(set(video_ids))
                
                results.append({
                    'Handle': channel_handle,
                    'TotalShorts': total_shorts
                })
                
                os.makedirs(final_dump_folder)
                with open(f"{final_dump_folder}/overall_video_ids_array.json", 'w', encoding='utf-8') as f:
                    f.write(json.dumps(video_ids))
                
                if os.path.exists("cache"):
                    shutil.move("cache", f"{final_dump_folder}/cache")
            
            except Exception as e:
                print(f"Error processing {channel_videos_url}: {str(e)}")
                results.append({
                    'Handle': channel_videos_url,
                    'TotalShorts': 0
                })
                continue
        
        # Save results to CSV
        output_csv = 'shorts_count.csv'
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_csv, index=False)
        print(f"Results written to {output_csv}")
        
    except FileNotFoundError:
        print(f"Error: XLSX file '{XLSX_FILE}' not found.")
    except Exception as e:
        print(f"Unexpected error in main: {e}")

if __name__ == '__main__':
    main()