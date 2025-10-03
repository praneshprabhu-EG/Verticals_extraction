import os
import pandas as pd

# Folder to process
ROOT_FOLDER = 'ChannelScrappedDataOnly'
CSV_FILE = 'shorts_report.csv'
COMBINED_CSV = 'combined_report_only.csv'

def process_subfolder(subfolder_path):
    """Processes a subfolder: reads shorts_report.csv and computes totals."""
    csv_path = os.path.join(subfolder_path, CSV_FILE)
    
    if not os.path.exists(csv_path):
        print(f"Skipping {subfolder_path}: No {CSV_FILE} found")
        return None
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            print(f"Skipping {subfolder_path}: Empty CSV")
            return None
        
        # Get channel name from subfolder or first row
        channel_name = os.path.basename(subfolder_path)
        
        # Total shorts: Number of rows in CSV (assuming one per short)
        total_shorts = len(df)
        
        # Total duration: Sum of duration_seconds (handle non-numeric gracefully)
        total_duration = df['duration_seconds'].sum()
        
        return {
            'channel_name': channel_name,
            'total_shorts': total_shorts,
            'total_duration_seconds': total_duration
        }
    except Exception as e:
        print(f"Error processing {csv_path}: {e}")
        return None

def main():
    """Main function to process all subfolders and create combined CSV."""
    results = []
    
    for subfolder in os.listdir(ROOT_FOLDER):
        subfolder_path = os.path.join(ROOT_FOLDER, subfolder)
        if os.path.isdir(subfolder_path):
            channel_data = process_subfolder(subfolder_path)
            if channel_data:
                results.append(channel_data)
    
    if not results:
        print("No valid data found to combine.")
        return
    
    # Create combined DataFrame and save to CSV
    combined_df = pd.DataFrame(results)
    combined_df.to_csv(COMBINED_CSV, index=False)
    print(f"Combined report written to {COMBINED_CSV} with {len(combined_df)} channels.")

if __name__ == '__main__':
    main()