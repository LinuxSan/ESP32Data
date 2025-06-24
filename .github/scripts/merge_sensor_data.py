import os
import glob
import pandas as pd
from datetime import datetime, timedelta # Import timedelta

def cleanup_old_sensor_files(directory='data', hours_old=48):
    """
    Deletes individual sensor data files older than a specified number of hours.
    """
    now = datetime.now()
    cutoff_time = now - timedelta(hours=hours_old)
    
    files = glob.glob(os.path.join(directory, 'sensor_*.csv'))
    
    for file in files:
        try:
            file_mod_time = datetime.fromtimestamp(os.path.getmtime(file))
            if file_mod_time < cutoff_time:
                os.remove(file)
                print(f"Deleted old file: {file}")
        except Exception as e:
            print(f"Error deleting {file}: {e}")

def merge_sensor_files():
    # Find all sensor data files
    files = sorted(glob.glob('data/sensor_*.csv'))
    
    if not files:
        print("No sensor files found")
        return False

    # Read and combine all files
    dfs = []
    for file in files:
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if not dfs:
        print("No valid data found")
        return False

    # Combine and deduplicate
    combined = pd.concat(dfs, ignore_index=True)
    combined.drop_duplicates(inplace=True)
    combined.sort_values(by=['timestamp'], inplace=True)

    # Save merged file
    os.makedirs('data', exist_ok=True)
    combined.to_csv('data/sensor_data_combined.csv', index=False)
    
    # Cleanup individual files after merging and saving
    cleanup_old_sensor_files() # Call the cleanup function here
    
    return True

if __name__ == "__main__":
    print(f"Starting merge at {datetime.now()}")
    if merge_sensor_files():
        print("Merge completed successfully")
    else:
        print("Merge failed")
        exit(1)
