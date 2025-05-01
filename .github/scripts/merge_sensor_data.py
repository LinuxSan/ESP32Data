import os
import glob
import pandas as pd
from datetime import datetime

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
    
    # Cleanup (optional): Remove individual files
    # for file in files:
    #     os.remove(file)
    
    return True

if __name__ == "__main__":
    print(f"Starting merge at {datetime.now()}")
    if merge_sensor_files():
        print("Merge completed successfully")
    else:
        print("Merge failed")
        exit(1)
