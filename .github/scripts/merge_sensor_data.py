import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import subprocess

def get_git_commit_time(filepath):
    """
    Gets the last commit time of a file from Git history.
    Returns a datetime object or None if an error occurs.
    """
    try:
        # Use git log to get the commit date of the last commit that modified the file
        command = ["git", "log", "-1", "--format=%cd", "--date=iso-strict", "--", filepath]
        result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=os.path.dirname(filepath) or '.')
        
        # Parse the ISO 8601 formatted date
        commit_time_str = result.stdout.strip()
        if commit_time_str:
            return datetime.fromisoformat(commit_time_str)
    except subprocess.CalledProcessError as e:
        print(f"Error getting git commit time for {filepath}: {e}")
        print(f"Stderr: {e.stderr}")
    except ValueError as e:
        print(f"Error parsing date for {filepath}: {e}")
    return None

def cleanup_old_sensor_files(directory='data', hours_old=48):
    """
    Deletes individual sensor data files in the repository
    that were last committed more than a specified number of hours ago.
    """
    now = datetime.now()
    cutoff_time = now - timedelta(hours=hours_old)
    
    files = glob.glob(os.path.join(directory, 'sensor_*.csv'))
    
    # Ensure the script is run from the root of the repository for git commands to work correctly
    original_cwd = os.getcwd()
    try:
        # Navigate to the repository root if not already there
        repo_root = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True).stdout.strip()
        os.chdir(repo_root)

        for file_path in files:
            # Get path relative to the Git repository root
            relative_filepath = os.path.relpath(file_path, repo_root)
            file_commit_time = get_git_commit_time(relative_filepath)

            if file_commit_time and file_commit_time < cutoff_time:
                try:
                    os.remove(file_path)
                    print(f"Deleted old file: {file_path}")
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")
            elif file_commit_time:
                print(f"Keeping {file_path} (last committed at {file_commit_time})")
            else:
                print(f"Could not determine commit time for {file_path}, skipping deletion.")
    except subprocess.CalledProcessError as e:
        print(f"Error finding repository root or running git command: {e}")
        print(f"Stderr: {e.stderr}")
    finally:
        os.chdir(original_cwd) # Change back to original working directory


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
    
    # After merging and saving the combined file, clean up old individual sensor files
    cleanup_old_sensor_files() 
    
    return True

if __name__ == "__main__":
    print(f"Starting merge at {datetime.now()}")
    if merge_sensor_files():
        print("Merge completed successfully")
        # Additionally, commit the deletion changes if any files were removed
        try:
            subprocess.run(["git", "config", "--global", "user.name", "\"GitHub Actions\""], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "\"actions@github.com\""], check=True)
            subprocess.run(["git", "add", "data/"], check=True) # Add data directory to stage deleted files
            commit_output = subprocess.run(["git", "commit", "-m", "Cleaned up old sensor files [skip ci]"], capture_output=True, text=True)
            if "nothing to commit" not in commit_output.stdout and "no changes added to commit" not in commit_output.stderr:
                print("Changes committed:", commit_output.stdout.strip())
                subprocess.run(["git", "push"], check=True)
                print("Deletion changes pushed to repository.")
            else:
                print("No deletion changes to commit or push.")
        except subprocess.CalledProcessError as e:
            print(f"Error committing or pushing deletion changes: {e}")
            print(f"Stderr: {e.stderr}")
    else:
        print("Merge failed")
        exit(1)
