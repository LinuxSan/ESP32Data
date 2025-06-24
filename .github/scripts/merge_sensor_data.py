import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import subprocess

def is_git_tracked(filepath):
    """Checks if a file is tracked by Git."""
    try:
        subprocess.run(["git", "ls-files", "--error-unmatch", filepath],
                       capture_output=True, check=True, cwd=os.path.dirname(filepath) or '.',
                       text=True,
                       timeout=10)
        return True
    except subprocess.CalledProcessError:
        return False
    except FileNotFoundError:
        print("Error: 'git' command not found. Ensure Git is installed and in PATH.")
        return False
    except subprocess.TimeoutExpired:
        print(f"Timeout checking git tracking for {filepath}.")
        return False

def get_git_commit_time(filepath):
    """
    Gets the last commit time of a file from Git history.
    Returns a datetime object or None if an error occurs.
    """
    try:
        command = ["git", "log", "-1", "--format=%cd", "--date=iso-strict", "--", filepath]
        result = subprocess.run(command, capture_output=True, text=True, check=True, cwd=os.path.dirname(filepath) or '.', timeout=10)

        commit_time_str = result.stdout.strip()
        if commit_time_str:
            return datetime.fromisoformat(commit_time_str)
    except subprocess.CalledProcessError as e:
        print(f"Git command failed for {filepath}: {e.stderr.strip()}")
    except ValueError as e:
        print(f"Error parsing date for {filepath}: {e}")
    except subprocess.TimeoutExpired:
        print(f"Timeout getting git commit time for {filepath}.")
    return None

def cleanup_sensor_files_by_count(directory='data', files_to_keep=100):
    """
    Keeps only the newest 'files_to_keep' sensor data files and deletes all others.
    Recency is determined by Git commit time for tracked files, and filesystem
    modification time for untracked files.
    """
    file_info = [] # List to store (timestamp, filepath, source_of_timestamp) tuples

    files = glob.glob(os.path.join(directory, 'sensor_*.csv'))

    original_cwd = os.getcwd()
    repo_root = None
    try:
        # Determine repository root
        repo_root_process = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=False, timeout=10)
        if repo_root_process.returncode == 0:
            repo_root = repo_root_process.stdout.strip()
            os.chdir(repo_root)
            print(f"Changed current directory to Git repository root: {repo_root}")
        else:
            print(f"Warning: Could not determine Git repository root. Git commands might fail or work relative to current dir. Error: {repo_root_process.stderr.strip()}")
            repo_root = original_cwd # Fallback

        for file_path in files:
            timestamp = None
            source = ""
            try:
                # Try to get Git commit time first for tracked files
                if repo_root and is_git_tracked(os.path.relpath(file_path, repo_root)):
                    timestamp = get_git_commit_time(os.path.relpath(file_path, repo_root))
                    source = "Git commit time"

                # If not Git tracked or Git time failed, fall back to filesystem modification time
                if not timestamp:
                    timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))
                    source = "Filesystem modification time (untracked or Git lookup failed)"

                if timestamp:
                    file_info.append((timestamp, file_path, source))
                else:
                    print(f"Could not determine any time for {file_path}, skipping from cleanup logic.")
            except Exception as e:
                print(f"Error processing file {file_path} for cleanup: {e}")

    except Exception as e:
        print(f"An error occurred during file information gathering: {e}")
    finally:
        os.chdir(original_cwd) # Change back to original working directory

    # Sort files by timestamp in descending order (newest first)
    file_info.sort(key=lambda x: x[0], reverse=True)

    # Identify files to keep and files to delete
    files_to_delete = file_info[files_to_keep:]
    files_to_keep_list = file_info[:files_to_keep]

    print(f"\nKeeping the newest {len(files_to_keep_list)} files:")
    for ts, fp, src in files_to_keep_list:
        print(f"- {os.path.basename(fp)} (Timestamp: {ts.strftime('%Y-%m-%d %H:%M:%S')} - Source: {src})")

    print(f"\nDeleting {len(files_to_delete)} older files:")
    for ts, fp, src in files_to_delete:
        try:
            os.remove(fp)
            print(f"Deleted: {os.path.basename(fp)} (Timestamp: {ts.strftime('%Y-%m-%d %H:%M:%S')} - Source: {src})")
        except Exception as e:
            print(f"Error deleting {fp}: {e}")

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

    # After merging and saving the combined file, clean up individual sensor files
    # Keep only the newest 100 files
    cleanup_sensor_files_by_count(files_to_keep=100)

    return True

if __name__ == "__main__":
    print(f"Starting merge at {datetime.now()}")
    if merge_sensor_files():
        print("Merge completed successfully")
        # Attempt to commit and push changes including deletions
        try:
            subprocess.run(["git", "config", "--global", "user.name", "\"GitHub Actions\""], check=True)
            subprocess.run(["git", "config", "--global", "user.email", "\"actions@github.com\""], check=True)

            # Stage all changes in the data/ directory (including deleted files and the combined CSV)
            subprocess.run(["git", "add", "data/"], check=True)

            # Commit only if there are actual changes
            commit_result = subprocess.run(["git", "commit", "-m", "Cleaned up old sensor files and updated combined data [skip ci]"], capture_output=True, text=True, check=False)

            if "nothing to commit" not in commit_result.stdout and "no changes added to commit" not in commit_result.stderr:
                print("Changes committed:", commit_result.stdout.strip())
                subprocess.run(["git", "push"], check=True)
                print("Deletion and merge changes pushed to repository.")
            else:
                print("No changes to commit or push (either no old files to delete or no new data).")
        except subprocess.CalledProcessError as e:
            print(f"Error committing or pushing deletion changes: {e}")
            print(f"Stderr: {e.stderr}")
        except FileNotFoundError:
            print("Git command not found. Ensure Git is installed in the runner environment.")
    else:
        print("Merge failed")
        exit(1)
