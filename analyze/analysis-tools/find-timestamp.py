import gzip
import json
import glob
import argparse
import os

"""
Example: python3 find-timestamp.py "cpu-load.jsonl-*.gz" 1762300801

This script scan through the .gz json files and print the file name, and line numbers
containing the specific timestamp.
"""


def search_files(file_pattern, target_timestamp):
    # 1. Gather Files
    files = sorted(glob.glob(str(file_pattern)))
    if not files:
        print(f"No files found matching: {file_pattern}")
        return

    print(f"Searching for timestamp {target_timestamp} in {len(files)} files...")
    print("-" * 60)

    found_count = 0

    # 2. Iterate sequentially
    for filepath in files:
        try:
            # Auto-detect GZIP vs Plain Text
            if filepath.endswith('.gz'):
                opener = gzip.open(filepath, 'rt', encoding='utf-8')
            else:
                opener = open(filepath, 'r', encoding='utf-8')

            with opener as f:
                for line_num, line in enumerate(f, 1):
                    if not line.strip(): continue
                    
                    # Optimization: Fast string check first
                    if str(target_timestamp) not in line:
                        continue

                    try:
                        # Parsing Check
                        data = json.loads(line)
                        ts = data.get('TimeStamp')

                        if ts is not None and int(float(ts)) == int(target_timestamp):
                            print(f"FOUND in: {os.path.basename(filepath)} | Line: {line_num}")
                            found_count += 1
                            
                    except (ValueError, json.JSONDecodeError):
                        continue
                        
        except Exception as e:
            print(f"Error reading {filepath}: {e}")

    print("-" * 60)
    print(f"Search complete. Total matches found: {found_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple search for timestamp in log files.")
    parser.add_argument("file_pattern", type=str, help="File pattern (e.g. 'cpu-load.jsonl-*.gz')")
    parser.add_argument("timestamp", type=int, help="The Unix timestamp to find")
    
    args = parser.parse_args()
    
    search_files(args.file_pattern, args.timestamp)
