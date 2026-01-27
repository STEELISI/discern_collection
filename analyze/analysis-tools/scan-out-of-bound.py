import gzip
import json
import glob
import os
import argparse
from datetime import datetime, timezone
"""
This script prints the out-of-bound data count, based on the name of the files
As the input require a batch of files (cpu-load.jsonl-YYYYMMDDHHMMSS.gz), and the data
time is determined to be 24 hours prior to the date indicated in the data's name. 

Example: python3 scan-out-of-bound.py "cpu-load.jsonl-*.gz"
"""
def check_file_bounds(file_pattern):
    # Expand wildcard pattern
    files = sorted(glob.glob(str(file_pattern)))
    
    if not files:
        print(f"No files found matching: {file_pattern}")
        return

    print(f"Checking {len(files)} files for time-bound errors...")
    print("-" * 70)
    print(f"{'Filename':<50} | {'Out of Bound Count'}")
    print("-" * 70)

    total_out_of_bound_global = 0

    for filepath in files:
        filename = os.path.basename(filepath)
        
        # 1. Parse Timestamp from Filename
        # Format: cpu-load.jsonl-YYYYMMDDHHMMSS.gz
        try:
            # Extract the date part: remove prefix and extension
            # Assumes format: "prefix-YYYYMMDDHHMMSS.gz"
            # We split by '-' and take the last part, then strip '.gz'
            date_str = filename.split('-')[-1].replace('.gz', '')
            
            # Parse to datetime object (Assume UTC)
            end_dt = datetime.strptime(date_str, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            
            # Convert to Unix Timestamp
            end_ts = end_dt.timestamp()
            start_ts = end_ts - 86400  # 24 hours prior
            
        except (ValueError, IndexError):
            print(f"{filename:<50} | [SKIP] Could not parse date from name")
            continue

        # 2. Scan File Content
        out_of_bound_count = 0
        
        try:
            # Auto-handle .gz or plain files
            if filepath.endswith('.gz'):
                opener = gzip.open(filepath, 'rt', encoding='utf-8')
            else:
                opener = open(filepath, 'r', encoding='utf-8')

            with opener as f:
                for line in f:
                    if not line.strip(): continue
                    
                    try:
                        data = json.loads(line)
                        ts = data.get('TimeStamp')
                        
                        if ts is None:
                            continue
                            
                        ts_val = float(ts)
                        
                        # 3. The Constraint Check
                        # If timestamp is BEFORE start or AFTER end -> Error
                        if ts_val < start_ts or ts_val > end_ts:
                            out_of_bound_count += 1
                            
                    except (ValueError, json.JSONDecodeError):
                        continue
        
        except Exception as e:
            print(f"{filename:<50} | Error: {e}")
            continue

        # 3. Print Result
        # We highlight non-zero counts for easier reading
        if out_of_bound_count > 0:
            count_display = f"!! {out_of_bound_count} !!" 
        else:
            count_display = "0"
            
        print(f"{filename:<50} | {count_display}")
        total_out_of_bound_global += out_of_bound_count

    print("-" * 70)
    print(f"Scan Complete. Total Misplaced Logs: {total_out_of_bound_global}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count datapoints outside the file's time window.")
    parser.add_argument("file_pattern", type=str, help="Pattern, e.g. 'cpu-load.jsonl-*.gz'")
    
    args = parser.parse_args()
    
    check_file_bounds(args.file_pattern)
