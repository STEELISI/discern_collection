import gzip
import json
import os
import argparse
from datetime import datetime, timezone
from collections import Counter

"""
Input:  "cpu-load.jsonl-yyyymmddhhmmss.gz"
Example: python3 count-oob-exp.py path-to-jsonl-file

This script takes in the (before parsed) whole json file;
the range is determined to be 24 hours prior to the time noted in the file name
output the Total Logs Scanned, Out of Bound Logs, and Unique realization affected.
"""

def get_realization_id(dev_id):
    """
    Converts DevID to Realization Name.
    Input:  "asn3.bnsdq.ozxfs.guxnq.fvmxg"
    Output: "bnsdq.ozxfs.guxnq.fvmxg"
    """
    if not dev_id:
        return "unknown"
    
    # Split only on the first dot
    parts = dev_id.split('.', 1)
    
    if len(parts) > 1:
        return parts[1]
    return dev_id # Fallback if no dots exist

def analyze_bounds(filepath):
    filename = os.path.basename(filepath)
    
    # 1. Parse Time Bounds from Filename
    try:
        # Expected: prefix-YYYYMMDDHHMMSS.gz
        date_part = filename.split('-')[-1].replace('.gz', '')
        end_dt = datetime.strptime(date_part, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        end_ts = end_dt.timestamp()
        start_ts = end_ts - 86400 # 24 Hours prior
        
        print(f"Analyzing File: {filename}")
        print(f"Valid Window:   {datetime.fromtimestamp(start_ts, timezone.utc)} to {end_dt}")
        print("-" * 60)
        
    except (ValueError, IndexError):
        print(f"Error: Could not parse timestamp from filename: {filename}")
        print("Expected format like: cpu-load.jsonl-20251121000001.gz")
        return

    # 2. Scan File
    # We use a set to store unique affected realizations
    affected_realizations = set()
    total_out_of_bound_logs = 0
    total_logs = 0

    try:
        if filepath.endswith('.gz'):
            opener = gzip.open(filepath, 'rt', encoding='utf-8')
        else:
            opener = open(filepath, 'r', encoding='utf-8')

        with opener as f:
            for line in f:
                if not line.strip(): continue
                total_logs += 1
                
                try:
                    data = json.loads(line)
                    ts = data.get('TimeStamp')
                    dev_id = data.get('DevID')

                    if ts is None: continue

                    ts_val = float(ts)

                    # 3. Check Bounds
                    if ts_val < start_ts or ts_val > end_ts:
                        # Log is Out of Bound!
                        total_out_of_bound_logs += 1
                        
                        # Extract Realization Name
                        exp_name = get_realization_id(dev_id)
                        affected_realizations.add(exp_name)

                except (ValueError, json.JSONDecodeError):
                    continue

    except Exception as e:
        print(f"Error reading file: {e}")
        return

    # 4. Report Results
    print(f"Total Logs Scanned:       {total_logs}")
    print(f"Out of Bound Logs:        {total_out_of_bound_logs}")
    print(f"Unique Realizations Hit:   {len(affected_realizations)}")
    print("-" * 60)
    
    if affected_realizations:
        print("Top 10 Affected Realizations (Sample):")
        # Sort just to make the list consistent
        for exp in sorted(list(affected_realizations))[:10]:
            print(f" - {exp}")
        
        if len(affected_realizations) > 10:
            print(f"... and {len(affected_realizations) - 10} more.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count unique realizations with out-of-bound data.")
    parser.add_argument("input_file", type=str, help="Path to the .gz or .jsonl file")
    
    args = parser.parse_args()
    
    analyze_bounds(args.input_file)
