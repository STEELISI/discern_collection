import json
import csv
import argparse
from pathlib import Path
from collections import defaultdict

"""
Reads JSON stream in small batches and immediately appends to the correct CSV based on DevID.

Path Structure: ./[suffix_path]/[prefix]-data/file.csv
Example: client.a.b.c.d -> ./a_b_c_d/client-data/file.csv
"""


# How many lines to process in RAM before writing to disk
BATCH_SIZE = 5000

def get_val_robust(data, key, default='N/A'):
    """
    Safely extracts a value whether it is a single item or a list.
    """
    val = data.get(key)
    if val is None:
        return default
    
    # If it turns out to be a list, grab the first item
    if isinstance(val, list):
        return val[0] if len(val) > 0 else default
    
    return val

def flush_buffer(buffer, initialized_files):
    """
    Writes all buffered rows to their respective CSV files.
    """
    for file_path_str, rows in buffer.items():
        if not rows:
            continue
            
        output_path = Path(file_path_str)
        
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Header Logic
        write_header = False
        if file_path_str not in initialized_files:
            if not output_path.exists():
                write_header = True
            initialized_files.add(file_path_str)
            
        # Write Append
        try:
            with open(output_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(['timestamp', 'device_id', 'location', 'size', 'hash', 'owner', 'group'])
                writer.writerows(rows)
        except IOError as e:
            print(f"Error writing to {output_path}: {e}")

    # Clear memory
    buffer.clear()

def process_file_changes(input_file):
    buffer = defaultdict(list)
    initialized_files = set()
    line_count = 0

    print(f"Processing: {input_file}")

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                
                try:
                    log_entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # --- 1. Routing Logic ---
                dev_id = log_entry.get('DevID', 'unknown')
                parts = dev_id.split('.')
                
                if len(parts) >= 2:
                    folder_sub = f"{parts[0]}-data"
                    folder_base = "_".join(parts[1:])
                else:
                    folder_base = "unknown_device_group"
                    folder_sub = f"{dev_id}-data"

                output_file = Path(folder_base) / folder_sub / "file.csv"
                output_path_str = str(output_file)

                # --- 2. Extract Data ---
                # Using robust extraction to handle your specific data quirks
                timestamp = log_entry.get('TimeStamp', '0')
                location = get_val_robust(log_entry, 'Location')
                size = log_entry.get('Size', 0)
                file_hash = log_entry.get('Hash', 'N/A')
                owner = log_entry.get('Owner', 'N/A') # Flat structure per your latest sample
                group = log_entry.get('Group', 'N/A')

                row = [timestamp, dev_id, location, size, file_hash, owner, group]
                
                # --- 3. Buffer ---
                buffer[output_path_str].append(row)
                line_count += 1

                # Flush every BATCH_SIZE lines
                if line_count >= BATCH_SIZE:
                    flush_buffer(buffer, initialized_files)
                    line_count = 0

        # --- 4. Final Flush ---
        flush_buffer(buffer, initialized_files)
        print("Done.")

    except FileNotFoundError:
        print(f"Error: Input file not found: {input_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process File Change JSON logs to CSV.")
    parser.add_argument("input_file", type=Path, help="Path to input .json/.jsonl file")
    args = parser.parse_args()
    
    process_file_changes(args.input_file)
