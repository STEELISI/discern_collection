import os
import sys
import shutil
import json
import csv
import re
import argparse
from pathlib import Path

"""
The pruning script which perform data removal based on the configuration.

MIN_DURATION means the minimum length of continuous data stream 
(with gap less than GAP_THRESHOLD) that we considered valid.

Everything invalid would be removed from the dataset base directory, 
and into the backup folder.

To run the pruning, this program also require the output of prune-recon.py, which save the time of running 
the reconnaissance again.

Input:   
    data_dir     Main data directory
    backup_dir   Backup directory
    report_file  Path to the .txt report file
"""

# --- Configuration ---
GAP_THRESHOLD = 15.0
MIN_DURATION = 30 * 60
REQUIRED_FILES = ['cpu-load.csv', 'proc-cpu.csv', 'proc-mem.csv']

# --- REPORT PARSER ---
def parse_report_actions(report_path):
    """
    Parses the report file to find experiments marked as Pruned or Removed.
    Returns two sets: experiments_to_remove, experiments_to_prune
    """
    to_remove = set()
    to_prune = set()
    
    # Regex to capture experiment status lines
    # Example: "  - bnsdq_ozxfs_bqwrd_spbau, 🔴 Removed (...)"
    # Example: "  - bnsdq_exp_name, 🟡 Pruned (...)"
    line_pattern = re.compile(r"^\s*-\s*([\w\d_]+),\s*([🔴🟡🟢])\s*(Removed|Pruned|Clean)")
    
    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            for line in f:
                match = line_pattern.search(line)
                if match:
                    exp_name = match.group(1)
                    status = match.group(3) # Removed / Pruned / Clean
                    
                    if status == "Removed":
                        to_remove.add(exp_name)
                    elif status == "Pruned":
                        to_prune.add(exp_name)
                        
    except Exception as e:
        print(f"Error reading report file: {e}")
        sys.exit(1)
        
    return to_remove, to_prune

# --- TIMELINE LOGIC (Reused for Pruning) ---
def get_file_timeline(filepath):
    timestamps = []
    if not os.path.exists(filepath): return []
    try:
        is_jsonl = False
        with open(filepath, 'r') as f:
            first = f.readline().strip()
            if not first: return []
            try:
                json.loads(first)
                is_jsonl = True
            except: is_jsonl = False

        with open(filepath, 'r') as f:
            if is_jsonl:
                for line in f:
                    if not line.strip(): continue
                    try:
                        d = json.loads(line)
                        if 'TimeStamp' in d: timestamps.append(float(d['TimeStamp']))
                        elif 'timestamp' in d: timestamps.append(float(d['timestamp']))
                    except: continue
            else:
                reader = csv.reader(f)
                header = next(reader, None)
                ts_idx = -1
                if header:
                    for i, c in enumerate(header):
                        if 'time' in c.lower(): ts_idx = i; break
                if ts_idx != -1:
                    for row in reader:
                        try:
                            if row: timestamps.append(float(row[ts_idx]))
                        except: continue
    except: return []

    if not timestamps: return []
    timestamps.sort()
    
    intervals = []
    curr = timestamps[0]
    last = timestamps[0]
    for t in timestamps[1:]:
        if (t - last) > GAP_THRESHOLD:
            intervals.append((curr, last))
            curr = t
        last = t
    intervals.append((curr, last))
    return intervals

def intersect_intervals(intervals_a, intervals_b):
    result = []
    i, j = 0, 0
    while i < len(intervals_a) and j < len(intervals_b):
        start = max(intervals_a[i][0], intervals_b[j][0])
        end = min(intervals_a[i][1], intervals_b[j][1])
        if start < end: result.append((start, end))
        if intervals_a[i][1] < intervals_b[j][1]: i += 1
        else: j += 1
    return result

def intersect_multiple(timelines_list):
    if not timelines_list: return []
    curr = timelines_list[0]
    for other in timelines_list[1:]:
        curr = intersect_intervals(curr, other)
        if not curr: return []
    return curr

def calculate_pruning_intervals(exp_path):
    """Calculates the global valid intervals for a given experiment."""
    node_timelines = []
    try:
        items = os.listdir(exp_path)
    except: return []

    for item in items:
        node_path = os.path.join(exp_path, item)
        if os.path.isdir(node_path):
            has_files = any(os.path.exists(os.path.join(node_path, f)) for f in REQUIRED_FILES)
            if not has_files: continue

            node_file_timelines = []
            files_missing = False
            for fname in REQUIRED_FILES:
                tl = get_file_timeline(os.path.join(node_path, fname))
                if not tl: 
                    files_missing = True; break
                node_file_timelines.append(tl)
            
            if files_missing: return [] # Invalid node = Invalid exp
            
            node_valid = intersect_multiple(node_file_timelines)
            if not node_valid: return []
            node_timelines.append(node_valid)

    if not node_timelines: return []
    global_valid = intersect_multiple(node_timelines)
    
    # Filter Duration
    final = []
    for s, e in global_valid:
        if (e - s) >= MIN_DURATION: final.append((s, e))
    return final

# --- ACTION FUNCTIONS ---

def prune_file(filepath, valid_intervals):
    """Rewrites a single file keeping only rows inside valid_intervals."""
    temp_path = filepath + ".tmp"
    try:
        # Detect Format
        is_jsonl = False
        with open(filepath, 'r') as f:
            try:
                first = f.readline().strip()
                if first:
                    json.loads(first)
                    is_jsonl = True
            except: pass

        with open(filepath, 'r') as fin, open(temp_path, 'w') as fout:
            reader, writer = None, None
            ts_idx = -1
            
            if not is_jsonl:
                # Reset pointer
                fin.seek(0)
                reader = csv.reader(fin)
                writer = csv.writer(fout)
                header = next(reader, None)
                if header:
                    writer.writerow(header)
                    for i, c in enumerate(header):
                        if 'time' in c.lower(): ts_idx = i; break
                if ts_idx == -1: return # Cannot process CSV without timestamp
            else:
                fin.seek(0)

            iterator = reader if not is_jsonl else fin
            
            for item in iterator:
                try:
                    ts = None
                    line_write = None
                    if is_jsonl:
                        if not item.strip(): continue
                        d = json.loads(item)
                        if 'TimeStamp' in d: ts = float(d['TimeStamp'])
                        elif 'timestamp' in d: ts = float(d['timestamp'])
                        line_write = item
                    else:
                        if item:
                            ts = float(item[ts_idx])
                            line_write = item
                    
                    if ts is not None:
                        is_valid = False
                        for s, e in valid_intervals:
                            if s <= ts <= e:
                                is_valid = True
                                break
                        
                        if is_valid:
                            if is_jsonl: fout.write(line_write)
                            else: writer.writerow(line_write)
                except: continue
                
        os.replace(temp_path, filepath)
    except Exception as e:
        print(f"    Error pruning {os.path.basename(filepath)}: {e}")
        if os.path.exists(temp_path): os.remove(temp_path)

def process_removal(exp_name, data_dir, backup_dir):
    src = os.path.join(data_dir, exp_name)
    dst = os.path.join(backup_dir, exp_name)
    
    if not os.path.exists(src): return
    
    print(f"  [MOVE] {exp_name} -> Backup")
    if os.path.exists(dst):
        print(f"    Warning: {exp_name} already exists in backup. Skipping move.")
        return
        
    try:
        shutil.move(src, dst)
    except Exception as e:
        print(f"    Error moving folder: {e}")

def process_pruning(exp_name, data_dir, backup_dir):
    src = os.path.join(data_dir, exp_name)
    dst = os.path.join(backup_dir, exp_name)
    
    if not os.path.exists(src): return
    
    print(f"  [PRUNE] {exp_name}")
    
    # 1. Calculate Intervals FIRST (Before modification)
    intervals = calculate_pruning_intervals(src)
    if not intervals:
        print(f"    Error: Pruning calculation returned 0 valid segments. Moving to backup instead.")
        process_removal(exp_name, data_dir, backup_dir)
        return

    # 2. Copy original to backup
    if not os.path.exists(dst):
        print("    -> Backing up original...")
        try:
            shutil.copytree(src, dst)
        except Exception as e:
            print(f"    Backup failed: {e}. Aborting pruning.")
            return
    else:
        print("    -> Backup already exists, proceeding to prune main copy...")

    # 3. Apply changes to MAIN directory
    print(f"    -> Applying prune ({len(intervals)} segments)...")
    for item in os.listdir(src):
        node_path = os.path.join(src, item)
        if os.path.isdir(node_path):
            for fname in os.listdir(node_path):
                if fname.endswith('.csv'):
                    prune_file(os.path.join(node_path, fname), intervals)

# --- MAIN ---
def main():
    parser = argparse.ArgumentParser(description="Execute cleaning based on report.")
    parser.add_argument("data_dir", help="Main data directory")
    parser.add_argument("backup_dir", help="Backup directory")
    parser.add_argument("report_file", help="Path to the .txt report file")
    args = parser.parse_args()

    if not os.path.exists(args.report_file):
        print("Report file not found.")
        sys.exit(1)
        
    if not os.path.exists(args.backup_dir):
        os.makedirs(args.backup_dir)

    print("Parsing Report...")
    to_remove, to_prune = parse_report_actions(args.report_file)
    
    print(f"Found {len(to_remove)} experiments to REMOVE.")
    print(f"Found {len(to_prune)} experiments to PRUNE.")
    print("="*60)
    
    # Process Removals (Fastest)
    if to_remove:
        print("\n--- Processing Removals ---")
        for exp in to_remove:
            process_removal(exp, args.data_dir, args.backup_dir)
            
    # Process Pruning (Slower)
    if to_prune:
        print("\n--- Processing Pruning ---")
        for exp in to_prune:
            process_pruning(exp, args.data_dir, args.backup_dir)

    print("\nExecution Complete.")

if __name__ == "__main__":
    main()
