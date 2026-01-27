import os
import json
import csv
import argparse
from collections import defaultdict

"""
This program is a pre-prune visualizer, mainly denotes how many
realizations, topologies would be impacted when the pruning is completed.

Configuration below could be adjusted for the level of pruning wanted. 

MIN_DURATION means the minimum length of continuous data stream 
(with gap less than GAP_THRESHOLD) that we considered valid
"""

# --- Configuration ---
GAP_THRESHOLD = 15.0       # Seconds (Gap definition)
MIN_DURATION = 30 * 60     # Seconds (Minimum valid segment length)
IGNORE_FOLDERS = {'tools', 'errorfolder', '.git', '.DS_Store', 'archive','backup'}

REQUIRED_FILES = [
    'cpu-load.csv',
    'proc-cpu.csv',
    'proc-mem.csv'
]

# --- PARSING & MATH FUNCTIONS ---

def get_file_timeline(filepath):
    """
    Parses a file (JSONL or CSV) and returns intervals where data is continuous.
    Auto-detects format based on first line.
    """
    timestamps = []
    
    if not os.path.exists(filepath):
        return []

    try:
        # 1. Determine Format
        is_jsonl = False
        with open(filepath, 'r') as f:
            first_line = f.readline().strip()
            if not first_line:
                return [] # Empty file
            try:
                json.loads(first_line)
                is_jsonl = True
            except json.JSONDecodeError:
                is_jsonl = False

        # 2. Parse based on format
        with open(filepath, 'r') as f:
            if is_jsonl:
                for line in f:
                    if not line.strip(): continue
                    try:
                        data = json.loads(line)
                        # Handle different casing if necessary
                        if 'TimeStamp' in data:
                            timestamps.append(float(data['TimeStamp']))
                        elif 'timestamp' in data:
                            timestamps.append(float(data['timestamp']))
                    except (json.JSONDecodeError, KeyError, ValueError):
                        continue
            
            else: # CSV Mode
                reader = csv.reader(f)
                header = next(reader, None)
                ts_idx = -1
                
                if header:
                    for i, col in enumerate(header):
                        # Robust check for "timestamp" or "time"
                        if 'time' in col.lower():
                            ts_idx = i
                            break
                
                if ts_idx != -1:
                    for row in reader:
                        try:
                            if row: 
                                val = float(row[ts_idx])
                                timestamps.append(val)
                        except (ValueError, IndexError):
                            continue

    except Exception:
        return []

    if not timestamps:
        return []

    timestamps.sort()
    
    # 3. Build Intervals
    intervals = []
    current_start = timestamps[0]
    last_time = timestamps[0]
    
    for t in timestamps[1:]:
        if (t - last_time) > GAP_THRESHOLD:
            intervals.append((current_start, last_time))
            current_start = t
        last_time = t
    
    intervals.append((current_start, last_time))
    return intervals

def intersect_intervals(intervals_a, intervals_b):
    """Mathematical Intersection of two timelines."""
    result = []
    i, j = 0, 0
    while i < len(intervals_a) and j < len(intervals_b):
        a_start, a_end = intervals_a[i]
        b_start, b_end = intervals_b[j]
        
        start = max(a_start, b_start)
        end = min(a_end, b_end)
        
        if start < end:
            result.append((start, end))
        
        if a_end < b_end:
            i += 1
        else:
            j += 1
    return result

def intersect_multiple(timelines_list):
    """Intersect a list of timelines (A ∩ B ∩ C)."""
    if not timelines_list: return []
    current = timelines_list[0]
    for other in timelines_list[1:]:
        current = intersect_intervals(current, other)
        if not current: return []
    return current

# --- HEALTH ANALYSIS ---

def analyze_experiment_health(exp_path):
    node_timelines = []
    
    try:
        sub_items = os.listdir(exp_path)
    except OSError:
        return 'removed', "Cannot access folder"

    # 1. GATHER NODE TIMELINES (Local Intersection)
    for item in sub_items:
        node_path = os.path.join(exp_path, item)
        if os.path.isdir(node_path):
            
            # Check if this is a valid node folder (must contain at least one heartbeat file)
            has_files = any(os.path.exists(os.path.join(node_path, f)) for f in REQUIRED_FILES)
            if not has_files:
                continue 

            # Check ALL required files for this node
            node_file_timelines = []
            files_missing = False
            
            for filename in REQUIRED_FILES:
                fpath = os.path.join(node_path, filename)
                timeline = get_file_timeline(fpath) # Auto-detects format now
                
                if not timeline:
                    # If Node A is missing 'proc-mem', Node A is invalid.
                    files_missing = True 
                    break
                node_file_timelines.append(timeline)
            
            if files_missing:
                return 'removed', f"Node '{item}' incomplete/unparseable data"

            # Node A Valid Time = CPU ∩ Mem ∩ Proc
            node_valid_timeline = intersect_multiple(node_file_timelines)
            
            if not node_valid_timeline:
                return 'removed', f"Node '{item}' local files mismatch"

            node_timelines.append(node_valid_timeline)
    
    if not node_timelines:
        return 'removed', "No valid node folders found"

    # 2. CALCULATE EXPERIMENT TIMELINE (Global Intersection)
    global_valid = intersect_multiple(node_timelines)

    if not global_valid:
        return 'removed', "Nodes have ZERO overlapping synchronization"

    # 3. DURATION CHECK
    final_intervals = []
    discarded_count = 0
    
    for start, end in global_valid:
        duration = end - start
        if duration >= MIN_DURATION:
            final_intervals.append((start, end))
        else:
            discarded_count += 1

    # 4. REPORT STATUS
    if not final_intervals:
        return 'removed', f"All global overlaps < 30min (Discarded {discarded_count})"
    
    if discarded_count > 0:
        return 'pruned', f"Discard {discarded_count} short segments from Global Timeline"
    
    if len(final_intervals) > 1:
        return 'pruned', f"Split into {len(final_intervals)} segments (Global Sync Gap)"

    return 'clean', "Continuous Global Sync > 30min"

# --- MAIN SCANNER ---

def scan_and_report(root_dir):
    if not os.path.exists(root_dir):
        print(f"Error: Directory '{root_dir}' does not exist.")
        return

    topology_groups = defaultdict(list)

    print(f"Scanning directory: {root_dir}")
    print("Logic: Auto-Detect JSON/CSV -> Intersect(Cpu, Mem, Proc) -> Intersect(Nodes)")
    print("-" * 60)
    
    try:
        items = os.listdir(root_dir)
    except OSError as e:
        print(f"Error accessing directory: {e}")
        return

    for experiment_name in items:
        if experiment_name in IGNORE_FOLDERS or experiment_name.startswith('.'):
            continue
        experiment_path = os.path.join(root_dir, experiment_name)
        if not os.path.isdir(experiment_path): continue

        # Identify Topology
        node_names = []
        try:
            sub_items = os.listdir(experiment_path)
            for item in sub_items:
                item_path = os.path.join(experiment_path, item)
                if os.path.isdir(item_path) and not item.startswith('.'):
                    if item.endswith('-data'): clean_name = item[:-5]
                    else: clean_name = item
                    node_names.append(clean_name)
        except OSError: continue

        if not node_names: signature = ("<Empty>",)
        else: signature = tuple(sorted(node_names))

        # Analyze Health
        status, details = analyze_experiment_health(experiment_path)

        topology_groups[signature].append({
            'name': experiment_name,
            'status': status,
            'details': details
        })

    # Output
    print("="*60)
    print(f"TOTAL UNIQUE TOPOLOGIES: {len(topology_groups)}")
    print("="*60)
    
    sorted_groups = sorted(topology_groups.items(), key=lambda x: len(x[1]), reverse=True)
    status_order = {'clean': 0, 'pruned': 1, 'removed': 2}

    for i, (nodes, experiments) in enumerate(sorted_groups, 1):
        total = len(experiments)
        n_clean = sum(1 for e in experiments if e['status'] == 'clean')
        n_pruned = sum(1 for e in experiments if e['status'] == 'pruned')
        n_removed = sum(1 for e in experiments if e['status'] == 'removed')

        experiments.sort(key=lambda x: (status_order[x['status']], x['name']))

        print(f"\nTOPOLOGY GROUP #{i}")
        print(f"Nodes: [{', '.join(nodes)}]")
        print(f"Stats: {n_clean} Clean | {n_pruned} Pruned | {n_removed} Removed (Total {total})")
        print("Experiments:")
        for exp in experiments:
            icon = "⚪"
            if exp['status'] == 'clean': icon = "🟢 Clean"
            elif exp['status'] == 'pruned': icon = "🟡 Pruned"
            elif exp['status'] == 'removed': icon = "🔴 Removed"
            print(f"  - {exp['name']}, {icon} ({exp['details']})")
        print("-" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Path to data directory")
    args = parser.parse_args()
    scan_and_report(args.path)
