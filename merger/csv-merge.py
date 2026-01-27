import os
import csv
import itertools
import random
import sys
import shutil

# --- CONFIGURATION ---
IGNORE_COLUMNS_MAP = {
    "proc-mem.csv": {"name", "cpu"},
    "proc-new.csv": {"name", "cpu"},
    "proc-cpu.csv": {"name", "cpu"},
    "network.csv": set(),
    "cpu-load.csv": set(),
    "interfaces.csv": set(),
    "file.csv": set()
}

FILE_MAPPINGS = {
    "cpu-load.csv": ["cpu-load.csv", "cpu-load-res.csv"],
    "file.csv": ["file.csv", "file-res.csv"],
    "network.csv": ["network.csv", "network-res.csv"],
    "proc-cpu.csv": ["proc-cpu.csv", "proc-cpu-res.csv"],
    "proc-mem.csv": ["proc-mem.csv", "proc-mem-res.csv"],
    "proc-new.csv": ["proc-new.csv", "proc-new-res.csv"],
    "interfaces.csv": ["interfaces.csv", "interface.csv", "interfaces-res.csv", "interface-res.csv"]
}

# --- HELPER FUNCTIONS ---

def get_csv_headers(filepath):
    try:
        with open(filepath, 'r', newline='') as f:
            reader = csv.reader(f)
            headers = next(reader)
            return [h.strip() for h in headers if h.strip()]
    except: return []

def generate_unified_dev_id(file1_path, file2_path, default_id="unified.dev.id"):
    id1, id2 = None, None
    for fpath in [file1_path, file2_path]:
        current_id = None
        try:
            with open(fpath, 'r', newline='') as f:
                reader = csv.DictReader(f)
                row = next(reader)
                if 'device_id' in row: current_id = row['device_id']
        except: pass
        if fpath == file1_path: id1 = current_id
        else: id2 = current_id

    if not id1 or not id2: return default_id
    parts1, parts2 = id1.split('.'), id2.split('.')
    new_parts = []
    for i in range(min(len(parts1), len(parts2))):
        if parts1[i] == parts2[i]: new_parts.append(parts1[i])
        else: new_parts.append(f"{parts1[i]}-{parts2[i]}")
    return ".".join(new_parts)

def get_write_headers(file1, file2, filename):
    h1 = get_csv_headers(file1)
    h2 = get_csv_headers(file2)
    base_name = filename.replace('-res.csv', '.csv')
    if "interface" in base_name: base_name = "interfaces.csv"
    ignore = IGNORE_COLUMNS_MAP.get(base_name, set())

    if "cpu-load" in filename:
        combined = set(h1).union(set(h2)) - ignore
        sorted_cols = sorted(list(combined))
        for priority in ['device_id', 'device', 'timestamp']:
            if priority in sorted_cols:
                sorted_cols.insert(0, sorted_cols.pop(sorted_cols.index(priority)))
        return sorted_cols
    else:
        common = set(h1).intersection(set(h2)) - ignore
        return [h for h in h1 if h in common]

def get_timespan_from_file(filepath):
    first_ts, last_ts = None, None
    try:
        with open(filepath, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'timestamp' in row and row['timestamp']:
                    first_ts = float(row['timestamp'])
                    break
        if first_ts is None: return None

        with open(filepath, 'r', newline='') as f:
            f.seek(0, os.SEEK_END)
            f.seek(max(0, f.tell() - 10000), os.SEEK_SET)
            lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                parts = last_line.split(',')
                if parts[0].replace('.','',1).isdigit():
                    last_ts = float(parts[0])
        
        if last_ts is None:
             with open(filepath, 'r', newline='') as f:
                 reader = csv.DictReader(f)
                 for row in reader: 
                    if 'timestamp' in row: last_ts = float(row['timestamp'])

        if first_ts is not None and last_ts is not None:
            return first_ts, last_ts, last_ts - first_ts
    except: return None
    return None

# --- CORE PROCESSING LOGIC ---

def finetune_cpu_csv(all_data, file1, file2, start_time, end_time, fieldnames, unified_id):
    data_before, data_during, data_after = [], [], []
    
    # Partition based on the calculated Malicious Window
    for row in all_data:
        try:
            ts = float(row['timestamp'])
            if ts < start_time: data_before.append(row)
            elif start_time <= ts <= end_time: data_during.append(row)
            else: data_after.append(row)
        except: continue

    data_during.sort(key=lambda x: float(x['timestamp']))
    processed_during = []
    load_cols = [f for f in fieldnames if 'load_core' in f]

    idx_start = 0
    if len(data_during) > 2:
        t0 = float(data_during[0]['timestamp'])
        t1 = float(data_during[1]['timestamp'])
        t2 = float(data_during[2]['timestamp'])
        if (t1 - t0) > (t2 - t1):
            idx_start = 1
            data_before.append(data_during[0])

    for i in range(idx_start, len(data_during) - 1, 2):
        item1 = data_during[i]
        item2 = data_during[i+1]
        try:
            t1 = float(item1['timestamp'])
            t2 = float(item2['timestamp'])
            avg_ts = (t1 + t2) / 2
            
            merged = {}
            for k in fieldnames:
                val = item1.get(k)
                if val is None or str(val).strip() == "":
                    merged[k] = "N/A"
                else:
                    merged[k] = str(val)

            merged['timestamp'] = str(avg_ts)
            if 'device_id' in merged: merged['device_id'] = unified_id
            
            for col in load_cols:
                val1_str = str(item1.get(col, "0"))
                val2_str = str(item2.get(col, "0"))
                val1 = 0.0 if val1_str == "N/A" else (float(val1_str) if val1_str.replace('.','',1).isdigit() else 0.0)
                val2 = 0.0 if val2_str == "N/A" else (float(val2_str) if val2_str.replace('.','',1).isdigit() else 0.0)
                merged[col] = min(val1 + val2, 100.0)
            
            m1 = int(item1.get('malicious', 0))
            m2 = int(item2.get('malicious', 0))
            merged['malicious'] = 1 if (m1 or m2) else 0

            processed_during.append(merged)
        except Exception as e: continue
        
    return data_before + processed_during + data_after

def process_csv_pair(file1, file2, output_path, golden_offset, times, filename):
    fieldnames = get_write_headers(file1, file2, filename)
    if not fieldnames: return

    write_fields = fieldnames + ['malicious'] if 'malicious' not in fieldnames else fieldnames
    all_rows = []
    unified_dev_id = generate_unified_dev_id(file1, file2)
    
    def read_and_shift(fpath, time_delta, is_malicious=False):
        rows = []
        try:
            with open(fpath, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    clean = {}
                    for k in fieldnames:
                        val = row.get(k)
                        if "cpu-load" in filename and "load_core" in k:
                            clean[k] = "0.0" if (val is None or str(val).strip() == "" or str(val) == "N/A") else str(val)
                        elif val is None or str(val).strip() == "":
                            clean[k] = "N/A"
                        else:
                            clean[k] = str(val)
                    
                    if 'timestamp' in clean and clean['timestamp'] != "N/A":
                        try:
                            ts = float(clean['timestamp'])
                            clean['timestamp'] = str(ts + time_delta)
                        except: clean['timestamp'] = "N/A"
                    
                    if 'device_id' in clean: clean['device_id'] = unified_dev_id
                    clean['malicious'] = 1 if is_malicious else 0
                    rows.append(clean)
        except Exception as e: print(f"    [ERR] Reading {fpath}: {e}")
        return rows

    all_rows.extend(read_and_shift(file1, 0, is_malicious=False))
    all_rows.extend(read_and_shift(file2, golden_offset, is_malicious=True))

    if "cpu-load" in filename and all_rows:
        mal_start_merged = times['target_injection_time']
        mal_end_merged = mal_start_merged + times['s2_span']
        all_rows = finetune_cpu_csv(all_rows, file1, file2, mal_start_merged, mal_end_merged, write_fields, unified_dev_id)

    all_rows.sort(key=lambda x: float(x.get('timestamp', 0) if x.get('timestamp') != "N/A" else 0))

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=write_fields)
        writer.writeheader()
        writer.writerows(all_rows)

def resolve_file_path(folder, possible_names):
    for name in possible_names:
        path = os.path.join(folder, name)
        if os.path.exists(path): return path
    return None

def process_and_combine_data(folder1, folder2, output_folder, golden_offset, times):
    if not os.path.exists(output_folder): os.makedirs(output_folder)

    for output_name, possible_sources in FILE_MAPPINGS.items():
        f1 = resolve_file_path(folder1, possible_sources)
        f2 = resolve_file_path(folder2, possible_sources)
        out = os.path.join(output_folder, output_name)
        
        if not f1 or not f2:
            if f1 and not f2: shutil.copy(f1, out)
            continue

        process_csv_pair(f1, f2, out, golden_offset, times, output_name)

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define Inputs
    # parent1_list = ['../bnsdq_ozxfs_rzona_uxkia/']
    # parent1_list = ['../bnsdq_ozxfs_rzona_nmtny/']
    parent1_list = ['../bnsdq_ozxfs_rzona_lnllj/']

    # parent2_list = ['../synthetic/cryptominer/0']
    # parent2_list = ['../synthetic/exfiltrate/1']
    parent2_list = ['../synthetic/ransomware/2']
    
    base_output_folder = os.path.normpath(os.path.join(script_dir, '../merge/'))
    if not os.path.exists(base_output_folder):
        os.makedirs(base_output_folder, exist_ok=True)

    for parent_folder1, parent_folder2 in zip(parent1_list, parent2_list):
        
        legit_name = os.path.basename(os.path.normpath(parent_folder1))
        mal_path = os.path.normpath(parent_folder2)
        mal_index = os.path.basename(mal_path)                 
        mal_type = os.path.basename(os.path.dirname(mal_path)) 
        batch_folder_name = f"{legit_name}-{mal_type}_{mal_index}"
        
        print(f"\nBatch: {batch_folder_name}")

        if not os.path.exists(parent_folder1) or not os.path.exists(parent_folder2):
            print(f"  [ERR] Folder not found.")
            continue

        subfolders1 = sorted([d for d in os.listdir(parent_folder1) if os.path.isdir(os.path.join(parent_folder1, d))])
        subfolders2 = sorted([d for d in os.listdir(parent_folder2) if os.path.isdir(os.path.join(parent_folder2, d))])

        # --- 1. DETERMINE GLOBAL CONSTRAINTS (Scanning Heartbeat Files) ---
        min_s1_span = float('inf')
        s1_global_start = float('inf') 
        
        # Files to scan for start/end times. 
        scan_files = [
            "cpu-load.csv", "cpu-load-res.csv",
            "proc-mem.csv", "proc-mem-res.csv",
            "proc-cpu.csv", "proc-cpu-res.csv",
            "proc-new.csv", "proc-new-res.csv"
        ]

        for sub in subfolders1:
            for fname in scan_files:
                p = resolve_file_path(os.path.join(parent_folder1, sub), [fname])
                if p:
                    info = get_timespan_from_file(p)
                    if info: 
                        min_s1_span = min(min_s1_span, info[2])
                        s1_global_start = min(s1_global_start, info[0])

        max_s2_span = 0
        s2_global_start = float('inf') 
        
        for sub in subfolders2:
            for fname in scan_files:
                p = resolve_file_path(os.path.join(parent_folder2, sub), [fname])
                if p:
                    info = get_timespan_from_file(p)
                    if info: 
                        max_s2_span = max(max_s2_span, info[2])
                        s2_global_start = min(s2_global_start, info[0])

        if min_s1_span == float('inf') or min_s1_span < max_s2_span:
            print(f"  [SKIP] Invalid time spans (Legit Span: {min_s1_span}, Mal Span: {max_s2_span})")
            continue

        # --- 2. GOLDEN CALCULATION ---
        available_gap = min_s1_span - max_s2_span
        if available_gap < 0:
             print(f"  [SKIP] Malicious data longer than legit data.")
             continue

        injection_relative_offset = int(available_gap * random.uniform(0.0, 1.0))
        target_injection_time = s1_global_start + injection_relative_offset
        
        # Golden Delta uses the absolute earliest malicious heartbeat found
        golden_delta = target_injection_time - s2_global_start
        
        print(f"  Gap: {available_gap}s | Inject at: +{injection_relative_offset}s")
        print(f"  Legit Start: {s1_global_start} | Mal Absolute Start: {s2_global_start}")
        print(f"  Golden Delta: {golden_delta}")

        times_info = {
            'target_injection_time': target_injection_time, 
            's2_span': max_s2_span
        }

        folder_permutations = list(itertools.product(subfolders1, subfolders2))
        
        for sub1, sub2 in folder_permutations:
            src1 = os.path.join(parent_folder1, sub1)
            src2 = os.path.join(parent_folder2, sub2)
            out_path = os.path.join(base_output_folder, batch_folder_name, f"{sub1}_{sub2}")
            
            process_and_combine_data(src1, src2, out_path, golden_delta, times_info)
            
            m_start = target_injection_time
            m_end = m_start + max_s2_span
            with open(os.path.join(out_path, "malicious_time.txt"), 'w') as f:
                f.write(f'{{"malicious_start_time": {m_start}, "malicious_end_time": {m_end}}}')
