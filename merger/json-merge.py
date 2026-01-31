import os
import json
import itertools
import random
import subprocess
import sys

def generate_unified_dev_id(file1_path, file2_path, default_id="unified.dev.id"):
    """
    Reads the first line of two files to parse their DevIDs and merge them.
    """
    dev_id1, dev_id2 = None, None
    try:
        with open(file1_path, 'r') as f:
            dev_id1 = json.loads(f.readline()).get("DevID")
    except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError):
        pass
    try:
        with open(file2_path, 'r') as f:
            dev_id2 = json.loads(f.readline()).get("DevID")
    except (FileNotFoundError, json.JSONDecodeError, KeyError, AttributeError):
        pass

    if not dev_id1 or not dev_id2:
        return default_id

    parts1, parts2 = dev_id1.split('.'), dev_id2.split('.')
    new_parts = []
    for i in range(min(len(parts1), len(parts2))):
        if parts1[i] == parts2[i]:
            new_parts.append(parts1[i])
        else:
            new_parts.append(f"{parts1[i]}-{parts2[i]}")
    return ".".join(new_parts)

def normalize_cpu_cores(data_list, max_cores):
    """Pads the 'Load' array of each record in a list to a specified length."""
    for item in data_list:
        if 'Load' in item and isinstance(item['Load'], list):
            current_cores = len(item['Load'])
            if current_cores < max_cores:
                item['Load'].extend([0.0] * (max_cores - current_cores))
    return data_list

def finetune_cpu_data(all_data, file1_path, file2_path, malicious_start_time, malicious_end_time):
    data_before = []
    data_during = []
    data_after = []

    for item in all_data:
        try:
            timestamp = int(item.get('TimeStamp', 0))
            if timestamp < malicious_start_time:
                data_before.append(item)
            elif malicious_start_time <= timestamp <= malicious_end_time:
                data_during.append(item)
            else:
                data_after.append(item)
        except (ValueError, TypeError):
            continue
        
    max_cores = 1
    for item in all_data:
        load = item.get('Load', [])
        if isinstance(load, list):
            max_cores = max(max_cores, len(load))

    data_before = normalize_cpu_cores(data_before, max_cores)
    data_after = normalize_cpu_cores(data_after, max_cores)

    data_during.sort(key=lambda x: int(x.get('TimeStamp', 0)))

    unified_dev_id = generate_unified_dev_id(file1_path, file2_path)
    processed_during_data = []
    start = 0
    if len(data_during) > 2 and (int(data_during[1]['TimeStamp']) - int(data_during[0]['TimeStamp'] ) > int(data_during[2]['TimeStamp']) - int(data_during[1]['TimeStamp'])):
        start = 1
        data_before.append(data_during[0])

    for i in range(start, len(data_during) - 1, 2):
        item1 = data_during[i]
        item2 = data_during[i+1]

        try:
            ts1 = int(item1['TimeStamp'])
            ts2 = int(item2['TimeStamp'])
            avg_timestamp = str(int((ts1 + ts2) / 2))

            load1 = item1.get('Load', [])
            load2 = item2.get('Load', [])
            
            summed_load = [min(x + y, 100.0) for x, y in itertools.zip_longest(load1, load2, fillvalue=0.0)]

            merged_record = {
                'TimeStamp': avg_timestamp,
                'Load': summed_load,
                'DevID': unified_dev_id,
                'malicious': item1.get('malicious', 0) or item2.get('malicious', 0)
            }
            processed_during_data.append(merged_record)

        except (KeyError, TypeError, ValueError):
            continue
    
    return data_before + processed_during_data + data_after

def find_earliest_timestamp(filepath):
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    return int(data["TimeStamp"])
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    return float('inf')

def get_timespan_from_file(filepath):
    first_ts, last_ts = None, None
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if line.strip():
                    first_ts = int(json.loads(line)['TimeStamp'])
                    break
            
            if first_ts is None: return None

            f.seek(0, os.SEEK_END)
            buffer_size = min(8192, f.tell())
            f.seek(f.tell() - buffer_size, os.SEEK_SET)
            buffer = f.read(buffer_size)
            
            lines = buffer.strip().split('\n')
            if lines and lines[-1]:
                 last_ts = int(json.loads(lines[-1])['TimeStamp'])
            else: 
                f.seek(0)
                all_lines = [l for l in f.readlines() if l.strip()]
                if all_lines:
                    last_ts = int(json.loads(all_lines[-1])['TimeStamp'])

        if first_ts is not None and last_ts is not None:
            return first_ts, last_ts, last_ts - first_ts
            
    except (FileNotFoundError, json.JSONDecodeError, KeyError, IndexError):
        return None
    return None

def process_network_data(file1_path, file2_path,s1_start,s2_start,time_offset_file1, time_offset_file2):
    print("Running isolated processing for network-data.txt...")
    all_data = []
    unified_dev_id = generate_unified_dev_id(file1_path, file2_path)
    
    # Process Legitimate
    try:
        with open(file1_path, 'r') as f:
            for line in f:
                 if not line.strip(): continue
                 try:
                    data = json.loads(line)
                    data["TimeStamp"] = str(int(data["TimeStamp"]) + time_offset_file1)
                    data["malicious"] = 0
                    if "DevID" in data:
                        data["DevID"] = unified_dev_id
                    if "Packets" in data and isinstance(data["Packets"], list):
                            for i, packet in enumerate(data["Packets"]):
                                data["Packets"][i]["TimeStamp"] = str(int(data["Packets"][i]["TimeStamp"]) + time_offset_file1)
                    all_data.append(data)
                 except (json.JSONDecodeError, KeyError):
                    pass
    except FileNotFoundError:
        print(f"Warning: File not found: {file1_path}")

    # Process Malicious (CORRECTED: Simple Addition)
    try:
        with open(file2_path, 'r') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    # FIX: Just add the Golden Offset
                    data["TimeStamp"] = str(int(data["TimeStamp"]) + time_offset_file2)
                    data["malicious"] = 1
                    
                    if "Packets" in data and isinstance(data["Packets"], list):
                        for i, packet in enumerate(data["Packets"]):
                            # FIX: Just add the Golden Offset
                            data["Packets"][i]["TimeStamp"] = str(int(data["Packets"][i]["TimeStamp"]) + time_offset_file2)
                    all_data.append(data)
                except (json.JSONDecodeError, KeyError):
                    pass
    except FileNotFoundError:
        print(f"Warning: File not found: {file2_path}")
            
    return all_data

def process_file_data(file1_path,file2_path,s1_start,s2_start, time_offset_file1, time_offset_file2):
    all_data = []
    # FIX: Do NOT use find_earliest_timestamp(file1_path) here. It causes the 'inf' crash.
    # We rely on 's1_start' passed from main for the legitimate start time.
    
    unified_dev_id = generate_unified_dev_id(file1_path, file2_path)
    local_min = find_earliest_timestamp(file2_path) # Safe to keep for file2 logic check
    
    try:
        with open(file1_path, 'r') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    data["TimeStamp"] = str(int(data["TimeStamp"]) + time_offset_file1)
                    data["malicious"] = 0
                    if "DevID" in data:
                        data["DevID"] = unified_dev_id
                    all_data.append(data)
                except (json.JSONDecodeError, KeyError):
                    pass
    except FileNotFoundError:
        pass

    try:
        with open(file2_path, 'r') as f:
            # We already got local_min above, just reset pointer
            f.seek(0) 

            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    # Handle File Initialization Event
                    if data["TimeStamp"] == str(local_min):
                        # Use s1_start (passed from main) instead of recalculating
                        data["TimeStamp"] = str(s1_start + time_offset_file1)
                    else:
                        # FIX: Just add the Golden Offset
                        data["TimeStamp"] = str(int(data["TimeStamp"]) + time_offset_file2)
                    
                    data["malicious"] = 1
                    if "DevID" in data:
                        data["DevID"] = unified_dev_id
                    all_data.append(data)
                except (json.JSONDecodeError, KeyError):
                    pass
    except FileNotFoundError:
        pass
        
    return all_data

def run_csv_converter(input_file_path):
    filename = os.path.basename(input_file_path)
    data_type = filename.replace("-data.txt", "")
    script_dir = os.path.dirname(__file__)
    script_path = os.path.join(script_dir, '..', 'analyze', 'convert-to-csv', f"csv-{data_type}.py")

    if not os.path.exists(script_path):
        return

    output_csv_path = os.path.join(os.path.dirname(input_file_path), f"{data_type}-res.csv")
    command = [sys.executable, script_path, input_file_path, "-o", output_csv_path]
    
    print(f"  > Running CSV converter for {filename}...")
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"  > Error running CSV converter for {filename}: {e}")

def process_and_combine_data(folder1, folder2, output_folder,s1_start, s2_start, time_offset_file1, time_offset_file2,malicious_start_time,malicious_end_time):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    common_files = sorted([
        "cpu-load-data.txt", "file-data.txt", "network-data.txt",
        "proc-cpu-data.txt", "proc-mem-data.txt", "proc-new-data.txt", "interfaces-data.txt"
    ])

    for filename in common_files:
        print(f"--- Processing {filename} ---")
        file1_path = os.path.join(folder1, filename)
        file2_path = os.path.join(folder2, filename)
        output_file_path = os.path.join(output_folder, filename)

        all_data = []

        if filename == "network-data.txt":
            all_data = process_network_data(file1_path, file2_path,s1_start,s2_start,time_offset_file1, time_offset_file2)
        elif filename == "file-data.txt":
            all_data = process_file_data(file1_path,file2_path, s1_start,s2_start,time_offset_file1, time_offset_file2)
        else:
            unified_dev_id = generate_unified_dev_id(file1_path, file2_path)
            try:
                with open(file1_path, 'r') as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            data = json.loads(line)
                            data["TimeStamp"] = str(int(data["TimeStamp"]) + time_offset_file1)
                            data["malicious"] = 0
                            if "DevID" in data:
                                data["DevID"] = unified_dev_id
                            all_data.append(data)
                        except (json.JSONDecodeError, KeyError):
                            pass
            except FileNotFoundError:
                pass

            try:
                with open(file2_path, 'r') as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            data = json.loads(line)
                            # FIX: Just add the Golden Offset
                            data["TimeStamp"] = str(int(data["TimeStamp"]) + time_offset_file2)
                            data["malicious"] = 1
                            if "DevID" in data:
                                data["DevID"] = unified_dev_id
                            all_data.append(data)
                        except (json.JSONDecodeError, KeyError):
                            pass
            except FileNotFoundError:
                pass

            if filename == "cpu-load-data.txt" and all_data:
                all_data = finetune_cpu_data(all_data, file1_path, file2_path,malicious_start_time,malicious_end_time )
        
        if not all_data:
            open(output_file_path, 'w').close()
            continue
        
        all_data.sort(key=lambda x: int(x['TimeStamp']))

        with open(output_file_path, 'w') as f:
            for data in all_data:
                f.write(json.dumps(data) + '\n')

        run_csv_converter(output_file_path)

if __name__ == "__main__":
    

    parent1_list = [
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/0/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/0/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/0/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/0/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/0/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/0/',
        '../../DISCERN_data/synthetic/legitimate/llm/0/',
        '../../DISCERN_data/synthetic/legitimate/llm/0/',
        '../../DISCERN_data/synthetic/legitimate/llm/0/',
        '../../DISCERN_data/synthetic/legitimate/llm/0/',
        '../../DISCERN_data/synthetic/legitimate/llm/0/',
        '../../DISCERN_data/synthetic/legitimate/llm/0/',
        '../../DISCERN_data/synthetic/legitimate/svm/0/',
        '../../DISCERN_data/synthetic/legitimate/svm/0/',
        '../../DISCERN_data/synthetic/legitimate/svm/0/',
        '../../DISCERN_data/synthetic/legitimate/svm/0/',
        '../../DISCERN_data/synthetic/legitimate/svm/0/',
        '../../DISCERN_data/synthetic/legitimate/svm/0/',
        '../../DISCERN_data/synthetic/legitimate/synflood/0/',
        '../../DISCERN_data/synthetic/legitimate/synflood/0/',
        '../../DISCERN_data/synthetic/legitimate/synflood/0/',
        '../../DISCERN_data/synthetic/legitimate/synflood/0/',
        '../../DISCERN_data/synthetic/legitimate/synflood/0/',
        '../../DISCERN_data/synthetic/legitimate/synflood/0/',
        
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/1/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/1/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/1/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/1/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/1/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/1/',
        '../../DISCERN_data/synthetic/legitimate/llm/1/',
        '../../DISCERN_data/synthetic/legitimate/llm/1/',
        '../../DISCERN_data/synthetic/legitimate/llm/1/',
        '../../DISCERN_data/synthetic/legitimate/llm/1/',
        '../../DISCERN_data/synthetic/legitimate/llm/1/',
        '../../DISCERN_data/synthetic/legitimate/llm/1/',
        '../../DISCERN_data/synthetic/legitimate/svm/1/',
        '../../DISCERN_data/synthetic/legitimate/svm/1/',
        '../../DISCERN_data/synthetic/legitimate/svm/1/',
        '../../DISCERN_data/synthetic/legitimate/svm/1/',
        '../../DISCERN_data/synthetic/legitimate/svm/1/',
        '../../DISCERN_data/synthetic/legitimate/svm/1/',
        '../../DISCERN_data/synthetic/legitimate/synflood/1/',
        '../../DISCERN_data/synthetic/legitimate/synflood/1/',
        '../../DISCERN_data/synthetic/legitimate/synflood/1/',
        '../../DISCERN_data/synthetic/legitimate/synflood/1/',
        '../../DISCERN_data/synthetic/legitimate/synflood/1/',
        '../../DISCERN_data/synthetic/legitimate/synflood/1/',
        
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/2/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/2/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/2/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/2/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/2/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/2/',
        '../../DISCERN_data/synthetic/legitimate/llm/2/',
        '../../DISCERN_data/synthetic/legitimate/llm/2/',
        '../../DISCERN_data/synthetic/legitimate/llm/2/',
        '../../DISCERN_data/synthetic/legitimate/llm/2/',
        '../../DISCERN_data/synthetic/legitimate/llm/2/',
        '../../DISCERN_data/synthetic/legitimate/llm/2/',
        '../../DISCERN_data/synthetic/legitimate/svm/2/',
        '../../DISCERN_data/synthetic/legitimate/svm/2/',
        '../../DISCERN_data/synthetic/legitimate/svm/2/',
        '../../DISCERN_data/synthetic/legitimate/svm/2/',
        '../../DISCERN_data/synthetic/legitimate/svm/2/',
        '../../DISCERN_data/synthetic/legitimate/svm/2/',
        '../../DISCERN_data/synthetic/legitimate/synflood/2/',
        '../../DISCERN_data/synthetic/legitimate/synflood/2/',
        '../../DISCERN_data/synthetic/legitimate/synflood/2/',
        '../../DISCERN_data/synthetic/legitimate/synflood/2/',
        '../../DISCERN_data/synthetic/legitimate/synflood/2/',
        '../../DISCERN_data/synthetic/legitimate/synflood/2/',
        
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/3/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/3/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/3/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/3/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/3/',
        '../../DISCERN_data/synthetic/legitimate/dnsmitm/3/',
        '../../DISCERN_data/synthetic/legitimate/llm/3/',
        '../../DISCERN_data/synthetic/legitimate/llm/3/',
        '../../DISCERN_data/synthetic/legitimate/llm/3/',
        '../../DISCERN_data/synthetic/legitimate/llm/3/',
        '../../DISCERN_data/synthetic/legitimate/llm/3/',
        '../../DISCERN_data/synthetic/legitimate/llm/3/',
        '../../DISCERN_data/synthetic/legitimate/svm/3/',
        '../../DISCERN_data/synthetic/legitimate/svm/3/',
        '../../DISCERN_data/synthetic/legitimate/svm/3/',
        '../../DISCERN_data/synthetic/legitimate/svm/3/',
        '../../DISCERN_data/synthetic/legitimate/svm/3/',
        '../../DISCERN_data/synthetic/legitimate/svm/3/',
        '../../DISCERN_data/synthetic/legitimate/synflood/3/',
        '../../DISCERN_data/synthetic/legitimate/synflood/3/',
        '../../DISCERN_data/synthetic/legitimate/synflood/3/',
        '../../DISCERN_data/synthetic/legitimate/synflood/3/',
        '../../DISCERN_data/synthetic/legitimate/synflood/3/',
        '../../DISCERN_data/synthetic/legitimate/synflood/3/',
        # Add more legitimate parent folders here
    ]
    
    parent2_list = [
        '../../DISCERN_data/synthetic/malicious/cryptominer/0/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/0/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/0/',
        '../../DISCERN_data/synthetic/malicious/portscanner/0/',
        '../../DISCERN_data/synthetic/malicious/ransomware/0/',
        '../../DISCERN_data/synthetic/malicious/spread/0/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/0/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/0/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/0/',
        '../../DISCERN_data/synthetic/malicious/portscanner/0/',
        '../../DISCERN_data/synthetic/malicious/ransomware/0/',
        '../../DISCERN_data/synthetic/malicious/spread/0/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/0/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/0/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/0/',
        '../../DISCERN_data/synthetic/malicious/portscanner/0/',
        '../../DISCERN_data/synthetic/malicious/ransomware/0/',
        '../../DISCERN_data/synthetic/malicious/spread/0/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/0/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/0/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/0/',
        '../../DISCERN_data/synthetic/malicious/portscanner/0/',
        '../../DISCERN_data/synthetic/malicious/ransomware/0/',
        '../../DISCERN_data/synthetic/malicious/spread/0/',
        
        '../../DISCERN_data/synthetic/malicious/cryptominer/1/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/1/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/1/',
        '../../DISCERN_data/synthetic/malicious/portscanner/1/',
        '../../DISCERN_data/synthetic/malicious/ransomware/1/',
        '../../DISCERN_data/synthetic/malicious/spread/1/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/1/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/1/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/1/',
        '../../DISCERN_data/synthetic/malicious/portscanner/1/',
        '../../DISCERN_data/synthetic/malicious/ransomware/1/',
        '../../DISCERN_data/synthetic/malicious/spread/1/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/1/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/1/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/1/',
        '../../DISCERN_data/synthetic/malicious/portscanner/1/',
        '../../DISCERN_data/synthetic/malicious/ransomware/1/',
        '../../DISCERN_data/synthetic/malicious/spread/1/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/1/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/1/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/1/',
        '../../DISCERN_data/synthetic/malicious/portscanner/1/',
        '../../DISCERN_data/synthetic/malicious/ransomware/1/',
        '../../DISCERN_data/synthetic/malicious/spread/1/',

        '../../DISCERN_data/synthetic/malicious/cryptominer/2/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/2/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/2/',
        '../../DISCERN_data/synthetic/malicious/portscanner/2/',
        '../../DISCERN_data/synthetic/malicious/ransomware/2/',
        '../../DISCERN_data/synthetic/malicious/spread/2/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/2/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/2/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/2/',
        '../../DISCERN_data/synthetic/malicious/portscanner/2/',
        '../../DISCERN_data/synthetic/malicious/ransomware/2/',
        '../../DISCERN_data/synthetic/malicious/spread/2/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/2/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/2/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/2/',
        '../../DISCERN_data/synthetic/malicious/portscanner/2/',
        '../../DISCERN_data/synthetic/malicious/ransomware/2/',
        '../../DISCERN_data/synthetic/malicious/spread/2/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/2/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/2/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/2/',
        '../../DISCERN_data/synthetic/malicious/portscanner/2/',
        '../../DISCERN_data/synthetic/malicious/ransomware/2/',
        '../../DISCERN_data/synthetic/malicious/spread/2/',

        '../../DISCERN_data/synthetic/malicious/cryptominer/3/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/3/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/3/',
        '../../DISCERN_data/synthetic/malicious/portscanner/3/',
        '../../DISCERN_data/synthetic/malicious/ransomware/3/',
        '../../DISCERN_data/synthetic/malicious/spread/3/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/3/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/3/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/3/',
        '../../DISCERN_data/synthetic/malicious/portscanner/3/',
        '../../DISCERN_data/synthetic/malicious/ransomware/3/',
        '../../DISCERN_data/synthetic/malicious/spread/3/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/3/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/3/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/3/',
        '../../DISCERN_data/synthetic/malicious/portscanner/3/',
        '../../DISCERN_data/synthetic/malicious/ransomware/3/',
        '../../DISCERN_data/synthetic/malicious/spread/3/',
        '../../DISCERN_data/synthetic/malicious/cryptominer/3/',
        '../../DISCERN_data/synthetic/malicious/exfiltrate/3/',
        '../../DISCERN_data/synthetic/malicious/internetscanner/3/',
        '../../DISCERN_data/synthetic/malicious/portscanner/3/',
        '../../DISCERN_data/synthetic/malicious/ransomware/3/',
        '../../DISCERN_data/synthetic/malicious/spread/3/',

        # Add more malicious parent folders here
    ]

    base_output_folder = '../../DISCERN_data/synthetic/merged/'
    
    for parent_folder1, parent_folder2 in zip(parent1_list, parent2_list):
        print(f"\n############################################################")
        print(f"### Batch: {parent_folder1} + {parent_folder2} ###")
        print(f"############################################################")
        
        time_offset_file1 = 0 
    
        try:
            name1 = os.path.basename(os.path.dirname(os.path.normpath(parent_folder1)))
            name2 = os.path.basename(os.path.dirname(os.path.normpath(parent_folder2)))
            common_dir = os.path.basename(os.path.normpath(parent_folder1))
        except Exception as e:
            continue

        specific_output_base = os.path.join(base_output_folder, f"{name1}_{name2}", common_dir)

        try:
            subfolders1 = sorted([d for d in os.listdir(parent_folder1) if os.path.isdir(os.path.join(parent_folder1, d)) and d != "summary"])
            subfolders2 = sorted([d for d in os.listdir(parent_folder2) if os.path.isdir(os.path.join(parent_folder2, d)) and d != "summary"])
        except FileNotFoundError:
            continue

        if not subfolders1 or not subfolders2:
            continue

        # --- STEP 1: FIND GLOBAL BOUNDARIES ---
        s1_global_start = float('inf')
        min_s1_span = float('inf')
        
        # Scan legit files
        for subfolder in subfolders1:
            s1_path = os.path.join(parent_folder1, subfolder, "proc-cpu-data.txt")
            timespan_info = get_timespan_from_file(s1_path)
            if timespan_info:
                start, end, span = timespan_info
                if start < s1_global_start: s1_global_start = start
                if span < min_s1_span: min_s1_span = span

        s2_global_start = float('inf')
        max_s2_span = 0
        
        # Scan malicious files
        for subfolder in subfolders2:
            s2_path = os.path.join(parent_folder2, subfolder, "proc-cpu-data.txt")
            timespan_info = get_timespan_from_file(s2_path)
            if timespan_info:
                start, end, span = timespan_info
                if start < s2_global_start: s2_global_start = start
                if span > max_s2_span: max_s2_span = span

        if min_s1_span == float('inf') or max_s2_span == 0:
            print("[FATAL] Could not determine baseline timespans. Skipping.")
            continue
            
        if min_s1_span < max_s2_span:
            print(f"[FATAL] Legitimate window ({min_s1_span}s) too short for attack ({max_s2_span}s). Skipping.")
            continue

        # --- STEP 2: CALCULATE GOLDEN OFFSET ---
        gap = min_s1_span - max_s2_span
        injection_delay = int(gap * random.uniform(0.0, 1.0))
        target_injection_epoch = s1_global_start + injection_delay
        
        # This offset is: (Target Time) - (Original Global Malicious Start)
        golden_offset = target_injection_epoch - s2_global_start
        
        print(f"  Global Mal Start: {s2_global_start}")
        print(f"  Target Epoch:     {target_injection_epoch}")
        print(f"  Golden Offset:    {golden_offset}")

        folder_permutations = list(itertools.product(subfolders1, subfolders2))
        
        for i, (subfolder1, subfolder2) in enumerate(folder_permutations):
            print(f"===== Combination {i+1}/{len(folder_permutations)} =====")
            
            source_path1 = os.path.join(parent_folder1, subfolder1)
            source_path2 = os.path.join(parent_folder2, subfolder2)
            output_path = os.path.join(specific_output_base, f"{subfolder1.replace('-data', '')}_{subfolder2.replace('-data', '')}")

            # Define 'local_s1_start' properly here before usage
            timespan_info1 = get_timespan_from_file(os.path.join(source_path1, "proc-cpu-data.txt"))
            local_s1_start = timespan_info1[0] if timespan_info1 else s1_global_start

            # Note: We pass 'local_s1_start' to be safe, but rely entirely on golden_offset (time_offset_file2) for malicious data
            process_and_combine_data(
                source_path1, 
                source_path2, 
                output_path, 
                local_s1_start, 0, 0,          
                golden_offset,    # The ONLY offset that matters now
                target_injection_epoch, 
                target_injection_epoch + max_s2_span
            )
            
            malicious_time_data = {
                "malicious_start_time": target_injection_epoch, 
                "malicious_end_time": target_injection_epoch + max_s2_span
            }
            with open(os.path.join(output_path, "malicious_time.txt"), 'w') as f:
                json.dump(malicious_time_data, f, indent=4)
            
            print(f"===== Finished Combination =====\n")
