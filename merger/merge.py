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
        # Check if 'Load' key exists and its value is a list
        if 'Load' in item and isinstance(item['Load'], list):
            current_cores = len(item['Load'])
            if current_cores < max_cores:
                # Append the necessary number of zeros
                item['Load'].extend([0.0] * (max_cores - current_cores))
    return data_list

def finetune_cpu_data(all_data, file1_path, file2_path, malicious_start_time, malicious_end_time):
    data_before = []
    data_during = []
    data_after = []

    # Partition the data into three lists: before, during, and after the merge window.
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
            print(f"Warning: Could not parse timestamp for item: {item}. Skipping in finetune.")
            continue
        
    max_cores = 1
    for item in all_data:
        load = item.get('Load', [])
        if isinstance(load, list):
            max_cores = max(max_cores, len(load))

    # Pad the 'Load' arrays in the 'before' and 'after' lists.
    data_before = normalize_cpu_cores(data_before, max_cores)
    data_after = normalize_cpu_cores(data_after, max_cores)

    # --- The pairing logic now runs ONLY on the 'data_during' list ---
    # Ensure data is sorted by timestamp, which is crucial for pairing adjacent items.
    data_during.sort(key=lambda x: int(x.get('TimeStamp', 0)))

    unified_dev_id = generate_unified_dev_id(file1_path, file2_path)
    processed_during_data = []
    start = 0
    if(int(data_during[1]['TimeStamp']) - int(data_during[0]['TimeStamp'] )> int(data_during[2]['TimeStamp']) - int(data_during[1]['TimeStamp'])):
        start = 1
        data_before.append(data_during[0])

    # Iterate through the 'during' list, taking two items at a time (a pair).
    for i in range(start, len(data_during) - 1, 2):
        item1 = data_during[i]
        item2 = data_during[i+1]

        try:
            # Calculate the average of the two timestamps
            ts1 = int(item1['TimeStamp'])
            ts2 = int(item2['TimeStamp'])
            avg_timestamp = str(int((ts1 + ts2) / 2))

            # Sum the 'Load' arrays, handling cases with different core counts
            load1 = item1.get('Load', [])
            load2 = item2.get('Load', [])
            
            # Use itertools.zip_longest to pad the shorter list with 0.0
            summed_load = [min(x + y, 100.0) for x, y in itertools.zip_longest(load1, load2, fillvalue=0.0)]

            # Construct the new, merged record
            merged_record = {
                'TimeStamp': avg_timestamp,
                'Load': summed_load,
                'DevID': unified_dev_id,
                'malicious': item1.get('malicious', 0) or item2.get('malicious', 0)
            }
            processed_during_data.append(merged_record)

        except (KeyError, TypeError, ValueError) as e:
            print(f"Warning: Skipping a pair during CPU data fine-tuning due to an error: {e}")
            continue
    
    # Combine the untouched 'before' and 'after' data with the processed 'during' data.
    return data_before + processed_during_data + data_after

def find_earliest_timestamp(filepath):
    """
    Finds the absolute earliest timestamp in a sorted network data file
    by only inspecting the first valid record.
    """
    try:
        with open(filepath, 'r') as f:
            # Find the first non-empty line
            first_line = ""
            for line in f:
                if line.strip():
                    first_line = line
                    break
            
            if not first_line:
                return float('inf')

            data = json.loads(first_line)
            earliest = int(data["TimeStamp"])
            return earliest

    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, KeyError):
        print(f"Warning: Could not parse first line of {filepath} to find earliest timestamp.")
    
    return float('inf')

def get_timespan_from_file(filepath):
    """
    Calculates the timespan (last_timestamp - first_timestamp) for a data file.
    
    Reads the first and last valid lines to get timestamps.
    
    Args:
        filepath (str): The path to the data file.

    Returns:
        tuple: (first_ts, last_ts, timespan) or None if file is invalid.
    """
    first_ts, last_ts = None, None
    try:
        with open(filepath, 'r') as f:
            # Find the first valid line for the first timestamp
            for line in f:
                if line.strip():
                    first_ts = int(json.loads(line)['TimeStamp'])
                    break
            
            if first_ts is None: # File is empty or contains only empty lines
                return None

            # Efficiently find the last line by reading the end of the file
            f.seek(0, os.SEEK_END)
            # Handle files smaller than the buffer
            buffer_size = min(8192, f.tell())
            f.seek(f.tell() - buffer_size, os.SEEK_SET)
            buffer = f.read(buffer_size)
            
            # Find the last non-empty line in the buffer
            lines = buffer.strip().split('\n')
            if lines and lines[-1]:
                 last_ts = int(json.loads(lines[-1])['TimeStamp'])
            else: # Fallback for very small files
                f.seek(0)
                all_lines = [l for l in f.readlines() if l.strip()]
                if all_lines:
                    last_ts = int(json.loads(all_lines[-1])['TimeStamp'])

        if first_ts is not None and last_ts is not None:
            return first_ts, last_ts, last_ts - first_ts
            
    except (FileNotFoundError, json.JSONDecodeError, KeyError, IndexError):
        # Handle cases where file doesn't exist, is malformed, or empty
        print(f"Warning: Could not read or parse timestamps from {filepath}")
        return None
    
    return None

def process_network_data(file1_path, file2_path,s1_start,s2_start,time_offset_file1, time_offset_file2):
    """
    A separate, isolated pipeline for processing network-data.txt to preserve
    the accuracy of nested timestamps.
    """
    print("Running isolated processing for network-data.txt...")
    all_data = []
    unified_dev_id = generate_unified_dev_id(file1_path, file2_path)
    min_timestamp = find_earliest_timestamp(file1_path)
    local_min = find_earliest_timestamp(file2_path)
    
    if min_timestamp == float('inf'):
        print(f"Warning: Could not establish a baseline timestamp from {file1_path}. Cannot process network data.")
        return []

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
                    print(f"Warning: Could not process line in {file1_path}: {line.strip()}")
    except FileNotFoundError:
        print(f"Warning: File not found: {file1_path}")

    
    try:
        with open(file2_path, 'r') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    
                    data["TimeStamp"] = str(int(data["TimeStamp"]) - s2_start + s1_start + time_offset_file2)
                    data["malicious"] = 1
                    
                    if "Packets" in data and isinstance(data["Packets"], list):
                        for i, packet in enumerate(data["Packets"]):
                            data["Packets"][i]["TimeStamp"] = str(int(data["Packets"][i]["TimeStamp"]) - s2_start + s1_start + time_offset_file2)
                    all_data.append(data)
                except (json.JSONDecodeError, KeyError):
                    print(f"Warning: Could not process line in {file2_path}: {line.strip()}")
    except FileNotFoundError:
        print(f"Warning: File not found: {file2_path}")
            
    return all_data

def process_file_data(file1_path,file2_path,s1_start,s2_start, time_offset_file1, time_offset_file2):
    all_data = []
    min_timestamp = find_earliest_timestamp(file1_path)
    # if(min_timestamp == float('inf')):
    #     min_timestamp = find_earliest_timestamp(file2_path)
    local_min = find_earliest_timestamp(file2_path)
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
                    print(f"Warning: Could not decode or process line in {file1_path}: {line.strip()}")
    except FileNotFoundError:
        print(f"Warning: File not found, skipping: {file1_path}")


    try:
        with open(file2_path, 'r') as f:
            first_line = f.readline()
            if not first_line.strip():
                local_min = 0 
                f.seek(0)
            else:
                try:
                    local_min = int(json.loads(first_line)["TimeStamp"])
                except (json.JSONDecodeError, KeyError):
                    print(f"Warning: Could not get local_min from first line of {file2_path}. Using 0.")
                    local_min = 0
                    f.seek(0)

            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    if data["TimeStamp"] == str(local_min):
                        # set file initialization to the same as the file1
                        data["TimeStamp"] = str(min_timestamp +time_offset_file1)
                    else:
                        data["TimeStamp"] = str(int(data["TimeStamp"]) - s2_start + s1_start + time_offset_file2)
                        data["malicious"] = 1
                        if "DevID" in data:
                            data["DevID"] = unified_dev_id
                        all_data.append(data)
                except (json.JSONDecodeError, KeyError):
                    print(f"Warning: Could not decode or process line in {file2_path}: {line.strip()}")
    except FileNotFoundError:
        print(f"Warning: File not found, skipping: {file2_path}")
        
    return all_data

def run_csv_converter(input_file_path):
    """
    Dynamically finds and runs the correct CSV converter script for the given input file.
    """
    filename = os.path.basename(input_file_path)
    # e.g., "cpu-load-data.txt" -> "cpu-load"
    data_type = filename.replace("-data.txt", "")
    
    # This script is in merger/, converters are in analyze/convert-to-csv/
    script_dir = os.path.dirname(__file__)
    script_path = os.path.join(script_dir, '..', 'analyze', 'convert-to-csv', f"csv-{data_type}.py")

    if not os.path.exists(script_path):
        print(f"  > CSV converter not found for {data_type} at {script_path}. Skipping CSV generation.")
        return

    # e.g. "output/cpu-load-data.txt" -> "output/cpu-load-res.csv"
    output_csv_path = os.path.join(
        os.path.dirname(input_file_path),
        f"{data_type}-res.csv"
    )

    command = [
        sys.executable,
        script_path,
        input_file_path,
        "-o",
        output_csv_path
    ]
    
    print(f"  > Running CSV converter for {filename}...")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"  > Successfully created {os.path.basename(output_csv_path)}")
        if result.stdout:
            # Print converter output, indented for clarity
            for line in result.stdout.strip().split('\\n'):
                print(f"    {line}")
    except subprocess.CalledProcessError as e:
        print(f"  > Error running CSV converter for {filename}:")
        print(f"    Command: {' '.join(command)}")
        print(f"    Exit Code: {e.returncode}")
        print(f"    Stderr: {e.stderr.strip()}")
        print(f"    Stdout: {e.stdout.strip()}")

def process_and_combine_data(folder1, folder2, output_folder,s1_start, s2_start, time_offset_file1, time_offset_file2,malicious_start_time,malicious_end_time):
    """
    Main processing function that merges files using your original timestamp logic
    and then calls fine-tuning functions as needed.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    common_files = sorted([
        "cpu-load-data.txt", "file-data.txt", "network-data.txt",
        "proc-cpu-data.txt", "proc-mem-data.txt", "proc-new-data.txt"
    ])

    for filename in common_files:
        print(f"--- Processing {filename} ---")
        file1_path = os.path.join(folder1, filename)
        file2_path = os.path.join(folder2, filename)
        output_file_path = os.path.join(output_folder, filename)

        all_data = []

        # seperated processing for network and file data
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
                            print(f"Warning: Could not decode or process line in {file1_path}: {line.strip()}")
            except FileNotFoundError:
                print(f"Warning: File not found, skipping: {file1_path}")


            try:
                with open(file2_path, 'r') as f:
                    for line in f:
                        if not line.strip(): continue
                        try:
                            data = json.loads(line)
                            data["TimeStamp"] = str(int(data["TimeStamp"]) - s2_start + s1_start + time_offset_file2)
                            data["malicious"] = 1
                            if "DevID" in data:
                                data["DevID"] = unified_dev_id
                            all_data.append(data)
                        except (json.JSONDecodeError, KeyError):
                            print(f"Warning: Could not decode or process line in {file2_path}: {line.strip()}")
            except FileNotFoundError:
                print(f"Warning: File not found, skipping: {file2_path}")

            if filename == "cpu-load-data.txt" and all_data:
                all_data = finetune_cpu_data(all_data, file1_path, file2_path,malicious_start_time,malicious_end_time )
                
            
        
        if not all_data:
            print(f"Warning: No data to write for {filename}.")
            open(output_file_path, 'w').close()
            continue
        
        all_data.sort(key=lambda x: int(x['TimeStamp']))

        with open(output_file_path, 'w') as f:
            for data in all_data:
                f.write(json.dumps(data) + '\n')

        print(f"Successfully processed and combined '{filename}'")
        run_csv_converter(output_file_path)
        print("-" * (25 + len(filename)))


# This block runs when the script is executed
if __name__ == "__main__":
    
    # --- 1. DEFINE YOUR LISTS OF FOLDERS HERE ---
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
    
    # --- 2. THE SCRIPT WILL LOOP THROUGH THE LISTS ---
    # The zip function pairs the first item of parent1_list with the first of parent2_list, and so on.
    for parent_folder1, parent_folder2 in zip(parent1_list, parent2_list):
        print(f"\n\n\n############################################################")
        print(f"### Starting new batch: {os.path.basename(os.path.normpath(parent_folder1))} + {os.path.basename(os.path.normpath(parent_folder2))} ###")
        print(f"### Starting new batch: {parent_folder1} + {parent_folder2} ###")
        print(f"############################################################\n")
        
        # --- The logic from your original main() function is now inside the loop ---
        time_offset_file1 = 0 
    
        try:
            name1 = os.path.basename(os.path.dirname(os.path.normpath(parent_folder1)))
            name2 = os.path.basename(os.path.dirname(os.path.normpath(parent_folder2)))
            common_dir = os.path.basename(os.path.normpath(parent_folder1))
        except Exception as e:
            print(f"Could not automatically determine names from paths. Error: {e}")
            continue # Skip to the next pair in the list

        specific_output_base = os.path.join(base_output_folder, f"{name1}_{name2}", common_dir)

        try:
            subfolders1 = sorted([d for d in os.listdir(parent_folder1) if os.path.isdir(os.path.join(parent_folder1, d)) and d != "summary"])
            subfolders2 = sorted([d for d in os.listdir(parent_folder2) if os.path.isdir(os.path.join(parent_folder2, d)) and d != "summary"])
        except FileNotFoundError as e:
            print(f"Error: Could not find one of the parent directories. {e}")
            continue

        if not subfolders1 or not subfolders2:
            print("Warning: One or both parent directories are empty. Skipping this pair.")
            continue

        # --- Pre-processing: Find the tightest-case scenario for this pair ---
        min_s1_span = float('inf')
        for subfolder in subfolders1:
            s1_path = os.path.join(parent_folder1, subfolder, "proc-cpu-data.txt")
            timespan_info = get_timespan_from_file(s1_path)
            if timespan_info and timespan_info[2] < min_s1_span:
                min_s1_span = timespan_info[2]

        max_s2_span = 0
        for subfolder in subfolders2:
            s2_path = os.path.join(parent_folder2, subfolder, "proc-cpu-data.txt")
            timespan_info = get_timespan_from_file(s2_path)
            if timespan_info and timespan_info[2] > max_s2_span:
                max_s2_span = timespan_info[2]

        if min_s1_span == float('inf') or max_s2_span == 0:
            print("[FATAL] Could not determine baseline timespans. Skipping this pair.")
            continue

        if min_s1_span < max_s2_span:
            print(f"[FATAL] Shortest S1 span ({min_s1_span}) cannot fit longest S2 span ({max_s2_span}). Skipping this pair.")
            continue
        
        random_proportion = random.uniform(0.0, 1.0)
        folder_permutations = list(itertools.product(subfolders1, subfolders2))
        print(f"Generated {len(folder_permutations)} combinations for this batch.\n")
        
        for i, (subfolder1, subfolder2) in enumerate(folder_permutations):
            print(f"===== Starting Combination {i+1}/{len(folder_permutations)}: {subfolder1} + {subfolder2} =====")
            
            source_path1 = os.path.join(parent_folder1, subfolder1)
            source_path2 = os.path.join(parent_folder2, subfolder2)
            
            timespan_info1 = get_timespan_from_file(os.path.join(source_path1, "proc-cpu-data.txt"))
            timespan_info2 = get_timespan_from_file(os.path.join(source_path2, "proc-cpu-data.txt"))

            if not timespan_info1 or not timespan_info2:
                print(f"  [STOP] Could not determine timespan for this pair. Skipping.")
                continue

            s1_start, _, s1_span = timespan_info1
            s2_start, _, s2_span = timespan_info2

            if s1_span < s2_span:
                print(f"  [STOP] S1 span ({s1_span}) is too short for this S2 file ({s2_span}). Skipping.")
                continue
                
            available_gap = s1_span - s2_span
            time_offset_file2 = int(available_gap * random_proportion)

            output_path = os.path.join(specific_output_base, f"{subfolder1.replace('-data', '')}_{subfolder2.replace('-data', '')}")
            malicious_start_time = s1_start + time_offset_file2
            malicious_end_time = malicious_start_time + s2_span

            process_and_combine_data(source_path1, source_path2, output_path, s1_start, s2_start, time_offset_file1, time_offset_file2, malicious_start_time, malicious_end_time)
            
            malicious_time_data = {"malicious_start_time": malicious_start_time, "malicious_end_time": malicious_end_time}
            with open(os.path.join(output_path, "malicious_time.txt"), 'w') as f:
                json.dump(malicious_time_data, f, indent=4)
            
            print(f"===== Finished Combination: {subfolder1} + {subfolder2} =====\n")