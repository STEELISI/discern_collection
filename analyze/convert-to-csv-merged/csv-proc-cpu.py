import json
import csv
import argparse
from pathlib import Path

def convert_proc_cpu_to_csv(input_file_path, output_file_path):
    """
    Reads JSON process/CPU data and writes it to a CSV file.
    Handles missing keys in the JSON objects by using default values.
    Conditionally adds a 'malicious' column if the field is present
    in the source data.
    """
    # --- Check for 'malicious' field in the first line ---
    has_malicious_field = False
    try:
        with open(input_file_path, 'r') as f:
            for line in f:
                if line.strip():
                    if 'malicious' in json.loads(line):
                        has_malicious_field = True
                    break 
    except (json.JSONDecodeError, FileNotFoundError):
        pass

    header = [
        'timestamp', 'pid', 'ppid', 'real_uid', 'effective_uid', 'saved_uid',
        'filesystem_uid', 'real_gid', 'effective_gid', 'saved_gid', 'filesystem_gid',
        'vm_peak', 'vm_size', 'vm_hwm', 'vm_stk', 'vm_data', 'threads', 'name',
        'state', 'device_id', 'cpu'
    ]
    if has_malicious_field:
        header.append('malicious')

    with open(input_file_path, 'r') as infile, open(output_file_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)

        for line in infile:
            try:
                log_entry = json.loads(line)
            except json.JSONDecodeError:
                print(f"Warning: Skipping invalid JSON line: {line.strip()}")
                continue
            
            # Extract data using .get() to handle missing keys gracefully
            row = [
                log_entry.get('TimeStamp', 'N/A'),
                log_entry.get('Pid', 'N/A'),
                log_entry.get('PPid', 'N/A'),
                log_entry.get('RealUid', 'N/A'),
                log_entry.get('EffectiveUid', 'N/A'),
                log_entry.get('SavedUid', 'N/A'),
                log_entry.get('FilesystemUid', 'N/A'),
                log_entry.get('RealGid', 'N/A'),
                log_entry.get('EffectiveGid', 'N/A'),
                log_entry.get('SavedGid', 'N/A'),
                log_entry.get('FilesystemGid', 'N/A'),
                log_entry.get('VmPeak', 'N/A'),
                log_entry.get('VmSize', 'N/A'),
                log_entry.get('VmHWM', 'N/A'),
                log_entry.get('VmStk', 'N/A'),
                log_entry.get('VmData', 'N/A'),
                log_entry.get('Threads', 'N/A'),
                log_entry.get('Name', 'N/A'),
                log_entry.get('State', 'N/A'),
                log_entry.get('DevID', 'N/A'),
                log_entry.get('Cpu', 0.0)
            ]
            if has_malicious_field:
                row.append(log_entry.get('malicious', 0))
            writer.writerow(row)

    print(f"Successfully converted process/CPU data to '{output_file_path}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse process/CPU data (json-like) and convert it to a CSV file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input file with process/CPU data."
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default='proc-cpu-summary.csv',
        help="Path to save the output summary CSV file."
    )

    args = parser.parse_args()
    convert_proc_cpu_to_csv(args.input_file, args.output)
