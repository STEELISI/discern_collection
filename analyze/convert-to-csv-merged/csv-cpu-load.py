import json
import csv
import argparse
from pathlib import Path

def convert_cpu_load_to_csv(input_file_path, output_file_path):
    """
    Reads JSON CPU load data and writes it to a CSV file.
    Handles variable number of CPU cores by performing two passes on the input file.
    The first pass determines the maximum number of cores to create the header.
    The second pass writes the data to the CSV file. Conditionally adds a
    'malicious' column if the field is present in the source data.
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

    # First pass: determine the maximum number of cores
    max_cores = 0
    try:
        with open(input_file_path, 'r') as infile:
            for line in infile:
                try:
                    log_entry = json.loads(line)
                    num_cores = len(log_entry.get("Load", []))
                    if num_cores > max_cores:
                        max_cores = num_cores
                except json.JSONDecodeError:
                    print(f"Warning: Skipping invalid JSON line during first pass: {line.strip()}")
                    continue
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
        return

    # Prepare header based on the maximum number of cores found
    header = ['timestamp', 'device_id']
    for i in range(max_cores):
        header.append(f'load_core_{i}')
    if has_malicious_field:
        header.append('malicious')

    # Second pass: read data and write to CSV
    with open(input_file_path, 'r') as infile, open(output_file_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)

        for line in infile:
            try:
                log_entry = json.loads(line)
            except json.JSONDecodeError:
                print(f"Warning: Skipping invalid JSON line during second pass: {line.strip()}")
                continue

            timestamp = log_entry.get('TimeStamp', 'N/A')
            dev_id = log_entry.get('DevID', 'N/A')
            loads = log_entry.get('Load', [])

            row = [timestamp, dev_id] + loads
            
            # Pad the row with empty strings if it has fewer cores than the max
            if len(loads) < max_cores:
                row.extend([''] * (max_cores - len(loads)))

            if has_malicious_field:
                row.append(log_entry.get('malicious', 0))

            writer.writerow(row)

    print(f"Successfully converted CPU load data to '{output_file_path}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse CPU load data (json-like) and convert it to a CSV file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input file with CPU load data."
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default='cpu-load-summary.csv',
        help="Path to save the output summary CSV file."
    )

    args = parser.parse_args()
    convert_cpu_load_to_csv(args.input_file, args.output)
