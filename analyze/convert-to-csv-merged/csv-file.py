import json
import csv
import argparse
from pathlib import Path

def convert_file_data_to_csv(input_file_path, output_file_path):
    """
    Reads JSON file change data and writes it to a CSV file.
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
        'timestamp', 'device_id', 'location', 'size', 
        'hash', 'owner', 'group'
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

            # Extract data, providing default values for safety
            timestamp = log_entry.get('TimeStamp', 'N/A')
            dev_id = log_entry.get('DevID', 'N/A')
            
            # These fields are arrays in the JSON, we'll take the first element
            # and provide a default list to prevent index errors.
            location = log_entry.get('Location', ['N/A'])[0]
            size = log_entry.get('Size', ['0'])[0]
            hash_val = log_entry.get('Hash', ['N/A'])[0]
            
            # Ownership is an array of objects
            ownership = log_entry.get('Ownership', [{}])[0]
            owner = ownership.get('Owner', 'N/A')
            group = ownership.get('Group', 'N/A')

            row = [
                timestamp,
                dev_id,
                location,
                size,
                hash_val,
                owner,
                group
            ]
            if has_malicious_field:
                row.append(log_entry.get('malicious', 0))
            writer.writerow(row)

    print(f"Successfully converted file data to '{output_file_path}'")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse file change data (json-like) and convert it to a CSV file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input file with file change data."
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default='file-summary.csv',
        help="Path to save the output summary CSV file."
    )

    args = parser.parse_args()
    convert_file_data_to_csv(args.input_file, args.output)
