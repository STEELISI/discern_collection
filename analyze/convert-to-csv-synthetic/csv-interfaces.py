import json
import csv
import argparse
import sys
from pathlib import Path

def get_val(data, key, default="N/A"):
    """Safely extracts value from dictionary, handling various types."""
    val = data.get(key)
    if val is None:
        return default
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)

def check_for_malicious_field(input_file_path):
    """Scans the file to see if 'malicious' field exists."""
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    if "malicious" in data:
                        return True
                    return False # Check first valid line only
                except json.JSONDecodeError:
                    continue
    except:
        pass
    return False

def convert_interface_to_csv(input_file_path, output_file_path):
    # 1. Check for malicious field
    has_malicious = check_for_malicious_field(input_file_path)

    # 2. Define Header
    header = [
        "timestamp", "device_id", "interface_name", "action", 
        "hardware_addr", "ips"
    ]
    
    if has_malicious:
        header.append("malicious")

    rows_buffer = []

    print(f"Reading {input_file_path}...")

    try:
        with open(input_file_path, 'r', encoding='utf-8') as infile:
            for line in infile:
                if not line.strip(): continue

                try:
                    log_entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # --- Extract Fields ---
                timestamp = get_val(log_entry, "TimeStamp", "0")
                dev_id    = get_val(log_entry, "DevID", "N/A")
                if_name   = get_val(log_entry, "Name", "N/A")
                action    = get_val(log_entry, "Action", "N/A")
                hw_addr   = get_val(log_entry, "HardwareAddr", "N/A")
                
                # Handle IPs List -> String
                raw_ips = log_entry.get("IPs", [])
                if isinstance(raw_ips, list):
                    # Join with semicolon to keep CSV clean
                    ips_str = "; ".join(str(ip) for ip in raw_ips)
                else:
                    ips_str = str(raw_ips) if raw_ips else "N/A"

                # --- Build Row ---
                row = {
                    "timestamp": timestamp,
                    "device_id": dev_id,
                    "interface_name": if_name,
                    "action": action,
                    "hardware_addr": hw_addr,
                    "ips": ips_str
                }

                if has_malicious:
                    row["malicious"] = get_val(log_entry, "malicious", "0")

                rows_buffer.append(row)

    except FileNotFoundError:
        print(f"Error: Input file {input_file_path} not found.")
        sys.exit(1)

    # --- Sort by Timestamp ---
    try:
        rows_buffer.sort(key=lambda x: float(x['timestamp']))
    except ValueError:
        pass

    # --- Write to CSV ---
    print(f"Writing {len(rows_buffer)} rows to {output_file_path}...")
    try:
        with open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows_buffer)
        print("Done.")
    except IOError as e:
        print(f"Error writing output file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert JSON interface logs to flat CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file", 
        type=Path,
        help="Path to input interface-data.txt"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default="interface-res.csv",
        help="Path to save the output CSV."
    )

    args = parser.parse_args()
    convert_interface_to_csv(args.input_file, args.output)