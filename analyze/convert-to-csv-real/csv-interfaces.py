import json
import csv
import argparse
from pathlib import Path

def process_interfaces_splitter(input_file_path):
    """
    Reads JSON interface data and splits it into multiple CSV files 
    based on the DevID structure.
    
    Path Structure: ./[suffix_path]/[prefix]-data/interfaces.csv
    Example: client.a.b.c.d -> ./a_b_c_d/client-data/interfaces.csv
    """
    
    # Track initialized files to avoid redundant disk checks
    initialized_paths = set()

    # Fixed header for Interface data
    header = [
        'timestamp', 'device_id', 'interface_name', 'action', 
        'hardware_addr', 'ips'
    ]

    try:
        with open(input_file_path, 'r') as infile:
            for line in infile:
                if not line.strip(): 
                    continue
                
                try:
                    log_entry = json.loads(line)
                    if not isinstance(log_entry, dict):
                        continue
                except json.JSONDecodeError:
                    continue

                # --- 1. Dynamic Routing (Check DevID) ---
                dev_id = log_entry.get('DevID', 'unknown')
                parts = dev_id.split('.')
                
                if len(parts) >= 2:
                    # Example: client.bnsdq.ozxfs -> client-data / bnsdq_ozxfs...
                    folder_sub = f"{parts[0]}-data"
                    folder_base = "_".join(parts[1:])
                else:
                    folder_base = "unknown_device_group"
                    folder_sub = f"{dev_id}-data"

                output_dir = Path(folder_base) / folder_sub
                output_file = output_dir / "interfaces.csv"
                output_path_str = str(output_file)

                # --- 2. Extract Data Fields ---
                
                # Handle IPs list: Convert ["1.2.3.4", "::1"] -> "1.2.3.4; ::1"
                raw_ips = log_entry.get('IPs', [])
                if isinstance(raw_ips, list):
                    ips_str = "; ".join(raw_ips)
                else:
                    ips_str = str(raw_ips) if raw_ips else ""

                row = [
                    log_entry.get('TimeStamp', 'N/A'),
                    dev_id,
                    log_entry.get('Name', 'N/A'),
                    log_entry.get('Action', 'N/A'),
                    log_entry.get('HardwareAddr', 'N/A'), # Optional field
                    ips_str
                ]

                # --- 3. Write Data ---
                output_dir.mkdir(parents=True, exist_ok=True)

                write_header = False
                
                if output_path_str not in initialized_paths:
                    if not output_file.exists():
                        write_header = True
                    initialized_paths.add(output_path_str)

                with open(output_file, 'a', newline='') as outfile:
                    writer = csv.writer(outfile)
                    if write_header:
                        writer.writerow(header)
                    writer.writerow(row)

    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
    except IOError as e:
        print(f"File I/O Error: {e}")
    else:
        print(f"Processing complete. Data distributed into {len(initialized_paths)} unique CSV files.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Splits Interface JSON logs into folders based on DevID.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input file with interface data."
    )

    args = parser.parse_args()
    process_interfaces_splitter(args.input_file)
