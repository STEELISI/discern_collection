import json
import csv
import argparse
from pathlib import Path

def process_file_data_splitter(input_file_path):
    """
    Reads JSON file change data and splits it into multiple CSV files 
    based on the DevID structure.
    
    Path Structure: ./[suffix_path]/[prefix]-data/file.csv
    """
    
    # Track initialized files to avoid redundant disk checks
    initialized_paths = set()

    # Fixed header for File Change data
    header = [
        'timestamp', 'device_id', 'location', 'size', 
        'hash', 'owner', 'group'
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
                # Using 'file.csv' to match your original default
                output_file = output_dir / "file.csv" 
                output_path_str = str(output_file)

                # --- 2. Extract Data Fields (Handling Arrays Safely) ---
                timestamp = log_entry.get('TimeStamp', 'N/A')
                
                # Helper to safely get index 0 from a list
                def get_first(source, key, default='N/A'):
                    val = source.get(key)
                    if isinstance(val, list) and len(val) > 0:
                        return val[0]
                    return default

                location = get_first(log_entry, 'Location', 'N/A')
                size = get_first(log_entry, 'Size', '0')
                hash_val = get_first(log_entry, 'Hash', 'N/A')
                
                # Ownership is a list of objects: [{"Owner": "root", "Group": "root"}]
                owner = 'N/A'
                group = 'N/A'
                raw_ownership = log_entry.get('Ownership')
                
                if isinstance(raw_ownership, list) and len(raw_ownership) > 0:
                    first_obj = raw_ownership[0]
                    if isinstance(first_obj, dict):
                        owner = first_obj.get('Owner', 'N/A')
                        group = first_obj.get('Group', 'N/A')

                row = [
                    timestamp,
                    dev_id,
                    location,
                    size,
                    hash_val,
                    owner,
                    group
                ]

                # --- 3. Write Data ---
                output_dir.mkdir(parents=True, exist_ok=True)

                write_header = False
                
                # Check if we need to write header (New file on disk OR new in this run)
                if output_path_str not in initialized_paths:
                    if not output_file.exists():
                        write_header = True
                    initialized_paths.add(output_path_str)

                # Open in Append Mode
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
        description="Splits File Change JSON logs into folders based on DevID.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input file with file change data."
    )

    args = parser.parse_args()
    process_file_data_splitter(args.input_file)