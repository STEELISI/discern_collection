import json
import csv
import argparse
from pathlib import Path

def process_cpu_load_splitter(input_file_path):
    """
    Single-pass Router Script.
    Reads JSON stream line-by-line and immediately appends to the correct CSV based on DevID.
    
    Path Structure: ./[suffix_path]/[prefix]-data/cpu-load.csv
    Example: client.a.b.c.d -> ./a_b_c_d/client-data/cpu-load.csv
    """
    
    # Track which files we have already set up (headers written) in this run.
    # This avoids expensive disk checks (os.path.exists) for every single line.
    initialized_paths = set()

    try:
        # SINGLE PASS: Open the main input file once
        with open(input_file_path, 'r') as infile:
            for line in infile:
                if not line.strip(): 
                    continue
                
                try:
                    log_entry = json.loads(line)
                    # Skip invalid types (like plain strings or numbers in the JSON stream)
                    if not isinstance(log_entry, dict):
                        continue
                except json.JSONDecodeError:
                    continue

                # --- 1. Parse DevID to Determine Output Path ---
                dev_id = log_entry.get('DevID', 'unknown')
                parts = dev_id.split('.')
                
                if len(parts) >= 2:
                    # Example: attacker.bnsdq.ozxfs...
                    # prefix = attacker
                    # suffix = bnsdq.ozxfs...
                    folder_sub = f"{parts[0]}-data"
                    folder_base = "_".join(parts[1:])
                else:
                    # Fallback for weird DevIDs
                    folder_base = "unknown_device_group"
                    folder_sub = f"{dev_id}-data"

                output_dir = Path(folder_base) / folder_sub
                output_file = output_dir / "cpu-load.csv"
                output_path_str = str(output_file)

                # --- 2. Normalize Load Data ---
                # Handle scalar (19.0) vs List ([0.1, 0.2])
                raw_load = log_entry.get('Load')
                if isinstance(raw_load, list):
                    loads = raw_load
                elif isinstance(raw_load, (int, float, str)):
                    try:
                        loads = [float(raw_load)]
                    except ValueError:
                        loads = []
                else:
                    loads = []

                # --- 3. Write/Append Data ---
                timestamp = log_entry.get('TimeStamp', 'N/A')
                row = [timestamp, dev_id] + loads

                # Create folders if they don't exist
                output_dir.mkdir(parents=True, exist_ok=True)

                # Determine header logic
                write_header = False
                
                # Check 1: Have we seen this file in this script run?
                if output_path_str not in initialized_paths:
                    # Check 2: Does it exist physically on disk?
                    if not output_file.exists():
                        write_header = True
                    # Mark as initialized so we skip disk checks for future lines
                    initialized_paths.add(output_path_str)

                # Open the target CSV in Append mode ('a')
                with open(output_file, 'a', newline='') as outfile:
                    writer = csv.writer(outfile)
                    
                    if write_header:
                        header = ['timestamp', 'device_id']
                        # Create headers based on current line's load count
                        for i in range(len(loads)):
                            header.append(f'load_core_{i}')
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
        description="Single-pass Router: Splits JSON logs into folders based on DevID.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("input_file", type=Path, help="Path to input JSON data.")
    
    args = parser.parse_args()
    process_cpu_load_splitter(args.input_file)
