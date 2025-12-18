import json
import csv
import argparse
from pathlib import Path

def process_proc_mem_splitter(input_file_path):
    """
    Reads JSON process memory data and splits it into multiple CSV files 
    based on the DevID structure.
    
    Path Structure: ./[suffix_path]/[prefix]-data/proc-mem.csv
    """
    
    # Track initialized files to avoid redundant disk checks
    initialized_paths = set()

    # Header includes 'rss_shmem' as requested
    header = [
        'timestamp', 'pid', 'ppid', 'real_uid', 'effective_uid', 'saved_uid',
        'filesystem_uid', 'real_gid', 'effective_gid', 'saved_gid', 'filesystem_gid',
        'vm_peak', 'vm_size', 'vm_hwm', 'rss_shmem', 'vm_stk', 'vm_data', 'threads', 'name',
        'state', 'device_id', 'cpu'
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
                output_file = output_dir / "proc-mem.csv"
                output_path_str = str(output_file)

                # --- 2. Extract Data Fields ---
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
                    log_entry.get('RssShmem', 'N/A'),
                    log_entry.get('VmStk', 'N/A'),
                    log_entry.get('VmData', 'N/A'),
                    log_entry.get('Threads', 'N/A'),
                    log_entry.get('Name', 'N/A'),
                    log_entry.get('State', 'N/A'),
                    dev_id,
                    log_entry.get('Cpu', 0.0)
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
        description="Splits Process Memory JSON logs into folders based on DevID.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="Path to the input file with process memory data."
    )

    args = parser.parse_args()
    process_proc_mem_splitter(args.input_file)