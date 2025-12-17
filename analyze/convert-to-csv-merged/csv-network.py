import json
import csv
import argparse
from pathlib import Path

def convert_packets_to_csv(input_file_path, output_file_path):
    """
    Reads JSON packet data and writes each packet as a single row to a CSV file,
    using the raw timestamp. Conditionally adds a 'malicious' column if the
    field is present in the source data.
    """
    # --- Check for 'malicious' field in the first line ---
    has_malicious_field = False
    try:
        with open(input_file_path, 'r') as f:
            # Read the first non-empty line
            for line in f:
                if line.strip():
                    if 'malicious' in json.loads(line):
                        has_malicious_field = True
                    break 
    except (json.JSONDecodeError, FileNotFoundError):
        # Ignore if file is empty, not found, or first line is malformed
        pass

    header = [
        'timestamp', 'device', 'length', 'link_protocol', 
        'network_protocol', 'transport_protocol', 'src_ip', 'dst_ip', 
        'src_port', 'dst_port'
    ]
    if has_malicious_field:
        header.append('malicious')

    all_packets = []
    try:
        with open(input_file_path, 'r') as infile:
            for line in infile:
                try:
                    log_entry = json.loads(line)
                except json.JSONDecodeError:
                    print(f"Warning: Skipping invalid JSON line: {line.strip()}")
                    continue

                for packet in log_entry.get("Packets", []):
                    # --- Get the timestamp directly without conversion ---
                    timestamp = packet.get('TimeStamp', 0)
                    
                    src_ip, dst_ip = 'N/A', 'N/A'
                    src_port, dst_port = 0, 0

                    if 'IP' in packet:
                        src_ip = packet['IP'].get('SRCIP', 'N/A')
                        dst_ip = packet['IP'].get('DSTIP', 'N/A')
                        if 'UDP' in packet:
                            src_port = packet['UDP'].get('SrcPort', 0)
                            dst_port = packet['UDP'].get('DstPort', 0)
                        elif 'TCP' in packet:
                            src_port = packet['TCP'].get('SrcPort', 0)
                            dst_port = packet['TCP'].get('DstPort', 0)
                    
                    elif 'ARP' in packet:
                        src_ip = packet['ARP'].get('SrcProtAdd', 'N/A')
                        dst_ip = packet['ARP'].get('DstProtAdd', 'N/A')

                    row = [
                        timestamp,
                        packet.get('Dev', 'N/A'),
                        packet.get('Length', 0),
                        packet.get('LinkProtocol', 'N/A'),
                        packet.get('NetworkProtocol', 'N/A'),
                        packet.get('TransportProtocol', 'N/A'),
                        src_ip,
                        dst_ip,
                        src_port,
                        dst_port
                    ]
                    if has_malicious_field:
                        row.append(log_entry.get('malicious', 0))
                    all_packets.append(row)
    except FileNotFoundError:
        print(f"Error: Input file not found at '{input_file_path}'")
        return # Exit if the file doesn't exist

    # --- Sort the collected packets by timestamp ---
    try:
        all_packets.sort(key=lambda x: int(x[0]))
    except (ValueError, IndexError) as e:
        print(f"Warning: Could not sort packet data due to an error: {e}. The output may not be in chronological order.")


    # --- Write the sorted data to the CSV file ---
    with open(output_file_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        writer.writerows(all_packets)

    print(f"Successfully converted all packets to '{output_file_path}'")


# if __name__ == "__main__":
#     input_file = "./../../../discern_data/synthetic/legitimate/svm/0/learner-data/network-data.txt"
#     output_file = "packets_output.csv"
    
#     convert_packets_to_csv(input_file, output_file)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Parse and process the (json-like) data collected from the discern project to display the summary",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument(
        "input_file", 
        type=Path,
        help="Path to the input file change log (network-data.txt)"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default='network-summary.csv',
        help="Path to save the output summary CSV file."
    )
    

    args = parser.parse_args()
    convert_packets_to_csv(args.input_file, args.output)
    
