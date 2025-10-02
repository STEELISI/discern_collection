import json
import csv
import argparse
from pathlib import Path
from datetime import datetime

def convert_packets_to_csv(input_file_path, output_file_path):
    """
    Reads JSON packet data and writes each packet as a single row to a CSV file,
    using the raw timestamp.
    """
    # Changed the header to reflect the new timestamp format
    header = [
        'timestamp', 'device', 'length', 'link_protocol', 
        'network_protocol', 'transport_protocol', 'src_ip', 'dst_ip', 
        'src_port', 'dst_port'
    ]

    with open(input_file_path, 'r') as infile, open(output_file_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)

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
                writer.writerow(row)

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
    
