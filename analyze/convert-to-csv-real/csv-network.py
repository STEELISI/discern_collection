import json
import csv
import argparse
from pathlib import Path

def process_packets_splitter(input_file_path):
    """
    Reads JSON packet data and splits it into multiple CSV files.
    Designed to be 'lossless' based on the nuclear jq structure provided.
    
    Captures:
    - Standard 5-tuple (IPs, Ports, Protocol)
    - ARP Specifics (Operation, Protocol, Addresses)
    - IP Specifics (V4 flag)
    - Ethernet & DNS details (if present)
    """
    
    initialized_paths = set()

    # --- COMPREHENSIVE HEADER ---
    header = [
        'timestamp', 
        'device', 
        'length', 
        'link_protocol', 
        'network_protocol', 
        'transport_protocol', 
        'application_protocol',
        # IP Specifics
        'ip_v4',
        'src_ip', 
        'dst_ip', 
        # TCP / UDP (Ports)
        'src_port', 
        'dst_port',
        # ARP Specifics
        'arp_operation',
        'arp_protocol',
        # Ethernet (MACs) - kept for robustness
        'eth_src_mac', 
        'eth_dst_mac',
        # DNS specific - kept for robustness
        'dns_query_id', 
        'dns_query_name' 
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

                # --- 1. Routing via Outer DevID ---
                dev_id = log_entry.get('DevID', 'unknown')
                parts = dev_id.split('.')
                
                if len(parts) >= 2:
                    folder_sub = f"{parts[0]}-data"
                    folder_base = "_".join(parts[1:])
                else:
                    folder_base = "unknown_device_group"
                    folder_sub = f"{dev_id}-data"

                output_dir = Path(folder_base) / folder_sub
                output_file = output_dir / "network.csv"
                output_path_str = str(output_file)

                # --- 2. File Setup ---
                output_dir.mkdir(parents=True, exist_ok=True)
                write_header = False
                
                if output_path_str not in initialized_paths:
                    if not output_file.exists():
                        write_header = True
                    initialized_paths.add(output_path_str)

                # --- 3. Process Packets ---
                with open(output_file, 'a', newline='') as outfile:
                    writer = csv.writer(outfile)
                    
                    if write_header:
                        writer.writerow(header)

                    packets_list = log_entry.get("Packets", [])
                    if not isinstance(packets_list, list):
                        packets_list = []

                    for packet in packets_list:
                        if not isinstance(packet, dict):
                            continue

                        # --- 4. Extract Data Fields ---
                        
                        # Basic Info
                        timestamp = packet.get('TimeStamp', 0)
                        device = packet.get('Dev', 'N/A')
                        length = packet.get('Length', 0)
                        link_proto = packet.get('LinkProtocol', 'N/A')
                        net_proto = packet.get('NetworkProtocol', 'N/A')
                        trans_proto = packet.get('TransportProtocol', 'N/A')
                        app_proto = packet.get('ApplicationProtocol', 'N/A')

                        # Initialize variables for specific protocols
                        ip_v4 = 'N/A'
                        src_ip, dst_ip = 'N/A', 'N/A'
                        src_port, dst_port = 'N/A', 'N/A'
                        arp_op, arp_prot = 'N/A', 'N/A'
                        eth_src, eth_dst = 'N/A', 'N/A'
                        dns_id, dns_name = 'N/A', 'N/A'

                        # --- IP Layer Processing ---
                        if 'IP' in packet:
                            ip_data = packet['IP']
                            src_ip = ip_data.get('SRCIP', 'N/A')
                            dst_ip = ip_data.get('DSTIP', 'N/A')
                            ip_v4 = ip_data.get('V4', 'N/A') # Capture IP.V4
                            
                            # Check for Ports (UDP/TCP are usually inside IP packets)
                            if 'UDP' in packet:
                                src_port = packet['UDP'].get('SrcPort', 'N/A')
                                dst_port = packet['UDP'].get('DstPort', 'N/A')
                            elif 'TCP' in packet:
                                src_port = packet['TCP'].get('SrcPort', 'N/A')
                                dst_port = packet['TCP'].get('DstPort', 'N/A')
                        
                        # --- ARP Layer Processing ---
                        elif 'ARP' in packet:
                            arp_data = packet['ARP']
                            src_ip = arp_data.get('SrcProtAdd', 'N/A') # Mapping ARP IPs to main IP cols
                            dst_ip = arp_data.get('DstProtAdd', 'N/A')
                            arp_op = arp_data.get('Operation', 'N/A')  # Capture ARP.Operation
                            arp_prot = arp_data.get('Protocol', 'N/A') # Capture ARP.Protocol

                        # --- Ethernet Layer ---
                        if 'ETH' in packet:
                            # Safely handle ETH if it's a dict
                            if isinstance(packet['ETH'], dict):
                                eth_src = packet['ETH'].get('SrcMac', 'N/A')
                                eth_dst = packet['ETH'].get('DstMac', 'N/A')
                            # If ETH is just a string/flag (based on your jq output), we leave as N/A

                        # --- DNS Layer ---
                        if 'DNS' in packet:
                            if isinstance(packet['DNS'], dict):
                                dns_id = packet['DNS'].get('ID', 'N/A')
                                questions = packet['DNS'].get('Query', [])
                                if isinstance(questions, list) and len(questions) > 0:
                                    dns_name = questions[0].get('Name', 'N/A')
                                elif isinstance(questions, dict):
                                    dns_name = questions.get('Name', 'N/A')

                        # Construct Row
                        row = [
                            timestamp, 
                            device, 
                            length, 
                            link_proto, 
                            net_proto, 
                            trans_proto, 
                            app_proto,
                            ip_v4,          # New
                            src_ip, 
                            dst_ip, 
                            src_port, 
                            dst_port,
                            arp_op,         # New
                            arp_prot,       # New
                            eth_src, 
                            eth_dst,
                            dns_id, 
                            dns_name
                        ]
                        
                        writer.writerow(row)

    except FileNotFoundError:
        print(f"Error: Input file not found at {input_file_path}")
    except IOError as e:
        print(f"File I/O Error: {e}")
    else:
        print(f"Processing complete. Data distributed into {len(initialized_paths)} unique CSV files.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Splits Network Packet JSON logs (Lossless based on nuclear jq scan).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("input_file", type=Path, help="Path to the input file.")
    args = parser.parse_args()
    process_packets_splitter(args.input_file)