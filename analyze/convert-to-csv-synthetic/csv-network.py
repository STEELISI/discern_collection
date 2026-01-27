import json
import csv
import argparse
import sys
from pathlib import Path

def get_val(data, key, default="N/A"):
    """Safely extracts value from dictionary, handling integers/bools/strings."""
    val = data.get(key)
    if val is None:
        return default
    if isinstance(val, bool):
        return "true" if val else "false"
    return str(val)

def check_for_malicious_field(input_file_path):
    """Scans the first valid line to see if 'malicious' field exists."""
    try:
        with open(input_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    if "malicious" in data:
                        return True
                    # If we parsed a valid line and didn't find it, assume it's not there
                    return False 
                except json.JSONDecodeError:
                    continue
    except:
        pass
    return False

def convert_packets_to_csv(input_file_path, output_file_path):
    # 1. Check for malicious field
    has_malicious = check_for_malicious_field(input_file_path)
    
    # 2. Define Base Header (Matches C++ output format)
    header = [
        "timestamp", "device", "length", "link_protocol", "network_protocol",
        "transport_protocol", "application_protocol", "ip_version", "src_ip", "dst_ip",
        "src_port", "dst_port", "arp_operation", "arp_protocol", "arp_src_proto", "arp_dst_proto",
        "eth_src_mac", "eth_dst_mac"
    ]
    
    # Conditionally add malicious column
    if has_malicious:
        header.append("malicious")

    packets_buffer = []

    print(f"Reading {input_file_path}...")
    if has_malicious:
        print("  > Detected 'malicious' field. Including in output.")

    try:
        with open(input_file_path, 'r', encoding='utf-8') as infile:
            for line in infile:
                if not line.strip(): continue

                try:
                    log_entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Extract top-level malicious flag (defaults to 0 if missing but header exists)
                is_malicious = get_val(log_entry, "malicious", "0")

                # Iterate through the Nested Packets list
                for packet in log_entry.get("Packets", []):
                    
                    # --- 1. Extract Core Data ---
                    timestamp = get_val(packet, "TimeStamp", "0")
                    device    = get_val(packet, "Dev", "N/A")
                    length    = get_val(packet, "Length", "0")
                    link_p    = get_val(packet, "LinkProtocol", "N/A")
                    net_p     = get_val(packet, "NetworkProtocol", "N/A")
                    trans_p   = get_val(packet, "TransportProtocol", "N/A")
                    app_p     = get_val(packet, "ApplicationProtocol", "N/A")

                    # --- 2. Initialize Protocol Fields ---
                    ip_ver = "N/A"
                    src_ip, dst_ip = "N/A", "N/A"
                    src_port, dst_port = "N/A", "N/A"
                    arp_op, arp_prot = "N/A", "N/A"
                    arp_src, arp_dst = "N/A", "N/A"
                    eth_src, eth_dst = "", "" 

                    # --- 3. Nested Extraction Logic ---
                    
                    # IP Nesting
                    if "IP" in packet:
                        ip_data = packet["IP"]
                        src_ip = get_val(ip_data, "SRCIP")
                        dst_ip = get_val(ip_data, "DSTIP")
                        
                        # Version Heuristics
                        if ip_data.get("V4") is True:
                            ip_ver = "v4"
                        elif net_p == "IPv6" or ":" in src_ip:
                            ip_ver = "v6"
                        elif "Version" in ip_data:
                            ip_ver = get_val(ip_data, "Version")

                    # TCP/UDP Nesting
                    if "UDP" in packet:
                        src_port = get_val(packet["UDP"], "SrcPort")
                        dst_port = get_val(packet["UDP"], "DstPort")
                    elif "TCP" in packet:
                        src_port = get_val(packet["TCP"], "SrcPort")
                        dst_port = get_val(packet["TCP"], "DstPort")

                    # ARP Nesting
                    if "ARP" in packet:
                        arp_data = packet["ARP"]
                        arp_op = get_val(arp_data, "Operation")
                        arp_prot = get_val(arp_data, "Protocol")
                        arp_src = get_val(arp_data, "SrcProtAdd")
                        arp_dst = get_val(arp_data, "DstProtAdd")

                    # Ethernet Nesting
                    if "ETH" in packet:
                        eth_data = packet["ETH"]
                        if "SRC_MAC" in eth_data: eth_src = get_val(eth_data, "SRC_MAC")
                        if "DST_MAC" in eth_data: eth_dst = get_val(eth_data, "DST_MAC")

                    # --- 4. Build Row ---
                    row = {
                        "timestamp": timestamp,
                        "device": device,
                        "length": length,
                        "link_protocol": link_p,
                        "network_protocol": net_p,
                        "transport_protocol": trans_p,
                        "application_protocol": app_p,
                        "ip_version": ip_ver,
                        "src_ip": src_ip,
                        "dst_ip": dst_ip,
                        "src_port": src_port,
                        "dst_port": dst_port,
                        "arp_operation": arp_op,
                        "arp_protocol": arp_prot,
                        "arp_src_proto": arp_src,
                        "arp_dst_proto": arp_dst,
                        "eth_src_mac": eth_src,
                        "eth_dst_mac": eth_dst
                    }
                    
                    if has_malicious:
                        row["malicious"] = is_malicious
                        
                    packets_buffer.append(row)

    except FileNotFoundError:
        print(f"Error: Input file {input_file_path} not found.")
        sys.exit(1)

    # --- Sort by Timestamp ---
    try:
        packets_buffer.sort(key=lambda x: float(x['timestamp']))
    except ValueError:
        pass

    # --- Write to CSV ---
    print(f"Writing {len(packets_buffer)} rows to {output_file_path}...")
    try:
        with open(output_file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=header)
            writer.writeheader()
            writer.writerows(packets_buffer)
        print("Done.")
    except IOError as e:
        print(f"Error writing output file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert nested JSON network logs to flat CSV.")
    parser.add_argument("input_file", type=Path, help="Path to input network-data.txt")
    parser.add_argument("-o", "--output", type=Path, default="network.csv", help="Path to output CSV")

    args = parser.parse_args()
    convert_packets_to_csv(args.input_file, args.output)