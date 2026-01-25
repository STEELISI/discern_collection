#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <sstream>
#include <set>
#include <unordered_map>
#include "json.hpp" // https://github.com/nlohmann/json

using json = nlohmann::json;
namespace fs = std::filesystem;


/* 

This program reads JSON interface data and splits it into multiple CSV files 
based on the DevID structure.
This program require the json hpp file to compile: https://github.com/nlohmann/json

Path Structure: ./[suffix_path]/[prefix]-data/network.csv
Example: client.a.b.c.d -> ./a_b_c_d/client-data/network.csv

*/


// --- CONFIGURATION ---
const int BATCH_SIZE = 50000; // Buffer 50000 packets in RAM before writing to disk

// --- Helper: Safely extract values ---
std::string get_val(const json& j, const std::string& key, const std::string& default_val = "N/A") {
    if (!j.contains(key) || j[key].is_null()) return default_val;
    const auto& val = j[key];
    
    if (val.is_string()) return val.get<std::string>();
    if (val.is_number_integer() || val.is_number_unsigned()) return std::to_string(val.get<int64_t>());
    if (val.is_number_float()) return std::to_string(val.get<double>());
    if (val.is_boolean()) return val.get<bool>() ? "true" : "false";
    
    return default_val;
}

// --- Helper: Split string ---
std::vector<std::string> split(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// --- FLUSH FUNCTION (Writes RAM buffer to Disk) ---
void flush_buffers(std::unordered_map<std::string, std::string>& buffer_map, std::set<std::string>& initialized_paths) {
    for (auto& [file_path, data] : buffer_map) {
        if (data.empty()) continue;

        fs::path p(file_path);
        
        // Ensure folder exists
        if (!fs::exists(p.parent_path())) {
            fs::create_directories(p.parent_path());
        }

        // Header Logic
        bool write_header = false;
        if (initialized_paths.find(file_path) == initialized_paths.end()) {
            if (!fs::exists(p)) write_header = true;
            initialized_paths.insert(file_path);
        }

        // Write chunk to file
        std::ofstream outfile(file_path, std::ios::app);
        if (outfile.is_open()) {
            if (write_header) {
                outfile << "timestamp,device,length,link_protocol,network_protocol,"
                        << "transport_protocol,application_protocol,ip_version,src_ip,dst_ip,"
                        << "src_port,dst_port,arp_operation,arp_protocol,arp_src_proto,arp_dst_proto,"
                        << "eth_src_mac,eth_dst_mac\n";
            }
            outfile << data; 
            outfile.close();
        }
    }
    // Clear RAM for next batch
    buffer_map.clear();
}

int main(int argc, char* argv[]) {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(NULL);

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <input_file>" << std::endl;
        return 1;
    }

    std::string input_path = argv[1];
    std::ifstream infile(input_path);
    if (!infile.is_open()) {
        std::cerr << "Error: Could not open input file " << input_path << std::endl;
        return 1;
    }

    std::set<std::string> initialized_paths;
    std::unordered_map<std::string, std::string> buffer_map; // The RAM Buffer
    
    std::string line;
    int line_count = 0;
    int processed_count = 0; // Just for visual feedback if needed

    while (std::getline(infile, line)) {
        if (line.empty()) continue;

        try {
            auto log_entry = json::parse(line, nullptr, false);
            if (log_entry.is_discarded() || !log_entry.is_object()) continue;

            // --- 1. Routing Logic ---
            std::string dev_id = get_val(log_entry, "DevID", "unknown");
            std::vector<std::string> parts = split(dev_id, '.');
            
            std::string folder_base, folder_sub;
            if (parts.size() >= 2) {
                folder_sub = parts[0] + "-data";
                folder_base = parts[1];
                for(size_t i = 2; i < parts.size(); ++i) folder_base += "_" + parts[i];
            } else {
                folder_base = "unknown_device_group";
                folder_sub = dev_id + "-data";
            }

            fs::path output_dir = fs::path(folder_base) / folder_sub;
            fs::path output_file = output_dir / "network.csv";
            std::string output_path_str = output_file.string();

            // --- 2. Extract Data (New Flat Structure) ---
            // Note: In your new data, keys are at the top level.
            
            // Core
            std::string timestamp = get_val(log_entry, "TimeStamp", "0");
            std::string device    = get_val(log_entry, "Dev");
            std::string length    = get_val(log_entry, "Length", "0");
            std::string link_p    = get_val(log_entry, "LinkProtocol");
            std::string net_p     = get_val(log_entry, "NetworkProtocol");
            std::string trans_p   = get_val(log_entry, "TransportProtocol");
            std::string app_p     = get_val(log_entry, "ApplicationProtocol");

            // IP Data (Direct keys)
            std::string ip_ver = get_val(log_entry, "Version", "N/A");
            std::string src_ip = get_val(log_entry, "SRC_IP", "N/A");
            std::string dst_ip = get_val(log_entry, "DST_IP", "N/A");

            // Ports (Direct keys)
            std::string src_port = get_val(log_entry, "SrcPort", "N/A");
            std::string dst_port = get_val(log_entry, "DstPort", "N/A");

            // ARP Data (Direct keys)
            std::string arp_op        = get_val(log_entry, "Operation", "N/A");
            std::string arp_prot      = get_val(log_entry, "Protocol", "N/A");
            std::string arp_src_proto = get_val(log_entry, "SrcProtAdd", "N/A");
            std::string arp_dst_proto = get_val(log_entry, "DstProtAdd", "N/A");

            // Ethernet Data (Direct keys)
            std::string eth_src = get_val(log_entry, "SRC_MAC", "N/A");
            std::string eth_dst = get_val(log_entry, "DST_MAC", "N/A");

            // --- 3. Append to Buffer ---
            std::string& file_buffer = buffer_map[output_path_str]; 
            
            file_buffer += timestamp + "," + device + "," + length + "," 
                         + link_p + "," + net_p + "," + trans_p + "," 
                         + app_p + "," + ip_ver + "," + src_ip + "," + dst_ip + ","
                         + src_port + "," + dst_port + "," + arp_op + "," + arp_prot + "," 
                         + arp_src_proto + "," + arp_dst_proto + ","
                         + eth_src + "," + eth_dst + "\n";

            line_count++;
            processed_count++;

            // --- 4. Check Flush Condition ---
            if (line_count >= BATCH_SIZE) {
                flush_buffers(buffer_map, initialized_paths);
                line_count = 0;
            }

        } catch (...) { continue; }
    }

    // --- 5. Final Flush ---
    flush_buffers(buffer_map, initialized_paths);

    return 0;
}
