#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <filesystem>
#include <sstream>
#include <set>
#include <unordered_map>
#include "json.hpp" 

using json = nlohmann::json;
namespace fs = std::filesystem;

// --- CONFIGURATION ---
const int BATCH_SIZE = 20000; 

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

// --- FLUSH FUNCTION ---
void flush_buffers(std::unordered_map<std::string, std::string>& buffer_map, std::set<std::string>& initialized_paths) {
    
    for (auto& [file_path, data] : buffer_map) {
        if (data.empty()) continue;

        fs::path p(file_path);
        
        if (!fs::exists(p.parent_path())) {
            fs::create_directories(p.parent_path());
        }

        bool write_header = false;
        if (initialized_paths.find(file_path) == initialized_paths.end()) {
            if (!fs::exists(p)) write_header = true;
            initialized_paths.insert(file_path);
        }

        std::ofstream outfile(file_path, std::ios::app);
        if (outfile.is_open()) {
            if (write_header) {
                outfile << "timestamp,pid,ppid,real_uid,effective_uid,saved_uid,"
                        << "filesystem_uid,real_gid,effective_gid,saved_gid,filesystem_gid,"
                        << "vm_peak,vm_size,vm_hwm,vm_rss,rss_shmem,vm_stk,vm_data,threads,name,"
                        << "state,device_id,cpu\n";
            }
            outfile << data; 
            outfile.close();
        }
    }
    buffer_map.clear();
}

int main(int argc, char* argv[]) {
    // Maximize I/O Speed
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
    std::unordered_map<std::string, std::string> buffer_map;
    
    std::string line;
    int line_count = 0;

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
            
            // FILENAME: proc-new.csv
            fs::path output_file = output_dir / "proc-new.csv";
            std::string output_path_str = output_file.string();

            // --- 2. Buffer Data ---
            std::string row = get_val(log_entry, "TimeStamp") + ","
                            + get_val(log_entry, "Pid") + ","
                            + get_val(log_entry, "PPid") + ","
                            + get_val(log_entry, "RealUid") + ","
                            + get_val(log_entry, "EffectiveUid") + ","
                            + get_val(log_entry, "SavedUid") + ","
                            + get_val(log_entry, "FilesystemUid") + ","
                            + get_val(log_entry, "RealGid") + ","
                            + get_val(log_entry, "EffectiveGid") + ","
                            + get_val(log_entry, "SavedGid") + ","
                            + get_val(log_entry, "FilesystemGid") + ","
                            + get_val(log_entry, "VmPeak") + ","
                            + get_val(log_entry, "VmSize") + ","
                            + get_val(log_entry, "VmHWM") + ","
                            + get_val(log_entry, "VmRss") + "," 
                            + get_val(log_entry, "RssShmem") + "," 
                            + get_val(log_entry, "VmStk") + ","
                            + get_val(log_entry, "VmData") + ","
                            + get_val(log_entry, "Threads") + ","
                            + get_val(log_entry, "Name") + ","
                            + get_val(log_entry, "State") + ","
                            + dev_id + ","
                            + get_val(log_entry, "Cpu", "0.0") + "\n";
            
            buffer_map[output_path_str] += row;
            line_count++;

            // --- 3. Flush Check ---
            if (line_count >= BATCH_SIZE) {
                flush_buffers(buffer_map, initialized_paths);
                line_count = 0;
            }

        } catch (...) { continue; }
    }

    // --- 4. Final Flush ---
    flush_buffers(buffer_map, initialized_paths);

    return 0;
}