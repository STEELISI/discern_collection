import os
import argparse
import sys
from collections import defaultdict

"""
This program mainly count the impact of data pruning, mainly 
how many nodes, realization, topology, projects, and experiment are impacted (pruned, deleted)
in the pruning process.

The script rely on the backup folder's content to retrospect the pre-prune image.

Example: python3 prune-compare.py /path/to/current/data /path/to/backup
"""


# --- CONFIGURATION ---
# Add any folder names here that you want to skip
IGNORE_FOLDERS = {
    'tools', 
    'errorfolder',
    'synthetic',
    'merge',
    'backup'
}

def get_node_names(realization_path):
    """
    Scans a realization folder and returns a set of normalized node names.
    Expects folders like 'client-data', 'router-data'.
    Returns {'client', 'router'}
    """
    nodes = set()
    try:
        items = os.listdir(realization_path)
    except FileNotFoundError:
        return nodes

    for item in items:
        full_path = os.path.join(realization_path, item)
        if os.path.isdir(full_path) and item not in IGNORE_FOLDERS and not item.startswith('.'):
            # Normalize name: "router-data" -> "router"
            if item.endswith('-data'):
                nodes.add(item[:-5])
            else:
                nodes.add(item)
    return nodes

def get_project_name(realization_name):
    """
    Extracts PROJECT name from realization string (Last 1 part).
    Format: '..._projectname' -> 'projectname'
    """
    if '_' in realization_name:
        return realization_name.split('_')[-1]
    return "unknown"

def get_experiment_name(realization_name):
    """
    Extracts EXPERIMENT name (Last 2 parts).
    Format: '..._experiment_name' -> 'experiment_name'
    """
    parts = realization_name.split('_')
    if len(parts) >= 2:
        return f"{parts[-2]}_{parts[-1]}"
    return realization_name # Fallback if only 1 part exists

def scan_folder(base_path):
    """
    Returns a dictionary: { realization_name: set(node_names) }
    """
    result = {}
    if not os.path.exists(base_path):
        return result

    try:
        items = os.listdir(base_path)
    except OSError as e:
        print(f"Error accessing {base_path}: {e}")
        return result

    for realization_name in items:
        # --- EXCLUSION LOGIC ---
        if realization_name in IGNORE_FOLDERS or realization_name.startswith('.'):
            continue
            
        realization_path = os.path.join(base_path, realization_name)
        if os.path.isdir(realization_path):
            result[realization_name] = get_node_names(realization_path)
    return result

def main():
    parser = argparse.ArgumentParser(description="Analyze stats before and after pruning.")
    parser.add_argument("data_dir", help="Current Data Directory (Contains Clean + Pruned Realizations)")
    parser.add_argument("backup_dir", help="Backup Directory (Contains Original Pruned + Removed Realizations)")
    args = parser.parse_args()

    print(f"Scanning directories (ignoring: {', '.join(IGNORE_FOLDERS)})...")
    
    # 1. Scan Both Directories
    data_map = scan_folder(args.data_dir)
    backup_map = scan_folder(args.backup_dir)

    # 2. Reconstruct State
    all_realizations = set(data_map.keys()) | set(backup_map.keys())
    
    # -- Counters --
    stats_count = {'clean': 0, 'pruned': 0, 'removed': 0}
    
    # -- Sets for Uniqueness --
    nodes_before = set()
    nodes_after = set()
    
    # -- Data Structures --
    topology_counts = defaultdict(lambda: {'before': 0, 'after': 0})
    
    # Project: project -> {'clean': 0, 'pruned': 0, 'removed': 0}
    project_data = defaultdict(lambda: {'clean': 0, 'pruned': 0, 'removed': 0})
    project_set_before = set()
    project_set_after = set()
    
    # Experiment: experiment -> {'clean': 0, 'pruned': 0, 'removed': 0}
    experiment_data = defaultdict(lambda: {'clean': 0, 'pruned': 0, 'removed': 0})
    experiment_set_before = set()
    experiment_set_after = set()

    if not all_realizations:
        print("No realizations found. Check your paths or exclusion list.")
        return

    print(f"Analyzing {len(all_realizations)} total unique realizations...")

    for rez in all_realizations:
        # Determine Status
        in_data = rez in data_map
        in_backup = rez in backup_map
        
        status = "unknown"
        nodes = set()

        if in_data and not in_backup:
            status = 'clean'
            nodes = data_map[rez]
        elif in_data and in_backup:
            status = 'pruned'
            nodes = data_map[rez] 
        elif not in_data and in_backup:
            status = 'removed'
            nodes = backup_map[rez]
        
        # Aggregation
        stats_count[status] += 1
        
        # --- Project Logic ---
        proj_name = get_project_name(rez)
        project_data[proj_name][status] += 1
        project_set_before.add(proj_name)
        
        # --- Experiment Logic (Was Subproject) ---
        exp_name = get_experiment_name(rez)
        experiment_data[exp_name][status] += 1
        experiment_set_before.add(exp_name)

        # --- Node & Topology Logic ---
        nodes_before.update(nodes)
        if nodes:
            topo_sig = tuple(sorted(list(nodes)))
            topology_counts[topo_sig]['before'] += 1
            
            # "After" State Logic (If it wasn't removed)
            if status != 'removed':
                nodes_after.update(nodes)
                project_set_after.add(proj_name)
                experiment_set_after.add(exp_name)
                topology_counts[topo_sig]['after'] += 1

    # --- REPORTING ---
    
    # 1. Global Realization Stats
    total_before = len(all_realizations)
    total_after = stats_count['clean'] + stats_count['pruned']
    
    print("\n" + "="*60)
    print("GLOBAL REALIZATION STATS")
    print("="*60)
    print(f"{'Metric':<25} | {'Before':<10} | {'After':<10} | {'Delta':<10}")
    print("-" * 60)
    print(f"{'Total Realizations':<25} | {total_before:<10} | {total_after:<10} | -{stats_count['removed']}")
    print("-" * 60)
    print(f"  🟢 Clean (Untouched): {stats_count['clean']}")
    print(f"  🟡 Pruned (Modified): {stats_count['pruned']}")
    print(f"  🔴 Removed (Deleted): {stats_count['removed']}")

    # 2. Unique Counts
    print("\n" + "="*60)
    print("UNIQUE COMPONENT STATS")
    print("="*60)
    
    unique_topos_before = len(topology_counts)
    unique_topos_after = sum(1 for v in topology_counts.values() if v['after'] > 0)
    
    print(f"{'Component':<25} | {'Unique Before':<15} | {'Unique After':<15} | {'Loss'}")
    print("-" * 60)
    print(f"{'Nodes':<25} | {len(nodes_before):<15} | {len(nodes_after):<15} | -{len(nodes_before) - len(nodes_after)}")
    print(f"{'Projects':<25} | {len(project_set_before):<15} | {len(project_set_after):<15} | -{len(project_set_before) - len(project_set_after)}")
    print(f"{'Experiments':<25} | {len(experiment_set_before):<15} | {len(experiment_set_after):<15} | -{len(experiment_set_before) - len(experiment_set_after)}")
    print(f"{'Topologies':<25} | {unique_topos_before:<15} | {unique_topos_after:<15} | -{unique_topos_before - unique_topos_after}")

    # 3. Project Breakdown
    print("\n" + "="*60)
    print("PROJECT DETAILED BREAKDOWN")
    print("="*60)
    print(f"{'Project Name':<20} | {'Total':<6} | {'Clean':<6} | {'Pruned':<6} | {'Removed':<8}")
    print("-" * 60)
    
    sorted_projects = sorted(project_data.items(), 
                             key=lambda x: (x[1]['clean'] + x[1]['pruned'] + x[1]['removed']), 
                             reverse=True)

    for proj, counts in sorted_projects:
        total_p = counts['clean'] + counts['pruned'] + counts['removed']
        print(f"{proj:<20} | {total_p:<6} | {counts['clean']:<6} | {counts['pruned']:<6} | {counts['removed']:<8}")

    # 4. Experiment Breakdown
    print("\n" + "="*60)
    print("EXPERIMENT DETAILED BREAKDOWN")
    print("="*60)
    print(f"{'Experiment Name':<35} | {'Total':<6} | {'Clean':<6} | {'Pruned':<6} | {'Removed':<8}")
    print("-" * 80)
    
    sorted_experiments = sorted(experiment_data.items(), 
                                key=lambda x: (x[1]['clean'] + x[1]['pruned'] + x[1]['removed']), 
                                reverse=True)

    for exp_name, counts in sorted_experiments:
        total_p = counts['clean'] + counts['pruned'] + counts['removed']
        print(f"{exp_name:<35} | {total_p:<6} | {counts['clean']:<6} | {counts['pruned']:<6} | {counts['removed']:<8}")
    
    # 5. Topology Breakdown
    print("\n" + "="*60)
    print("TOPOLOGY DETAILED BREAKDOWN")
    print("="*60)
    print(f"{'Topology Nodes':<50} | {'Before':<8} | {'After':<8} | {'Status'}")
    print("-" * 90)

    sorted_topos = sorted(topology_counts.items(), key=lambda x: x[1]['before'], reverse=True)

    for nodes_tuple, counts in sorted_topos:
        before = counts['before']
        after = counts['after']
        topo_str = "[" + ", ".join(nodes_tuple) + "]"
        
        status_flag = ""
        if after == 0:
            status_flag = "🔴 FULLY REMOVED"
        elif after < before:
            status_flag = "🟡 PARTIALLY REMOVED"
            
        print(f"{topo_str:<50} | {before:<8} | {after:<8} | {status_flag}")
        
    print("="*60)

if __name__ == "__main__":
    main()