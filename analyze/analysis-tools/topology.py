import os
import argparse
from collections import defaultdict

"""
This script scans all the experiment in the provided project root directory,
and produce a listing of type of topologies that the whole dataset contains.
Topology is determined by the name of the nodes under the same realization.
"""

def scan_experiment_topologies(root_dir):
    """
    Scans a directory of experiments, parses [nodename]-data folders, 
    and groups experiments by their exact node topology.
    """
    
    if not os.path.exists(root_dir):
        print(f"Error: Directory '{root_dir}' does not exist.")
        return

    # Dictionary to group experiments
    # Key: Tuple of sorted, clean node names (The Topology Signature)
    # Value: List of experiment folder names
    topology_groups = defaultdict(list)

    print(f"Scanning directory: {root_dir} ...\n")

    try:
        items = os.listdir(root_dir)
    except OSError as e:
        print(f"Error accessing directory: {e}")
        return

    for experiment_name in items:
        experiment_path = os.path.join(root_dir, experiment_name)
        if experiment_name in {'tools','errorfolder'}:
            continue
        if not os.path.isdir(experiment_path):
            continue
            
        # --- PARSING LOGIC START ---
        node_names = []
        try:
            sub_items = os.listdir(experiment_path)
            for item in sub_items:
                item_path = os.path.join(experiment_path, item)
                
                # We only care about directories that are not hidden
                if os.path.isdir(item_path) and not item.startswith('.'):
                    
                    # Remove the '-data' suffix if present to get the clean node name
                    if item.endswith('-data'):
                        clean_name = item[:-5] # remove last 5 chars (-data)
                    else:
                        clean_name = item # fallback if naming convention differs
                        
                    node_names.append(clean_name)
                    
        except OSError:
            print(f"Warning: Could not access contents of {experiment_name}")
            continue
        # --- PARSING LOGIC END ---

        # Create Signature (Sorted to ignore folder order)
        if not node_names:
            signature = ("<Empty Experiment - No Nodes>",)
        else:
            signature = tuple(sorted(node_names))

        # Group by signature
        topology_groups[signature].append(experiment_name)

    # Output Results
    print("="*60)
    print(f"TOTAL UNIQUE TOPOLOGIES FOUND: {len(topology_groups)}")
    print("="*60)
    
    # Sort groups by count (descending)
    sorted_groups = sorted(topology_groups.items(), key=lambda x: len(x[1]), reverse=True)

    for i, (nodes, experiments) in enumerate(sorted_groups, 1):
        print(f"\nTOPOLOGY GROUP #{i}")
        print(f"Node Composition ({len(nodes)} nodes):")
        # Print clean node names
        print(f"  [{', '.join(nodes)}]")
        
        print(f"\nExperiment Count: {len(experiments)}")
        print("Experiment Names:")
        
        for exp in sorted(experiments):
            print(f"  - {exp}")
            
        print("-" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan and group experiments by node topology (stripping -data).")
    parser.add_argument("path", help="Path to the main directory containing experiment folders.")
    
    args = parser.parse_args()
    
    scan_experiment_topologies(args.path)
