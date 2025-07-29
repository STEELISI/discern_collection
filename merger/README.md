# Log Data Fusion & Processing Script

This script automates the process of merging and processing log files from two separate data source directories. It combines their corresponding log files, synchronizes timestamps, and applies specialized data processing rules to create a unified dataset.

## Features

* **Automatic Directory Pairing**: Discovers all subdirectories in the two main source folders and processes every possible combination (Cartesian product).
* **Timestamp Synchronization**: Aligns the timestamps from the second data source to the timeline of the first, creating a unified chronological sequence.
* **Unified Device ID**: Merges the `DevID` from both sources into a single, combined identifier (e.g., `a.b.c` and `a.d.c` become `a.b-d.c`).
* **Specialized Data Handling**:
    * **CPU Data (`cpu-load-data.txt`)**: Uses `pandas` to intelligently merge entries with the same timestamp by summing their CPU core loads.
    * **Network Data (`network-data.txt`)**: Employs a dedicated pipeline to correctly process files with nested timestamps, preserving their internal accuracy.
* **Structured Output**: Generates a clean, organized output directory structure that mirrors the combination of the source folders.


## 🚀 Usage

To use the script, you need to configure the source and output paths directly within the `main()` function in the script file.

1.  **Open the script** and navigate to the `main()` function.

2.  **Set the folder paths** in the configuration section:

    ```python
    # --- Configuration ---
    # Path to the first parent directory containing data subfolders
    parent_folder1 = '../../DISCERN/data/legitimate/synflood/0/'

    # Path to the second parent directory containing data subfolders
    parent_folder2 = '../../DISCERN/data/malicious/internetscanner/0/'

    # The base directory where all merged output will be saved
    base_output_folder = '../../DISCERN/data/merged/'

    # Optional: Apply a global time offset (delay) in seconds to all timestamps in a file
    time_offset_file1 = 0
    time_offset_file2 = 0
    ```

3.  **Run the script** from your terminal:
    ```bash
    python merge.py
    ```

The script will print its progress to the console, indicating which combination of folders it is currently processing.

---

## 🧠 How It Works

### Directory Structure

The script expects the following directory structure:
```
<path_to_legitimate_data>/
└── <legit_activity>/                      <- This is parent_folder1
    ├── <legit_machine_1-data>/
    │   ├── cpu-load-data.txt
    │   ├── network-data.txt
    │   ├── proc-cpu-data.txt              <- Must exist
    │   └── ... (other common data files)
    └── <legit_machine_2-data>/
        ├── cpu-load-data.txt
        ├── network-data.txt
        ├── proc-cpu-data.txt              <- Must exist
        └── ... (other common data files)

<path_to_malicious_data>/
└── <mal_activity>/                        <- This is parent_folder2
    ├── <mal_machine_X-data>/
    │   ├── cpu-load-data.txt
    │   ├── network-data.txt
    │   ├── proc-cpu-data.txt              <- Must exist
    │   └── ... (other common data files)
    └── <mal_machine_Y-data>/
        ├── cpu-load-data.txt
        ├── network-data.txt
        ├── proc-cpu-data.txt              <- Must exist
        └── ... (other common data files)

```


## Expected output format

```
<base_output_folder>/
└── <legit_activity>_<mal_activity>/
    └── <run_number>/
        ├── <legit_machine_1>_<mal_machine_A>/
        │   ├── cpu-load-data.txt
        │   ├── file-data.txt
        │   ├── network-data.txt
        │   ├── proc-cpu-data.txt
        │   ├── proc-mem-data.txt
        │   ├── proc-new-data.txt
        │   └── malicious_time.txt
        ├── <legit_machine_1>_<mal_machine_B>/
        │   ├── cpu-load-data.txt
        │   ├── file-data.txt
        │   ├── network-data.txt
        │   ├── proc-cpu-data.txt
        │   ├── proc-mem-data.txt
        │   ├── proc-new-data.txt
        │   └── malicious_time.txt
        ├── <legit_machine_2>_<mal_machine_A>/
        │   ├── cpu-load-data.txt
        │   ├── file-data.txt
        │   ├── network-data.txt
        │   ├── proc-cpu-data.txt
        │   ├── proc-mem-data.txt
        │   ├── proc-new-data.txt
        │   └── malicious_time.txt
        └── <legit_machine_2>_<mal_machine_B>/
            ├── cpu-load-data.txt
            ├── file-data.txt
            ├── network-data.txt
            ├── proc-cpu-data.txt
            ├── proc-mem-data.txt
            ├── proc-new-data.txt
            └── malicious_time.txt
```

### Timestamp Synchronization

The core of the merging logic is timestamp synchronization. To align the data from `node2` with `node1`, the script:
1.  Finds the earliest timestamp ($T_{n1\_start}$) in a node in `exp1`. This serves as the baseline for the new, unified timeline, the earliest timestamp is determined by `proc-cpu-data.txt`.
2.  Finds the earliest timestamp ($T_{n2\_start}$) in a node in `exp2`.
3.  A random generated number ($R$) is used for later malicious start time calculation, $R$ is smaller than the difference in timespan for `exp1` and `exp2`; the merge only runs when `exp1` has a longer timespan.

    $$1<=R <=T_{1span} - T_{2span}$$

4.  A malicious start time is calculated based of one of the nodes from `exp1`. The earliest timestamp ($T_{s1\_start}$)

    $$T_{malicious\_start} = T_{s1\_start} + R$$

5.  For each subsequent timestamp ($T_{current}$) in the node from `exp2`, it calculates the new timestamp ($T_{new}$) as:

    $$T_{new} = T_{current} -T_{n2\_start} + T_{n1\_start} + (T_{malicious\_start} - T_{n1\_start}) $$
    $$T_{new} = T_{current} -T_{n2\_start} +T_{malicious\_start}$$

Notice that $T_{s1\_start}$ does not neccessarily equal to $T_{n1\_start}$.

This ensures that the relative timing of events between different nodes is preserved, as if the `exp2` starts running at $T_{malicious\_start}$.


### Data Processing Logic

* **Generic Files**: For most files, the script reads each line (which is a JSON object), applies the timestamp synchronization, unifies the `DevID`, and appends the result to a list.
* **CPU Load Data**: After initial merging, this data is loaded into a `pandas` DataFrame. It's grouped by `TimeStamp`, and all CPU `Load` arrays for a given timestamp are summed element-wise. This handles cases where two records have the same timestamp by combining their core loads.
* **Network Data**: This file is processed in an isolated function because it contains nested timestamps within a `"Packets"` array. The processing logic ensures that both the primary `TimeStamp` and all nested timestamps are correctly synchronized.
