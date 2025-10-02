#!/bin/bash

# --- Configuration ---

# Set the base directory to search within.
BASE_DIR="./../../../discern_data/synthetic/malicious/"

# Set the directory containing your Python script.
SCRIPT_DIR="."

# --- Script and File Definitions ---

# The specific Python script to run.
PYTHON_SCRIPT="csv-network.py"

# The specific input filename to search for.
TARGET_INPUT="network-data.txt"

# --- Pre-run Checks ---

if [[ ! -d "$BASE_DIR" ]]; then
  echo "Error: Base directory not found at $BASE_DIR"
  exit 1
fi

script_file_path="$SCRIPT_DIR/$PYTHON_SCRIPT"
if [[ ! -f "$script_file_path" ]]; then
  echo "Error: Python script not found at '$script_file_path'"
  exit 1
fi

# --- Main Pipeline ---

# Use find to locate only files named 'network-data.txt' four levels deep.
find "$BASE_DIR" -mindepth 4 -maxdepth 4 -type f -name "$TARGET_INPUT" -print0 | while IFS= read -r -d $'\0' input_file_path; do
    echo "Processing file: '$input_file_path'"

    # Get the directory of the input file. This is the new, simplified output path.
    output_dir=$(dirname "$input_file_path")

    # The output filename is fixed since we only process network data.
    output_filename="network-res.csv"
    output_file_path="$output_dir/$output_filename"

    echo "   Output will be saved to: '$output_file_path'"

    # Run the Python script with the input and new output paths.
    python3 "$script_file_path" "$input_file_path" -o "$output_file_path"

    # Report the exit status of the Python script.
    exit_status=$?
    if [[ $exit_status -ne 0 ]]; then
        echo "   Warning: Python script exited with status $exit_status."
    fi
    echo "---"

done

echo "Processing finished."