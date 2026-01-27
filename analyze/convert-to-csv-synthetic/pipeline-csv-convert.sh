#!/bin/bash

# --- Configuration ---

# Set the base directory to search within.
BASE_DIR="./../../../discern_data/synthetic/legitimate/"

# Set the directory containing your Python script.
SCRIPT_DIR="."

# --- Script and File Definitions ---

# Array of prefixes for the different data types to process.
# This allows the script to dynamically handle each type.
DATA_TYPES=(
    "network"
    # "cpu-load"
    # "file"
    # "proc-cpu"
    # "proc-mem"
    # "proc-new"
    "interfaces"
)

# --- Pre-run Checks ---

if [[ ! -d "$BASE_DIR" ]]; then
  echo "Error: Base directory not found at $BASE_DIR"
  exit 1
fi

# --- Main Pipeline ---

# Loop through each defined data type.
for data_type in "${DATA_TYPES[@]}"; do
    
    # Dynamically set the script, input, and output filenames based on the type.
    PYTHON_SCRIPT="csv-${data_type}.py"
    TARGET_INPUT="${data_type}-data.txt"
    OUTPUT_FILENAME="${data_type}-res.csv"

    echo "--- Starting processing for data type: $data_type ---"

    script_file_path="$SCRIPT_DIR/$PYTHON_SCRIPT"
    if [[ ! -f "$script_file_path" ]]; then
      echo "   Warning: Script not found at '$script_file_path'. Skipping this type."
      continue
    fi

    # Use find to locate all input files for the current data type.
    find "$BASE_DIR" -mindepth 4 -maxdepth 4 -type f -name "$TARGET_INPUT" -print0 | while IFS= read -r -d $'\0' input_file_path; do
        echo "   Processing file: '$input_file_path'"

        # The output directory is the same as the input file's directory.
        output_dir=$(dirname "$input_file_path")
        output_file_path="$output_dir/$OUTPUT_FILENAME"

        echo "      Output will be saved to: '$output_file_path'"

        # Run the corresponding Python script.
        python3 "$script_file_path" "$input_file_path" -o "$output_file_path"

        # Report the exit status of the Python script.
        exit_status=$?
        if [[ $exit_status -ne 0 ]]; then
            echo "      Warning: Python script for '$input_file_path' exited with status $exit_status."
        fi
        echo "   ---"

    done
    echo "--- Finished processing for data type: $data_type ---"
    echo ""
done


echo "Processing finished."