#!/bin/bash

# ==============================================================================
# CONFIGURATION: Processing Tasks
# Format: "PYTHON_SCRIPT_FILENAME|FILE_PATTERN"
# We assume all python scripts are inside the ./tools/ directory.
# ==============================================================================
declare -a TASKS=(
    "csv-interfaces.py|interfaces.jsonl-*.gz"
    "csv-file.py|file.jsonl-*.gz"
    "csv-network.py|network.jsonl-*.gz"
    "csv-proc-cpu.py|proc-cpu.jsonl-*.gz"
    "csv-proc-new.py|proc-new.jsonl-*.gz"
    "csv-proc-mem.py|proc-mem.jsonl-*.gz"
)

TOOLS_DIR="./tools"

# ==============================================================================
# MAIN PROCESSING LOOP
# ==============================================================================

echo "Starting Global Batch Processing..."

# OUTER LOOP: Iterate through each Task (Script + Pattern)
for task in "${TASKS[@]}"; do
    
    # 1. Split the task string into Script and Pattern
    # ${task%|*} gets everything BEFORE the |
    # ${task#*|} gets everything AFTER the |
    SCRIPT_NAME="${task%|*}"
    FILE_PATTERN="${task#*|}"
    
    PYTHON_SCRIPT="$TOOLS_DIR/$SCRIPT_NAME"

    echo "================================================================"
    echo "TASK START: $SCRIPT_NAME on files matching '$FILE_PATTERN'"
    echo "================================================================"

    # Check if Python script exists
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        echo "CRITICAL ERROR: Script '$PYTHON_SCRIPT' not found. Skipping this task."
        continue
    fi

    # INNER LOOP: Process files matching the specific pattern
    # We use 'compgen' to safely check if files exist to avoid loop errors
    files=$(compgen -G "$FILE_PATTERN")
    
    if [ -z "$files" ]; then
        echo "No files found matching: $FILE_PATTERN. Skipping."
        continue
    fi

    for gz_file in $files; do
        
        echo "Processing: $gz_file"

        # A. Decompress (Keep original)
        gunzip -k -d "$gz_file"

        # B. Determine uncompressed filename
        uncompressed_file="${gz_file%.gz}"

        # C. SAFETY CHECK: Ensure we didn't get a weird filename
        if [[ "$uncompressed_file" == *".gz"* ]]; then
            echo "CRITICAL SAFETY ERROR: Filename '$uncompressed_file' still contains '.gz'."
            echo "Skipping this file to prevent accidental deletion."
            continue
        fi

        # D. Run Python Script & Cleanup
        if [ -f "$uncompressed_file" ]; then
            echo "  -> Running Python splitter..."
            python "$PYTHON_SCRIPT" "$uncompressed_file"
            
            # Check exit code of Python script (0 = Success)
            if [ $? -eq 0 ]; then
                echo "  -> Success. Removing temp file: $uncompressed_file"
                rm "$uncompressed_file"
            else
                echo "  -> PYTHON ERROR. Keeping temp file for debugging: $uncompressed_file"
            fi
        else
            echo "Error: Uncompressed file '$uncompressed_file' not found."
        fi

    done
    echo "Task complete for $SCRIPT_NAME"
    echo ""

done

echo "================================================================"
echo "All processing tasks finished."