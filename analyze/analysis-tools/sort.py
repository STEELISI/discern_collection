import pandas as pd
import argparse
from pathlib import Path
import sys

def sort_csv_by_time(input_file: Path, output_file: Path = None, verbose: bool = False):
    if not input_file.exists():
        print(f"Error: Input file {input_file} does not exist.")
        return

    try:
        if verbose:
            print(f"Reading {input_file}...")

        # 1. Read CSV
        # low_memory=False helps if files are huge and mixed types appear
        df = pd.read_csv(input_file, low_memory=False)

        if df.empty:
            print(f"Warning: File {input_file} is empty. Skipping.")
            return

        # 2. Find the Timestamp Column
        # We look for common names. You can add more if your data changes.
        possible_time_cols = ['timestamp', 'TimeStamp', 'time', 'Time', 'date', 'Date']
        time_col = None

        for col in df.columns:
            if col in possible_time_cols:
                time_col = col
                break
        
        # Fallback: if explicit match not found, check case-insensitive match
        if not time_col:
            for col in df.columns:
                if col.lower() in ['timestamp', 'time']:
                    time_col = col
                    break

        if not time_col:
            print(f"Error: Could not find a timestamp column in {input_file}.")
            print(f"Available columns: {list(df.columns)}")
            return

        if verbose:
            print(f"Detected timestamp column: '{time_col}'")

        # 3. Sort
        # We assume the timestamp is sortable (int, float, or ISO string).
        # If it's a messy string, we force it to numeric/datetime for sorting purposes only.
        
        # Optimization: Sort directly if it looks numeric
        if pd.api.types.is_numeric_dtype(df[time_col]):
             df.sort_values(by=[time_col], inplace=True)
        else:
            # If it's not strictly numeric, try converting to datetime temporarily to get the sort index
            # This handles mixed string formats better
            temp_sort_col = pd.to_datetime(df[time_col], errors='coerce')
            # If conversion fails completely (all NaT), fall back to string sort
            if temp_sort_col.isna().all():
                 df.sort_values(by=[time_col], inplace=True)
            else:
                 # Use the argsort of the temp column to reorder the real dataframe
                 df = df.iloc[temp_sort_col.argsort()]

        # 4. Write Output
        target_file = output_file if output_file else input_file
        
        if verbose:
            print(f"Writing sorted data to {target_file}...")
            
        df.to_csv(target_file, index=False)
        
        if verbose:
            print("Done.")

    except Exception as e:
        print(f"Critical Error processing {input_file}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sort any CSV file based on its timestamp column."
    )
    parser.add_argument(
        "input_file", 
        type=Path,
        help="Path to the input CSV file."
    )
    parser.add_argument(
        "-o", "--output", 
        type=Path, 
        help="Path to save sorted file. If omitted, OVERWRITES input file."
    )
    parser.add_argument(
        "-v", "--verbose", 
        action="store_true", 
        help="Show processing details."
    )

    args = parser.parse_args()
    
    sort_csv_by_time(args.input_file, args.output, args.verbose)
