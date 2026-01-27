import pandas as pd
import numpy as np
from pathlib import Path
import argparse
import sys

def process_cpu_load_csv_time_weighted(file_path: Path, verbose: bool = False):
    """
    Reads a CSV containing timestamp, device_id, and load (cpu-load), and calculates 
    time-weighted average CPU usage per device.
    Example: python3 cpu-sum.py ./path-to-dir/cpu-load.csv
    """
    
    if not file_path.is_file():
        print(f"Error: Input file not found at {file_path}")
        return None

    if verbose:
        print(f"Processing CSV file: {file_path}...")

    try:
        # 1. Read CSV directly using Pandas
        df = pd.read_csv(file_path)
        
        # Check if necessary columns exist
        required_columns = {'timestamp', 'device_id', 'load_core_0'}
        if not required_columns.issubset(df.columns):
            print(f"Error: CSV is missing required columns. Found: {df.columns.tolist()}")
            return None

        # 2. Normalize Column Names to match Logic (Internal Standardization)
        df.rename(columns={
            'timestamp': 'TimeStamp', 
            'device_id': 'DevID', 
            'load_core_0': 'CpuLoad'
        }, inplace=True)

        # 3. Data Cleaning
        # Convert TimeStamp to numeric (int) to ensure sorting works
        df['TimeStamp'] = pd.to_numeric(df['TimeStamp'], errors='coerce')
        # Convert Load to numeric
        df['CpuLoad'] = pd.to_numeric(df['CpuLoad'], errors='coerce')
        
        # Drop rows with bad data
        original_len = len(df)
        df.dropna(subset=['TimeStamp', 'DevID', 'CpuLoad'], inplace=True)
        
        if verbose and (len(df) < original_len):
            print(f"Dropped {original_len - len(df)} rows due to missing/invalid data.")

        if df.empty:
            print("DataFrame is empty after cleaning.")
            return None

        # 4. Convert TimeStamp to Datetime for accurate duration calculation
        # (Assuming unix timestamp input based on your sample)
        df['DateTime'] = pd.to_datetime(df['TimeStamp'], unit='s')

        # 5. Sort Data
        if verbose:
            print("Sorting data by DevID and TimeStamp...")
        df.sort_values(by=['DevID', 'DateTime'], inplace=True)

        # 6. Calculate Durations (Time-Weighted Logic)
        # Shift(-1) gets the time of the NEXT reading for the same device
        df['NextTime'] = df.groupby('DevID')['DateTime'].shift(-1)
        
        # Duration is the time *until* the next reading comes in
        # We use .dt.total_seconds() to get a float value
        df['Duration'] = (df['NextTime'] - df['DateTime']).dt.total_seconds()
        
        # Calculate Weighted Load for this interval
        df['LoadTimesDuration'] = df['CpuLoad'] * df['Duration'].fillna(0)

        # 7. Aggregation
        summary = df.groupby('DevID').agg(
            SumLoadTimesDuration=('LoadTimesDuration', 'sum'),
            TotalDurationSeconds=('Duration', 'sum'),
            MaxCpuUsage=('CpuLoad', 'max'),
            DataPoints=('CpuLoad', 'count')
        ).reset_index()

        # 8. Calculate Final Average
        # Average = (Sum of Weighted Loads) / (Total Time Duration)
        summary['AvgCpuUsage'] = summary.apply(
            lambda row: row['SumLoadTimesDuration'] / row['TotalDurationSeconds']
                        if row['TotalDurationSeconds'] > 0 else 0.0,
            axis=1
        )

        # Select and Reorder final columns
        final_summary = summary[[
            'DevID', 'AvgCpuUsage', 'MaxCpuUsage',
            'DataPoints', 'TotalDurationSeconds'
        ]]

        if verbose:
            print("Calculation complete.")
            
        return final_summary

    except Exception as e:
        print(f"An error occurred during processing: {e}")
        return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Summarize CPU CSV data (Time-Weighted Average)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "input_file", 
        type=Path,
        help="Path to the input CSV file (must have timestamp, device_id, load_core_0)"
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path('cpu-summary.csv'),
        help="Path to save the output summary CSV file."
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging."
    )

    args = parser.parse_args()

    # Process
    summary_df = process_cpu_load_csv_time_weighted(args.input_file, verbose=args.verbose)

    if summary_df is not None and not summary_df.empty:
        # Display settings for console print
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.float_format', '{:.2f}'.format)
        
        # Output to file
        try:
            summary_df.to_csv(args.output, index=False)
            if args.verbose:
                print(f"Summary successfully saved to: {args.output}")
            else:
                # Minimal output if not verbose
                print(f"Saved: {args.output}")
        except Exception as e:
            print(f"Error saving output file: {e}")
    else:
        print("No valid data generated.")
