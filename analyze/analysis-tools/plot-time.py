import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import argparse
from pathlib import Path
"""
Generate a binary timeline and print gap times, gap should be larger than
gap _threshold to be consider a gap.
Input: *.csv
Output: 1. a plot in png showing the data existance over time based on the timestamp
        2. a print to standard output showing the gap times

"""
def plot_binary_coverage(input_file: Path, output_image: Path = None, gap_threshold: float = 30.0):
    if not input_file.exists():
        print(f"Error: File {input_file} not found.")
        return

    print(f"Reading timestamps from {input_file}...")
    
    try:
        # 1. Quick Header Scan
        header = pd.read_csv(input_file, nrows=0)
        col_names = header.columns.tolist()

        # Robust auto-detection for timestamp column
        time_col = next((c for c in col_names if c.lower() in ['timestamp', 'time', 'datetime', 'date']), None)

        if not time_col:
            print(f"Error: No timestamp column found. Columns: {col_names}")
            return
        
        # 2. Read ONLY the timestamp column
        df = pd.read_csv(input_file, usecols=[time_col])

        # 3. Clean & Convert
        df[time_col] = pd.to_numeric(df[time_col], errors='coerce')
        df.dropna(subset=[time_col], inplace=True)
        
        # Convert to Datetime
        df['datetime'] = pd.to_datetime(df[time_col], unit='s')
        df.sort_values(by='datetime', inplace=True)
        
        # Drop duplicates for cleaner gap calculation
        df.drop_duplicates(subset=['datetime'], inplace=True)

        if df.empty:
            print("Error: File contains no valid timestamp data.")
            return

        # 4. Gap Detection Logic
        # Calculate time difference between current row and PREVIOUS row
        df['prev_datetime'] = df['datetime'].shift(1)
        df['time_diff'] = (df['datetime'] - df['prev_datetime']).dt.total_seconds()
        
        # Filter for gaps larger than threshold
        gaps = df[df['time_diff'] > gap_threshold].copy()

        # 5. Print Gaps to Terminal
        print(f"\n--- Gap Analysis (Threshold: >{gap_threshold}s) ---")
        
        if not gaps.empty:
            print(f"{'Start Time (Gap Begins)':<25} | {'End Time (Data Resumes)':<25} | {'Duration (s)':<10}")
            print("-" * 75)
            
            for index, row in gaps.iterrows():
                start = row['prev_datetime']
                end = row['datetime']
                duration = row['time_diff']
                
                print(f"{str(start):<25} | {str(end):<25} | {duration:.2f}")
        else:
            print("No significant data gaps detected.")

        # 6. Plotting (Original Binary Style)
        print(f"\nPlotting {len(df)} events...")
        
        plt.figure(figsize=(15, 4)) 

        # Plot every event at Y=1
        plt.scatter(df['datetime'], [1] * len(df), marker='|', s=500, alpha=0.2, color='black')

        # Formatting
        plt.title(f"Binary Data Coverage: {input_file.name}")
        plt.xlabel("Time")
        plt.yticks([]) 
        plt.ylim(0.9, 1.1)

        # Nice Time Formatting
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d\n%H:%M:%S'))
        plt.grid(True, axis='x', linestyle=':', alpha=0.5)
        plt.tight_layout()

        # 7. Save
        if not output_image:
            output_image = input_file.with_suffix('.png')
        
        plt.savefig(output_image, dpi=300)
        print(f"Binary plot saved to: {output_image}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a binary timeline and print gap times.")
    parser.add_argument("input_file", type=Path, help="Path to CSV file.")
    parser.add_argument("-o", "--output", type=Path, help="Output PNG path.")
    parser.add_argument("-t", "--threshold", type=float, default=30.0, help="Gap threshold in seconds (default: 30).")
    
    args = parser.parse_args()
    
    plot_binary_coverage(args.input_file, args.output, args.threshold)
