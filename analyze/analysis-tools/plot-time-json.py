import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import argparse
import json
import gzip
from pathlib import Path

"""
Generate a binary timeline plot, showing whether data exist at a specific time.
Input: file containing json like data
Output: a plot showing the data existance over time based on the timestamp
"""


def plot_binary_coverage_json(input_file: Path, output_image: Path = None):
    if not input_file.exists():
        print(f"Error: File {input_file} not found.")
        return

    print(f"Reading timestamps from {input_file}...")
    timestamps = []

    try:
        # 1. Open file (handle .gz automatically)
        if input_file.suffix == '.gz':
            opener = gzip.open(input_file, 'rt', encoding='utf-8')
        else:
            opener = open(input_file, 'r', encoding='utf-8')

        # 2. Extract Timestamps Line-by-Line
        with opener as f:
            for line in f:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    # Check for "TimeStamp" (case-sensitive usually, but adding fallback)
                    ts = data.get('TimeStamp') or data.get('timestamp')
                    
                    if ts is not None:
                        timestamps.append(float(ts))
                except (json.JSONDecodeError, ValueError):
                    continue

        if not timestamps:
            print("Error: No valid timestamps found in file.")
            return

        # 3. Create DataFrame
        # We manually build the DF from the list of floats
        df = pd.DataFrame(timestamps, columns=['timestamp'])
        
        # 4. Clean & Convert
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
        df.sort_values(by='datetime', inplace=True)

        # 5. Plotting (The Binary Style)
        print(f"Plotting {len(df)} events...")
        plt.figure(figsize=(15, 4)) 

        # Marker '|' creates the barcode effect
        plt.scatter(df['datetime'], [1] * len(df), marker='|', s=500, alpha=0.2, color='black')

        # Formatting
        plt.title(f"Binary Data Coverage: {input_file.name}")
        plt.xlabel("Time")
        plt.yticks([]) # Remove Y-axis
        plt.ylim(0.9, 1.1) 

        # Time formatting
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d\n%H:%M'))
        plt.grid(True, axis='x', linestyle=':', alpha=0.5)
        plt.tight_layout()

        # 6. Save
        if not output_image:
            # Smart naming: replace extension with .png
            # e.g., data.json -> data.png
            # e.g., data.json.gz -> data.png
            name_stem = input_file.name.replace('.gz', '').replace('.jsonl', '').replace('.json', '')
            output_image = input_file.with_name(f"{name_stem}_coverage.png")

        plt.savefig(output_image, dpi=300)
        print(f"Binary plot saved to: {output_image}")

        # --- Stats ---
        diffs = df['datetime'].diff().dt.total_seconds()
        max_gap = diffs.max()
        if max_gap > 0:
            print(f"Largest detected gap: {max_gap:.2f} seconds")
        else:
            print("Data is perfectly continuous.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate binary timeline from JSON logs.")
    parser.add_argument("input_file", type=Path, help="Path to JSON or JSON.GZ file.")
    parser.add_argument("-o", "--output", type=Path, help="Output PNG path.")
    
    args = parser.parse_args()
    
    plot_binary_coverage_json(args.input_file, args.output)
