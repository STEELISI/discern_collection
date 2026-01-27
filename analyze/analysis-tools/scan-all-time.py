import gzip
import json
import glob
import argparse
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from collections import Counter
from pathlib import Path
"""
This program scan and bin all the files. Outputing a plot showing the volumn
of data across time.

Example: python3 scan-all-time.py cpu-load.jsonl-*.gz -o cpu-volumn.png
"""
# --- CONFIGURATION ---
# 4K Resolution (approx)
FIG_WIDTH_INCHES = 20
FIG_HEIGHT_INCHES = 10
DPI = 200  # 20 * 200 = 4000 pixels wide

def parse_and_bin_files(file_pattern, bin_seconds=300):
    bin_counts = Counter()
    files = sorted(glob.glob(str(file_pattern)))
    total_files = len(files)
    
    if total_files == 0:
        print(f"No files found matching: {file_pattern}")
        return None

    print(f"Found {total_files} files. Processing (Bin Size: {bin_seconds}s)...")
    
    # Calculate start of bins to align them nicely (e.g. to the hour)
    
    valid_lines = 0
    for idx, filepath in enumerate(files):
        if idx % 10 == 0: print(f"Processing file {idx+1}/{total_files}...", end='\r')
        
        try:
            with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                for line in f:
                    if not line.strip(): continue
                    try:
                        data = json.loads(line)
                        ts = float(data.get('TimeStamp', 0))
                        if ts == 0: continue
                        
                        # Round down to nearest bin
                        bin_start = int(ts // bin_seconds * bin_seconds)
                        bin_counts[bin_start] += 1
                        valid_lines += 1
                    except:
                        continue
        except Exception as e:
            print(f"Skipping {filepath}: {e}")
            continue

    print(f"\nProcessing complete. Total logs: {valid_lines}")
    return bin_counts

def plot_histogram(bin_counts, output_file, bin_seconds=300):
    if not bin_counts:
        print("No data.")
        return

    print("Generating 4K Plot...")
    
    # 1. Prepare Data
    df = pd.DataFrame.from_dict(bin_counts, orient='index', columns=['count'])
    df.index = pd.to_datetime(df.index, unit='s')
    df.sort_index(inplace=True)
    
    # Resample to ensure empty bins (gaps) are actually 0
    freq_str = f"{bin_seconds}S"
    df = df.resample(freq_str).sum().fillna(0)
    
    # 2. Setup 4K Figure
    plt.figure(figsize=(FIG_WIDTH_INCHES, FIG_HEIGHT_INCHES), dpi=DPI)
    
    # 3. Plot Raw Volume (Blue Area)
    # Using linewidth=0 ensures the border doesn't dominate the fill color at high density
    plt.fill_between(df.index, df['count'], step="pre", color="#1f77b4", alpha=0.5, label="Log Volume (Raw)", linewidth=0)
    
    # 4. Plot Smoothed Trend (Orange Line)
    # This helps visualize the 'average' volume per bin over time, ignoring jagged spikes
    # Window of 12 bins = 1 Hour (if bin is 5 mins)
    window = int(3600 / bin_seconds) 
    if window > 1:
        df['smooth'] = df['count'].rolling(window=window, center=True).mean()
        plt.plot(df.index, df['smooth'], color="#ff7f0e", linewidth=1.5, label=f"1-Hour Moving Avg")

    # 5. Highlight Gaps (Red marks at bottom)
    zeros = df[df['count'] == 0]
    if not zeros.empty:
        # Plot markers at y=0. size=5 is small but visible on 4K
        plt.scatter(zeros.index, [0]*len(zeros), color='red', s=10, marker='|', label='Zero Data Gap', zorder=10)

    # 6. Formatting
    plt.title(f"Data Collection Volume (30 Days) | Bin: {bin_seconds}s | Total Logs: {int(df['count'].sum())}", fontsize=16)
    plt.ylabel("Logs per Bin", fontsize=12)
    plt.xlabel("Date (UTC)", fontsize=12)
    plt.grid(True, which='both', linestyle='--', linewidth=0.5, alpha=0.5)
    plt.legend(loc="upper right", fontsize=12)

    # Smart Date Ticks (1 Tick per Day)
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    plt.gcf().autofmt_xdate()

    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=DPI)
        print(f"Saved High-Res Graph to: {output_file}")
    else:
        plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file_pattern", type=str, help="Input pattern (e.g. 'cpu-load.jsonl-*.gz')")
    parser.add_argument("-o", "--output", type=str, default="volume_4k.png", help="Output filename")
    
    # DEFAULT CHANGED TO 300 (5 Minutes)
    parser.add_argument("--bin", type=int, default=300, help="Bin size in seconds (default: 300 / 5min)")
    
    args = parser.parse_args()
    
    counts = parse_and_bin_files(args.file_pattern, args.bin)
    if counts:
        plot_histogram(counts, args.output, args.bin)
