#!/usr/bin/env python3
"""
PFC Monitor Analysis Script

Analyzes PFC (Priority Flow Control) events from NS-3 simulation output.
Provides statistics and visualizations of pause/resume events.
"""

import argparse
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import numpy as np

try:
    import fastavro
except ImportError:
    print("Error: fastavro library not found.")
    print("Install it with: pip install fastavro")
    exit(1)


def read_avro_file(filepath):
    """Read Avro file and return records as a list of dictionaries."""
    records = []
    with open(filepath, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            records.append(record)
    return records


def analyze_pfc_events(df):
    """Generate basic statistics about PFC events."""
    
    print("=" * 80)
    print("PFC EVENT STATISTICS")
    print("=" * 80)
    
    # Basic counts
    total_events = len(df)
    pause_events = df[df['paused'] == True].shape[0]
    resume_events = df[df['paused'] == False].shape[0]
    
    print(f"\nTotal PFC events: {total_events}")
    print(f"  Pause events:  {pause_events} ({pause_events/total_events*100:.1f}%)")
    print(f"  Resume events: {resume_events} ({resume_events/total_events*100:.1f}%)")
    
    # Per-node statistics
    print("\n" + "-" * 80)
    print("PER-NODE STATISTICS")
    print("-" * 80)
    
    for node_id in sorted(df['node'].unique()):
        node_df = df[df['node'] == node_id]
        node_pauses = node_df[node_df['paused'] == True].shape[0]
        node_resumes = node_df[node_df['paused'] == False].shape[0]
        
        print(f"\nNode {node_id}:")
        print(f"  Total events: {len(node_df)}")
        print(f"  Pauses:  {node_pauses}")
        print(f"  Resumes: {node_resumes}")
        
        # Per-device statistics for this node
        for dev_id in sorted(node_df['dev'].unique()):
            dev_df = node_df[node_df['dev'] == dev_id]
            dev_pauses = dev_df[dev_df['paused'] == True].shape[0]
            dev_resumes = dev_df[dev_df['paused'] == False].shape[0]
            print(f"    Device {dev_id}: {dev_pauses} pauses, {dev_resumes} resumes")
    
    return {
        'total_events': total_events,
        'pause_events': pause_events,
        'resume_events': resume_events
    }


def calculate_pause_durations(df):
    """Calculate how long each node/device was paused."""
    
    print("\n" + "=" * 80)
    print("PAUSE DURATION ANALYSIS")
    print("=" * 80)
    
    durations = []
    
    # Group by node and device
    for (node_id, dev_id), group in df.groupby(['node', 'dev']):
        group = group.sort_values('time')
        
        pause_start = None
        total_paused_time = 0
        pause_count = 0
        
        for _, row in group.iterrows():
            if row['paused'] and pause_start is None:
                # Start of a pause period
                pause_start = row['time']
            elif not row['paused'] and pause_start is not None:
                # End of a pause period
                duration = row['time'] - pause_start
                total_paused_time += duration
                pause_count += 1
                durations.append({
                    'node': node_id,
                    'dev': dev_id,
                    'duration': duration
                })
                pause_start = None
        
        # Handle case where simulation ends during pause
        if pause_start is not None:
            final_time = group['time'].max()
            duration = final_time - pause_start
            total_paused_time += duration
            durations.append({
                'node': node_id,
                'dev': dev_id,
                'duration': duration
            })
        
        if pause_count > 0:
            avg_duration = total_paused_time / pause_count
            print(f"\nNode {node_id}, Device {dev_id}:")
            print(f"  Pause count: {pause_count}")
            print(f"  Total paused time: {total_paused_time:.6f}s")
            print(f"  Average pause duration: {avg_duration:.6f}s")
    
    return pd.DataFrame(durations) if durations else pd.DataFrame()


def plot_pfc_timeline(df, output_dir):
    """Plot PFC events over time."""
    
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Create a unique identifier for each node-device pair
    df['node_dev'] = df['node'].astype(str) + '-' + df['dev'].astype(str)
    unique_pairs = sorted(df['node_dev'].unique())
    
    # Assign y-coordinates to each node-device pair
    y_coords = {pair: i for i, pair in enumerate(unique_pairs)}
    
    # Plot pause events (filled rectangles)
    for (node_id, dev_id), group in df.groupby(['node', 'dev']):
        group = group.sort_values('time')
        node_dev_id = f"{node_id}-{dev_id}"
        y_pos = y_coords[node_dev_id]
        
        pause_start = None
        
        for _, row in group.iterrows():
            if row['paused'] and pause_start is None:
                pause_start = row['time']
            elif not row['paused'] and pause_start is not None:
                # Draw pause period
                duration = row['time'] - pause_start
                ax.barh(y_pos, duration, left=pause_start, height=0.8, 
                       color='red', alpha=0.6, edgecolor='darkred')
                pause_start = None
        
        # Handle ongoing pause at end
        if pause_start is not None:
            duration = group['time'].max() - pause_start
            ax.barh(y_pos, duration, left=pause_start, height=0.8, 
                   color='red', alpha=0.6, edgecolor='darkred')
    
    ax.set_yticks(range(len(unique_pairs)))
    ax.set_yticklabels([f"Node {pair}" for pair in unique_pairs])
    ax.set_xlabel('Time (s)', fontsize=12)
    ax.set_ylabel('Node-Device', fontsize=12)
    ax.set_title('PFC Pause Events Timeline', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    output_file = Path(output_dir) / 'pfc_timeline.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\nTimeline plot saved to: {output_file}")
    plt.close()


def plot_pause_duration_histogram(duration_df, output_dir):
    """Plot histogram of pause durations."""
    
    if duration_df.empty:
        print("\nNo pause durations to plot.")
        return
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Histogram
    axes[0].hist(duration_df['duration'], bins=30, color='steelblue', 
                edgecolor='black', alpha=0.7)
    axes[0].set_xlabel('Pause Duration (s)', fontsize=12)
    axes[0].set_ylabel('Frequency', fontsize=12)
    axes[0].set_title('Distribution of Pause Durations', fontsize=14, fontweight='bold')
    axes[0].grid(True, alpha=0.3)
    
    # Box plot per node
    if len(duration_df['node'].unique()) > 1:
        duration_df.boxplot(column='duration', by='node', ax=axes[1])
        axes[1].set_xlabel('Node', fontsize=12)
        axes[1].set_ylabel('Pause Duration (s)', fontsize=12)
        axes[1].set_title('Pause Durations by Node', fontsize=14, fontweight='bold')
        axes[1].get_figure().suptitle('')  # Remove automatic title
    else:
        axes[1].text(0.5, 0.5, 'Only one node present', 
                    ha='center', va='center', fontsize=14)
        axes[1].axis('off')
    
    plt.tight_layout()
    output_file = Path(output_dir) / 'pfc_duration_histogram.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Duration histogram saved to: {output_file}")
    plt.close()


def plot_event_frequency(df, output_dir):
    """Plot frequency of PFC events over time."""
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Create time bins
    time_bins = np.linspace(df['time'].min(), df['time'].max(), 50)
    df['time_bin'] = pd.cut(df['time'], bins=time_bins)
    
    # Count events per bin
    event_counts = df.groupby(['time_bin', 'paused']).size().unstack(fill_value=0)
    
    # Get bin centers for plotting
    bin_centers = [(interval.left + interval.right) / 2 for interval in event_counts.index]
    
    if True in event_counts.columns:
        ax.plot(bin_centers, event_counts[True], 'r-', marker='o', 
               label='Pause Events', linewidth=2)
    if False in event_counts.columns:
        ax.plot(bin_centers, event_counts[False], 'g-', marker='s', 
               label='Resume Events', linewidth=2)
    
    ax.set_xlabel('Time (s)', fontsize=12)
    ax.set_ylabel('Event Count', fontsize=12)
    ax.set_title('PFC Event Frequency Over Time', fontsize=14, fontweight='bold')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_file = Path(output_dir) / 'pfc_event_frequency.png'
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Event frequency plot saved to: {output_file}")
    plt.close()


def export_summary(df, stats, duration_df, output_dir):
    """Export summary statistics to a text file."""
    
    output_file = Path(output_dir) / 'pfc_summary.txt'
    
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("PFC MONITOR ANALYSIS SUMMARY\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Simulation time range: {df['time'].min():.6f}s - {df['time'].max():.6f}s\n")
        f.write(f"Total duration: {df['time'].max() - df['time'].min():.6f}s\n\n")
        
        f.write(f"Total PFC events: {stats['total_events']}\n")
        f.write(f"  Pause events:  {stats['pause_events']}\n")
        f.write(f"  Resume events: {stats['resume_events']}\n\n")
        
        if not duration_df.empty:
            f.write("Pause Duration Statistics:\n")
            f.write(f"  Mean:   {duration_df['duration'].mean():.6f}s\n")
            f.write(f"  Median: {duration_df['duration'].median():.6f}s\n")
            f.write(f"  Min:    {duration_df['duration'].min():.6f}s\n")
            f.write(f"  Max:    {duration_df['duration'].max():.6f}s\n")
            f.write(f"  Std:    {duration_df['duration'].std():.6f}s\n\n")
        
        f.write("Nodes affected: " + ", ".join(map(str, sorted(df['node'].unique()))) + "\n")
    
    print(f"\nSummary exported to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze PFC monitor output from NS-3 simulation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic analysis
  python analyze_pfc.py pfc_events.avro
  
  # Specify output directory
  python analyze_pfc.py pfc_events.avro -o results/
  
  # Skip plots (only show statistics)
  python analyze_pfc.py pfc_events.avro --no-plots
        """
    )
    
    parser.add_argument('input', type=str, help='Input Avro file path')
    parser.add_argument('-o', '--output', type=str, default='pfc_analysis',
                       help='Output directory for plots and summary (default: pfc_analysis)')
    parser.add_argument('--no-plots', action='store_true',
                       help='Skip generating plots')
    parser.add_argument('--csv', type=str, default=None,
                       help='Export data to CSV file')
    
    args = parser.parse_args()
    
    # Check input file
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}")
        return 1
    
    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Read data
    print(f"Reading Avro file: {input_path}")
    records = read_avro_file(input_path)
    
    if not records:
        print("Error: No records found in Avro file")
        return 1
    
    print(f"Loaded {len(records)} PFC events")
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Export to CSV if requested
    if args.csv:
        csv_path = Path(args.csv)
        df.to_csv(csv_path, index=False)
        print(f"\nData exported to CSV: {csv_path}")
    
    # Analyze events
    stats = analyze_pfc_events(df)
    
    # Calculate pause durations
    duration_df = calculate_pause_durations(df)
    
    # Generate plots
    if not args.no_plots:
        print("\n" + "=" * 80)
        print("GENERATING PLOTS")
        print("=" * 80)
        
        plot_pfc_timeline(df, output_dir)
        plot_pause_duration_histogram(duration_df, output_dir)
        plot_event_frequency(df, output_dir)
    
    # Export summary
    export_summary(df, stats, duration_df, output_dir)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"Results saved to: {output_dir.absolute()}")
    
    return 0


if __name__ == '__main__':
    exit(main())
