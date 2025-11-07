#!/usr/bin/env python3
"""
Analyze RDMA switch buffer statistics from SwitchBufferMonitor Avro output.
Provides buffer occupancy analysis, congestion detection, and visualizations.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import defaultdict

try:
    import avro.datafile
    import avro.io
except ImportError:
    print("Error: avro-python3 package not found.")
    print("Install it with: pip install avro-python3")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("Warning: tqdm package not found. Progress bars will be disabled.")
    print("Install it with: pip install tqdm")
    # Fallback: create a no-op tqdm
    class tqdm:
        def __init__(self, iterable=None, **kwargs):
            self.iterable = iterable
        def __iter__(self):
            return iter(self.iterable) if self.iterable else iter([])
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def update(self, n=1):
            pass


def read_avro_file(filepath: str) -> List[Dict]:
    """Read Avro file and return list of records."""
    records = []
    try:
        with open(filepath, 'rb') as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            
            # Try to get file size for progress bar
            try:
                file_size = Path(filepath).stat().st_size
                with tqdm(total=file_size, unit='B', unit_scale=True, 
                         desc="Reading Avro file") as pbar:
                    last_pos = 0
                    for record in reader:
                        records.append(record)
                        # Update progress based on file position
                        current_pos = f.tell()
                        pbar.update(current_pos - last_pos)
                        last_pos = current_pos
            except:
                # Fallback: just show record count
                for record in tqdm(reader, desc="Reading Avro records", unit=" records"):
                    records.append(record)
            
            reader.close()
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading Avro file: {e}")
        sys.exit(1)
    
    return records


def records_to_dataframe(records: List[Dict]) -> pd.DataFrame:
    """Convert list of records to pandas DataFrame."""
    print("Converting records to DataFrame...")
    df = pd.DataFrame(records)
    
    # Convert bytes to KB and MB
    print("Computing derived metrics...")
    df['egress_KB'] = df['egress_bytes'] / 1024
    df['ingress_KB'] = df['ingress_bytes'] / 1024
    df['egress_MB'] = df['egress_bytes'] / (1024 * 1024)
    df['ingress_MB'] = df['ingress_bytes'] / (1024 * 1024)
    df['total_bytes'] = df['egress_bytes'] + df['ingress_bytes']
    df['total_KB'] = df['total_bytes'] / 1024
    df['total_MB'] = df['total_bytes'] / (1024 * 1024)
    
    # Create a unique identifier for each port
    df['port_id'] = df['node'].astype(str) + ':' + df['iface'].astype(str)
    
    return df


def print_basic_stats(df: pd.DataFrame):
    """Print basic statistics about switch buffer usage."""
    print("\n" + "="*70)
    print("BASIC STATISTICS")
    print("="*70)
    
    print(f"\nTotal Records: {len(df)}")
    print(f"Unique Switches: {df['node'].nunique()}")
    print(f"Unique Ports: {df['port_id'].nunique()}")
    print(f"Time Range: {df['time'].min():.6f}s - {df['time'].max():.6f}s")
    print(f"Duration: {df['time'].max() - df['time'].min():.6f}s")
    
    print(f"\nBuffer Occupancy Statistics:")
    print(f"  Egress:")
    print(f"    Max:    {df['egress_MB'].max():.2f} MB ({df['egress_KB'].max():.2f} KB)")
    print(f"    Mean:   {df['egress_MB'].mean():.2f} MB ({df['egress_KB'].mean():.2f} KB)")
    print(f"    Median: {df['egress_MB'].median():.2f} MB ({df['egress_KB'].median():.2f} KB)")
    
    print(f"  Ingress:")
    print(f"    Max:    {df['ingress_MB'].max():.2f} MB ({df['ingress_KB'].max():.2f} KB)")
    print(f"    Mean:   {df['ingress_MB'].mean():.2f} MB ({df['ingress_KB'].mean():.2f} KB)")
    print(f"    Median: {df['ingress_MB'].median():.2f} MB ({df['ingress_KB'].median():.2f} KB)")
    
    print(f"  Total:")
    print(f"    Max:    {df['total_MB'].max():.2f} MB ({df['total_KB'].max():.2f} KB)")
    print(f"    Mean:   {df['total_MB'].mean():.2f} MB ({df['total_KB'].mean():.2f} KB)")
    print(f"    Median: {df['total_MB'].median():.2f} MB ({df['total_KB'].median():.2f} KB)")


def print_switch_summary(df: pd.DataFrame):
    """Print summary statistics per switch."""
    print("\n" + "="*70)
    print("PER-SWITCH SUMMARY")
    print("="*70)
    
    for node in tqdm(sorted(df['node'].unique()), desc="Analyzing switches", unit=" switches"):
        node_df = df[df['node'] == node]
        
        print(f"\nSwitch {node}:")
        print(f"  Ports monitored: {node_df['iface'].nunique()}")
        print(f"  Samples: {len(node_df)}")
        
        print(f"  Peak Egress Buffer:  {node_df['egress_KB'].max():.2f} KB")
        print(f"  Peak Ingress Buffer: {node_df['ingress_KB'].max():.2f} KB")
        print(f"  Peak Total Buffer:   {node_df['total_KB'].max():.2f} KB")
        
        # Find the most congested port
        port_max = node_df.groupby('iface')['total_KB'].max()
        most_congested = port_max.idxmax()
        max_buffer = port_max.max()
        
        print(f"  Most congested port: {most_congested} ({max_buffer:.2f} KB peak)")


def print_port_analysis(df: pd.DataFrame, top_n: int = 10):
    """Print analysis of most congested ports."""
    print("\n" + "="*70)
    print(f"TOP {top_n} MOST CONGESTED PORTS")
    print("="*70)
    
    print("Aggregating port statistics...")
    # Calculate peak buffer usage per port
    port_stats = df.groupby('port_id').agg({
        'total_KB': ['max', 'mean', 'std'],
        'egress_KB': 'max',
        'ingress_KB': 'max',
        'node': 'first',
        'iface': 'first'
    }).reset_index()
    
    port_stats.columns = ['port_id', 'peak_total_KB', 'mean_total_KB', 'std_total_KB',
                          'peak_egress_KB', 'peak_ingress_KB', 'node', 'iface']
    
    port_stats = port_stats.sort_values('peak_total_KB', ascending=False)
    
    print(f"\n{'Rank':<6}{'Port':<12}{'Peak Total':<15}{'Peak Egress':<15}{'Peak Ingress':<15}{'Avg Total':<12}")
    print("-" * 80)
    
    for idx, row in enumerate(port_stats.head(top_n).itertuples(), 1):
        print(f"{idx:<6}{row.port_id:<12}{row.peak_total_KB:>10.2f} KB  "
              f"{row.peak_egress_KB:>10.2f} KB  {row.peak_ingress_KB:>10.2f} KB  "
              f"{row.mean_total_KB:>8.2f} KB")


def detect_congestion_events(df: pd.DataFrame, threshold_kb: float = 100):
    """Detect and report congestion events."""
    print("\n" + "="*70)
    print(f"CONGESTION EVENTS (threshold: {threshold_kb} KB)")
    print("="*70)
    
    print("Detecting congestion events...")
    congested = df[df['total_KB'] > threshold_kb].copy()
    
    if len(congested) == 0:
        print(f"\nNo congestion events detected above {threshold_kb} KB threshold.")
        return
    
    print(f"\nTotal congestion samples: {len(congested)} ({len(congested)/len(df)*100:.1f}% of all samples)")
    print(f"Affected ports: {congested['port_id'].nunique()}")
    
    # Group consecutive events
    congestion_summary = congested.groupby('port_id').agg({
        'time': ['min', 'max', 'count'],
        'total_KB': ['max', 'mean']
    }).reset_index()
    
    congestion_summary.columns = ['port_id', 'first_time', 'last_time', 'samples', 'peak_KB', 'avg_KB']
    congestion_summary['duration'] = congestion_summary['last_time'] - congestion_summary['first_time']
    congestion_summary = congestion_summary.sort_values('peak_KB', ascending=False)
    
    print(f"\nMost Affected Ports:")
    print(f"{'Port':<12}{'Peak (KB)':<12}{'Avg (KB)':<12}{'Samples':<10}{'Duration (s)':<15}")
    print("-" * 70)
    
    for row in congestion_summary.head(10).itertuples():
        print(f"{row.port_id:<12}{row.peak_KB:>8.2f}    {row.avg_KB:>8.2f}    "
              f"{row.samples:<10}{row.duration:>10.6f}")


def plot_buffer_timeline(df: pd.DataFrame, output_file: str = None, max_ports: int = 10):
    """Plot buffer occupancy over time for each port."""
    
    print(f"Generating buffer timeline plot for top {max_ports} ports...")
    
    # Select top N most congested ports
    port_max = df.groupby('port_id')['total_KB'].max().sort_values(ascending=False)
    top_ports = port_max.head(max_ports).index.tolist()
    
    df_plot = df[df['port_id'].isin(top_ports)].copy()
    
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))
    
    # Plot 1: Egress buffer over time
    for port in tqdm(top_ports, desc="Plotting egress", leave=False):
        port_data = df_plot[df_plot['port_id'] == port]
        axes[0].plot(port_data['time'], port_data['egress_KB'], 
                    label=f'Port {port}', alpha=0.7, linewidth=1)
    
    axes[0].set_xlabel('Time (s)')
    axes[0].set_ylabel('Egress Buffer (KB)')
    axes[0].set_title(f'Egress Buffer Occupancy Over Time (Top {max_ports} Ports)', 
                     fontweight='bold')
    axes[0].legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    axes[0].grid(alpha=0.3)
    
    # Plot 2: Ingress buffer over time
    for port in tqdm(top_ports, desc="Plotting ingress", leave=False):
        port_data = df_plot[df_plot['port_id'] == port]
        axes[1].plot(port_data['time'], port_data['ingress_KB'], 
                    label=f'Port {port}', alpha=0.7, linewidth=1)
    
    axes[1].set_xlabel('Time (s)')
    axes[1].set_ylabel('Ingress Buffer (KB)')
    axes[1].set_title(f'Ingress Buffer Occupancy Over Time (Top {max_ports} Ports)', 
                     fontweight='bold')
    axes[1].legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    axes[1].grid(alpha=0.3)
    
    # Plot 3: Total buffer over time
    for port in tqdm(top_ports, desc="Plotting total", leave=False):
        port_data = df_plot[df_plot['port_id'] == port]
        axes[2].plot(port_data['time'], port_data['total_KB'], 
                    label=f'Port {port}', alpha=0.7, linewidth=1)
    
    axes[2].set_xlabel('Time (s)')
    axes[2].set_ylabel('Total Buffer (KB)')
    axes[2].set_title(f'Total Buffer Occupancy Over Time (Top {max_ports} Ports)', 
                     fontweight='bold')
    axes[2].legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    axes[2].grid(alpha=0.3)
    
    plt.tight_layout()
    
    if output_file:
        print(f"Saving timeline plot to {output_file}...")
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Buffer timeline plot saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def plot_buffer_heatmap(df: pd.DataFrame, output_file: str = None):
    """Plot heatmap of buffer occupancy across ports and time."""
    
    print("Generating buffer heatmap...")
    
    # Create time bins
    print("Creating time bins...")
    time_bins = pd.cut(df['time'], bins=50)
    df_binned = df.copy()
    df_binned['time_bin'] = time_bins
    
    # Calculate average buffer usage per port per time bin
    print("Computing heatmap data...")
    heatmap_data = df_binned.groupby(['port_id', 'time_bin'])['total_KB'].mean().unstack(fill_value=0)
    
    # Sort ports by peak usage
    port_order = df.groupby('port_id')['total_KB'].max().sort_values(ascending=False).index
    heatmap_data = heatmap_data.loc[port_order]
    
    print("Rendering heatmap...")
    fig, ax = plt.subplots(figsize=(14, max(8, len(port_order) * 0.3)))
    
    sns.heatmap(heatmap_data, cmap='YlOrRd', cbar_kws={'label': 'Buffer Occupancy (KB)'},
                linewidths=0, ax=ax)
    
    ax.set_xlabel('Time Window', fontsize=12)
    ax.set_ylabel('Port', fontsize=12)
    ax.set_title('Buffer Occupancy Heatmap (Port vs Time)', fontsize=14, fontweight='bold')
    
    # Simplify x-axis labels
    n_xticks = min(10, len(ax.get_xticklabels()))
    ax.set_xticks(np.linspace(0, len(ax.get_xticklabels()), n_xticks))
    ax.set_xticklabels([f'{i}' for i in range(n_xticks)], rotation=0)
    
    plt.tight_layout()
    
    if output_file:
        print(f"Saving heatmap to {output_file}...")
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Buffer heatmap saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def plot_buffer_distribution(df: pd.DataFrame, output_file: str = None):
    """Plot distribution of buffer occupancy."""
    
    print("Generating buffer distribution plots...")
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: Distribution of egress buffer
    print("Plotting egress distribution...")
    axes[0, 0].hist(df['egress_KB'], bins=50, color='steelblue', 
                   alpha=0.7, edgecolor='black')
    axes[0, 0].set_xlabel('Egress Buffer (KB)')
    axes[0, 0].set_ylabel('Frequency')
    axes[0, 0].set_title('Egress Buffer Distribution', fontweight='bold')
    axes[0, 0].set_yscale('log')
    axes[0, 0].grid(alpha=0.3)
    
    # Plot 2: Distribution of ingress buffer
    print("Plotting ingress distribution...")
    axes[0, 1].hist(df['ingress_KB'], bins=50, color='coral', 
                   alpha=0.7, edgecolor='black')
    axes[0, 1].set_xlabel('Ingress Buffer (KB)')
    axes[0, 1].set_ylabel('Frequency')
    axes[0, 1].set_title('Ingress Buffer Distribution', fontweight='bold')
    axes[0, 1].set_yscale('log')
    axes[0, 1].grid(alpha=0.3)
    
    # Plot 3: Box plot per switch
    print("Plotting per-switch boxplot...")
    df_box = df.copy()
    df_box['node_str'] = 'SW ' + df_box['node'].astype(str)
    
    df_box.boxplot(column='total_KB', by='node_str', ax=axes[1, 0])
    axes[1, 0].set_xlabel('Switch')
    axes[1, 0].set_ylabel('Total Buffer (KB)')
    axes[1, 0].set_title('Buffer Distribution per Switch', fontweight='bold')
    axes[1, 0].get_figure().suptitle('')  # Remove automatic title
    
    # Plot 4: CDF of total buffer
    print("Plotting CDF...")
    sorted_buffer = np.sort(df['total_KB'])
    cdf = np.arange(1, len(sorted_buffer) + 1) / len(sorted_buffer)
    
    axes[1, 1].plot(sorted_buffer, cdf * 100, linewidth=2, color='green')
    axes[1, 1].set_xlabel('Total Buffer (KB)')
    axes[1, 1].set_ylabel('CDF (%)')
    axes[1, 1].set_title('Cumulative Distribution of Buffer Occupancy', fontweight='bold')
    axes[1, 1].grid(alpha=0.3)
    
    # Add percentile lines
    percentiles = [50, 90, 95, 99]
    for p in percentiles:
        val = np.percentile(sorted_buffer, p)
        axes[1, 1].axvline(x=val, color='red', linestyle='--', alpha=0.5)
        axes[1, 1].text(val, 50, f'P{p}', rotation=90, verticalalignment='center')
    
    plt.tight_layout()
    
    if output_file:
        print(f"Saving distribution plots to {output_file}...")
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Buffer distribution plots saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def export_to_csv(df: pd.DataFrame, output_file: str):
    """Export analysis to CSV files."""
    
    print("\nExporting data to CSV files...")
    
    # Export raw data
    print(f"Exporting raw data...")
    df.to_csv(output_file, index=False)
    print(f"Raw data exported to: {output_file}")
    
    # Export per-port summary
    print("Computing port summary...")
    port_summary = df.groupby('port_id').agg({
        'egress_KB': ['max', 'mean', 'std'],
        'ingress_KB': ['max', 'mean', 'std'],
        'total_KB': ['max', 'mean', 'std'],
        'time': 'count'
    }).reset_index()
    
    port_summary.columns = ['port_id', 'egress_max', 'egress_mean', 'egress_std',
                           'ingress_max', 'ingress_mean', 'ingress_std',
                           'total_max', 'total_mean', 'total_std', 'samples']
    
    port_summary_file = output_file.replace('.csv', '_port_summary.csv')
    port_summary.to_csv(port_summary_file, index=False)
    print(f"Port summary exported to: {port_summary_file}")
    
    # Export per-switch summary
    print("Computing switch summary...")
    switch_summary = df.groupby('node').agg({
        'egress_KB': ['max', 'mean'],
        'ingress_KB': ['max', 'mean'],
        'total_KB': ['max', 'mean'],
        'iface': 'nunique',
        'time': 'count'
    }).reset_index()
    
    switch_summary.columns = ['node', 'egress_max', 'egress_mean',
                             'ingress_max', 'ingress_mean',
                             'total_max', 'total_mean', 'ports', 'samples']
    
    switch_summary_file = output_file.replace('.csv', '_switch_summary.csv')
    switch_summary.to_csv(switch_summary_file, index=False)
    print(f"Switch summary exported to: {switch_summary_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze RDMA switch buffer statistics from SwitchBufferMonitor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s switch_buffer.avro
  %(prog)s switch_buffer.avro --timeline timeline.png
  %(prog)s switch_buffer.avro --heatmap heatmap.png --distribution dist.png
  %(prog)s switch_buffer.avro --csv output.csv --threshold 200
  %(prog)s switch_buffer.avro --all --output-prefix results/analysis
        """
    )
    
    parser.add_argument('input', help='Input Avro file from SwitchBufferMonitor')
    parser.add_argument('--top', type=int, default=10,
                       help='Number of top ports to show (default: 10)')
    parser.add_argument('--threshold', type=float, default=100,
                       help='Congestion threshold in KB (default: 100)')
    parser.add_argument('--timeline', metavar='FILE',
                       help='Generate buffer timeline plot')
    parser.add_argument('--heatmap', metavar='FILE',
                       help='Generate buffer heatmap')
    parser.add_argument('--distribution', metavar='FILE',
                       help='Generate distribution plots')
    parser.add_argument('--csv', metavar='FILE',
                       help='Export analysis to CSV')
    parser.add_argument('--all', action='store_true',
                       help='Generate all plots (requires --output-prefix)')
    parser.add_argument('--output-prefix', metavar='PREFIX',
                       help='Prefix for output files when using --all')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress text output')
    
    args = parser.parse_args()
    
    # Handle --all option
    if args.all:
        if not args.output_prefix:
            print("Error: --all requires --output-prefix")
            sys.exit(1)
        args.timeline = f"{args.output_prefix}_timeline.png"
        args.heatmap = f"{args.output_prefix}_heatmap.png"
        args.distribution = f"{args.output_prefix}_distribution.png"
        args.csv = f"{args.output_prefix}_data.csv"
    
    # Read data
    if not args.quiet:
        print(f"Reading Avro file: {args.input}")
    
    records = read_avro_file(args.input)
    
    if not records:
        print("Error: No records found in Avro file")
        sys.exit(1)
    
    if not args.quiet:
        print(f"Found {len(records)} buffer monitoring records")
    
    df = records_to_dataframe(records)
    
    # Print analysis
    if not args.quiet:
        print_basic_stats(df)
        print_switch_summary(df)
        print_port_analysis(df, args.top)
        detect_congestion_events(df, args.threshold)
    
    # Generate plots
    if args.timeline:
        plot_buffer_timeline(df, args.timeline, max_ports=args.top)
    
    if args.heatmap:
        plot_buffer_heatmap(df, args.heatmap)
    
    if args.distribution:
        plot_buffer_distribution(df, args.distribution)
    
    # Export CSV
    if args.csv:
        export_to_csv(df, args.csv)
    
    if not args.quiet:
        print("\n" + "="*70)
        print("Analysis complete!")
        print("="*70 + "\n")


if __name__ == '__main__':
    main()
