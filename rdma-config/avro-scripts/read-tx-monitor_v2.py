#!/usr/bin/env python3
"""
Analyze RDMA network traffic from TxMonitor Avro output.
Provides statistics, visualizations, throughput, and traffic analysis.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Dict, Tuple
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

try:
    import avro.datafile
    import avro.io
except ImportError:
    print("Error: avro-python3 package not found.")
    print("Install it with: pip install avro-python3")
    sys.exit(1)


def read_avro_file(filepath: str) -> List[Dict]:
    """Read Avro file and return list of records."""
    records = []
    try:
        with open(filepath, 'rb') as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            for record in reader:
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
    df = pd.DataFrame(records)
    
    # Convert bytes to more readable units
    df['MB'] = df['bytes'] / (1024 * 1024)
    df['GB'] = df['bytes'] / (1024 * 1024 * 1024)
    
    return df


def print_basic_stats(df: pd.DataFrame):
    """Print basic statistics about the traffic."""
    print("\n" + "="*60)
    print("BASIC STATISTICS")
    print("="*60)
    
    total_bytes = df['bytes'].sum()
    total_mb = total_bytes / (1024 * 1024)
    total_gb = total_bytes / (1024 * 1024 * 1024)
    
    print(f"\nTotal Links: {len(df)}")
    print(f"Total Traffic: {total_bytes:,} bytes")
    print(f"               {total_mb:.2f} MB")
    print(f"               {total_gb:.4f} GB")
    
    print(f"\nTraffic per link:")
    print(f"  Mean:   {df['MB'].mean():.2f} MB")
    print(f"  Median: {df['MB'].median():.2f} MB")
    print(f"  Std:    {df['MB'].std():.2f} MB")
    print(f"  Min:    {df['MB'].min():.2f} MB")
    print(f"  Max:    {df['MB'].max():.2f} MB")
    
    unique_src = df['src'].nunique()
    unique_dst = df['dst'].nunique()
    
    print(f"\nUnique source nodes: {unique_src}")
    print(f"Unique destination nodes: {unique_dst}")


def print_top_talkers(df: pd.DataFrame, n: int = 10):
    """Print top N nodes by sent/received traffic."""
    print("\n" + "="*60)
    print(f"TOP {n} TALKERS")
    print("="*60)
    
    # Top senders
    top_senders = df.groupby('src')['MB'].sum().sort_values(ascending=False).head(n)
    print(f"\nTop {n} Senders:")
    print("-" * 30)
    for idx, (node, mb) in enumerate(top_senders.items(), 1):
        print(f"{idx:2d}. Node {node:3d}: {mb:10.2f} MB ({mb/1024:.4f} GB)")
    
    # Top receivers
    top_receivers = df.groupby('dst')['MB'].sum().sort_values(ascending=False).head(n)
    print(f"\nTop {n} Receivers:")
    print("-" * 30)
    for idx, (node, mb) in enumerate(top_receivers.items(), 1):
        print(f"{idx:2d}. Node {node:3d}: {mb:10.2f} MB ({mb/1024:.4f} GB)")


def print_heaviest_flows(df: pd.DataFrame, n: int = 10):
    """Print top N heaviest flows (src->dst pairs)."""
    print("\n" + "="*60)
    print(f"TOP {n} HEAVIEST FLOWS")
    print("="*60)
    
    df_sorted = df.sort_values('MB', ascending=False).head(n)
    
    print(f"\n{'Rank':<6}{'Source':<8}{'Dest':<8}{'Traffic':<20}{'Percentage':<10}")
    print("-" * 60)
    
    total_mb = df['MB'].sum()
    for idx, row in enumerate(df_sorted.itertuples(), 1):
        pct = (row.MB / total_mb) * 100
        print(f"{idx:<6}{row.src:<8}{row.dst:<8}{row.MB:10.2f} MB      {pct:6.2f}%")


def create_traffic_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Create a traffic matrix (src x dst)."""
    matrix = df.pivot_table(
        values='MB',
        index='src',
        columns='dst',
        aggfunc='sum',
        fill_value=0
    )
    return matrix


def plot_traffic_heatmap(df: pd.DataFrame, output_file: str = None):
    """Plot traffic heatmap."""
    matrix = create_traffic_matrix(df)
    
    plt.figure(figsize=(12, 10))
    
    if matrix.max().max() / matrix[matrix > 0].min().min() > 100:
        matrix_plot = np.log10(matrix + 0.001)
        cbar_label = 'Traffic (log10 MB)'
    else:
        matrix_plot = matrix
        cbar_label = 'Traffic (MB)'
    
    sns.heatmap(
        matrix_plot,
        cmap='YlOrRd',
        cbar_kws={'label': cbar_label},
        linewidths=0.5,
        linecolor='gray'
    )
    
    plt.title('Traffic Matrix (Source → Destination)', fontsize=14, fontweight='bold')
    plt.xlabel('Destination Node', fontsize=12)
    plt.ylabel('Source Node', fontsize=12)
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\nHeatmap saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def analyze_throughput(df: pd.DataFrame):
    """Compute average throughput per node using per-node active time."""
    print("\n" + "="*60)
    print("AVERAGE THROUGHPUT PER NODE")
    print("="*60)

    if 'time' not in df.columns:
        print("No 'time' field available for throughput computation.")
        return None, None, None

    # Compute per-node durations
    node_time_ranges = df.groupby('src')['time'].agg(['min', 'max'])
    node_durations = (node_time_ranges['max'] - 0.0).clip(lower=1e-12)  # avoid divide-by-zero
    #print("NODE DURATIONS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    #print(f"Node durations: {node_durations}")
    node_bytes = df.groupby('src')['bytes'].sum()

    src_throughput_bps = node_bytes / node_durations  # bytes per second
    src_throughput_mb_s = src_throughput_bps / (1024 * 1024)
    src_throughput_mbps = src_throughput_bps * 8 / 1e6

    dst_time_ranges = df.groupby('dst')['time'].agg(['min', 'max'])
    dst_durations = (dst_time_ranges['max'] - dst_time_ranges['min']).clip(lower=1e-12)
    dst_bytes = df.groupby('dst')['bytes'].sum()

    dst_throughput_bps = dst_bytes / dst_durations
    dst_throughput_mb_s = dst_throughput_bps / (1024 * 1024)
    dst_throughput_mbps = dst_throughput_bps * 8 / 1e6

    # Print summaries
    print(f"\nAverage Source Throughput: {src_throughput_mb_s.mean():.3f} MB/s")
    print(f"Average Destination Throughput: {dst_throughput_mb_s.mean():.3f} MB/s")

    print("\nTop 10 Source Nodes by Throughput (MB/s):")
    print(src_throughput_mb_s.sort_values(ascending=False).head(10))

    print("\nTop 10 Destination Nodes by Throughput (MB/s):")
    print(dst_throughput_mb_s.sort_values(ascending=False).head(10))

    # Return for plotting
    return src_throughput_mb_s, dst_throughput_mb_s, (df['time'].min(), df['time'].max())

def analyze_traffic_patterns(df: pd.DataFrame):
    """Analyze traffic patterns and print insights."""
    print("\n" + "="*60)
    print("TRAFFIC PATTERN ANALYSIS")
    print("="*60)
    
    total_traffic = df['MB'].sum()
    df_sorted = df.sort_values('MB', ascending=False)
    
    top_10_pct = (df_sorted.head(10)['MB'].sum() / total_traffic) * 100
    top_20_pct = (df_sorted.head(20)['MB'].sum() / total_traffic) * 100
    
    print(f"\nTraffic Concentration:")
    print(f"  Top 10 flows carry {top_10_pct:.1f}% of total traffic")
    print(f"  Top 20 flows carry {top_20_pct:.1f}% of total traffic")
    
    src_degree = df.groupby('src').size()
    dst_degree = df.groupby('dst').size()
    
    print(f"\nNode Connectivity:")
    print(f"  Avg outgoing connections per node: {src_degree.mean():.1f}")
    print(f"  Avg incoming connections per node: {dst_degree.mean():.1f}")
    print(f"  Max outgoing connections: {src_degree.max()}")
    print(f"  Max incoming connections: {dst_degree.max()}")
    
    src_traffic = df.groupby('src')['MB'].sum()
    dst_traffic = df.groupby('dst')['MB'].sum()
    
    if len(src_traffic) > 0 and len(dst_traffic) > 0:
        src_cv = src_traffic.std() / src_traffic.mean()
        dst_cv = dst_traffic.std() / dst_traffic.mean()
        
        print(f"\nTraffic Imbalance (Coefficient of Variation):")
        print(f"  Source nodes: {src_cv:.2f}")
        print(f"  Destination nodes: {dst_cv:.2f}")
        print(f"  (Higher values indicate more imbalanced traffic)")


def plot_traffic_distribution(df: pd.DataFrame, output_file: str = None):
    """Plot traffic distribution and throughput."""
    fig, axes = plt.subplots(3, 2, figsize=(14, 14))
    
    # 1. Top senders bar chart
    top_senders = df.groupby('src')['MB'].sum().sort_values(ascending=False).head(15)
    axes[0, 0].bar(range(len(top_senders)), top_senders.values, color='steelblue')
    axes[0, 0].set_xticks(range(len(top_senders)))
    axes[0, 0].set_xticklabels([f'N{x}' for x in top_senders.index], rotation=45)
    axes[0, 0].set_title('Top 15 Sending Nodes', fontweight='bold')
    axes[0, 0].set_ylabel('Traffic (MB)')
    axes[0, 0].grid(axis='y', alpha=0.3)
    
    # 2. Top receivers bar chart
    top_receivers = df.groupby('dst')['MB'].sum().sort_values(ascending=False).head(15)
    axes[0, 1].bar(range(len(top_receivers)), top_receivers.values, color='coral')
    axes[0, 1].set_xticks(range(len(top_receivers)))
    axes[0, 1].set_xticklabels([f'N{x}' for x in top_receivers.index], rotation=45)
    axes[0, 1].set_title('Top 15 Receiving Nodes', fontweight='bold')
    axes[0, 1].set_ylabel('Traffic (MB)')
    axes[0, 1].grid(axis='y', alpha=0.3)
    
    # 3. Traffic distribution histogram
    axes[1, 0].hist(df['MB'], bins=50, color='green', alpha=0.7, edgecolor='black')
    axes[1, 0].set_title('Link Traffic Distribution', fontweight='bold')
    axes[1, 0].set_xlabel('Traffic (MB)')
    axes[1, 0].set_ylabel('Number of Links')
    axes[1, 0].set_yscale('log')
    axes[1, 0].grid(axis='y', alpha=0.3)
    
    # 4. Cumulative traffic
    df_sorted = df.sort_values('MB', ascending=False).reset_index(drop=True)
    cumsum = df_sorted['MB'].cumsum()
    cumsum_pct = (cumsum / cumsum.iloc[-1]) * 100
    
    axes[1, 1].plot(range(len(cumsum_pct)), cumsum_pct, color='purple', linewidth=2)
    axes[1, 1].axhline(y=80, color='red', linestyle='--', alpha=0.5, label='80%')
    axes[1, 1].axhline(y=90, color='orange', linestyle='--', alpha=0.5, label='90%')
    axes[1, 1].set_title('Cumulative Traffic Distribution', fontweight='bold')
    axes[1, 1].set_xlabel('Number of Links (sorted by traffic)')
    axes[1, 1].set_ylabel('Cumulative Traffic (%)')
    axes[1, 1].legend()
    axes[1, 1].grid(alpha=0.3)
    
    # 5 & 6. Average throughput per node
    src_tp, dst_tp, duration = analyze_throughput(df)
    if src_tp is not None and dst_tp is not None:
        top_src_tp = src_tp.sort_values(ascending=False).head(15)
        top_dst_tp = dst_tp.sort_values(ascending=False).head(15)
        
        axes[2, 0].bar(range(len(top_src_tp)), top_src_tp.values, color='dodgerblue')
        axes[2, 0].set_xticks(range(len(top_src_tp)))
        axes[2, 0].set_xticklabels([f'N{x}' for x in top_src_tp.index], rotation=45)
        axes[2, 0].set_title('Top 15 Source Throughputs', fontweight='bold')
        axes[2, 0].set_ylabel('Throughput (MB/s)')
        axes[2, 0].grid(axis='y', alpha=0.3)
        
        axes[2, 1].bar(range(len(top_dst_tp)), top_dst_tp.values, color='orange')
        axes[2, 1].set_xticks(range(len(top_dst_tp)))
        axes[2, 1].set_xticklabels([f'N{x}' for x in top_dst_tp.index], rotation=45)
        axes[2, 1].set_title('Top 15 Destination Throughputs', fontweight='bold')
        axes[2, 1].set_ylabel('Throughput (MB/s)')
        axes[2, 1].grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Distribution and throughput plots saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def export_to_csv(df: pd.DataFrame, output_file: str):
    """Export data to CSV for further analysis."""
    src_summary = df.groupby('src').agg({
        'bytes': 'sum',
        'MB': 'sum',
        'dst': 'count'
    }).rename(columns={'dst': 'num_destinations'})
    src_summary.to_csv(output_file.replace('.csv', '_by_source.csv'))
    
    dst_summary = df.groupby('dst').agg({
        'bytes': 'sum',
        'MB': 'sum',
        'src': 'count'
    }).rename(columns={'src': 'num_sources'})
    dst_summary.to_csv(output_file.replace('.csv', '_by_dest.csv'))
    
    df.to_csv(output_file, index=False)
    
    print(f"\nCSV files exported:")
    print(f"  - {output_file}")
    print(f"  - {output_file.replace('.csv', '_by_source.csv')}")
    print(f"  - {output_file.replace('.csv', '_by_dest.csv')}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze RDMA network traffic from TxMonitor Avro output',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s tx_monitor.avro
  %(prog)s tx_monitor.avro --heatmap traffic_heatmap.png
  %(prog)s tx_monitor.avro --plots --csv output.csv
  %(prog)s tx_monitor.avro --top 20
        """
    )
    
    parser.add_argument('input', help='Input Avro file from TxMonitor')
    parser.add_argument('--top', type=int, default=10, 
                        help='Number of top talkers/flows to show (default: 10)')
    parser.add_argument('--heatmap', metavar='FILE',
                        help='Generate traffic heatmap and save to file')
    parser.add_argument('--plots', metavar='FILE', nargs='?', const='traffic_plots.png',
                        help='Generate distribution plots (default: traffic_plots.png)')
    parser.add_argument('--csv', metavar='FILE',
                        help='Export analysis to CSV file')
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress text output (only generate plots/csv)')
    
    args = parser.parse_args()
    
    if not args.quiet:
        print(f"Reading Avro file: {args.input}")
    
    records = read_avro_file(args.input)
    
    if not records:
        print("Error: No records found in Avro file")
        sys.exit(1)
    
    if not args.quiet:
        print(f"Found {len(records)} traffic records")
    
    df = records_to_dataframe(records)
    
    if not args.quiet:
        print_basic_stats(df)
        print_top_talkers(df, args.top)
        print_heaviest_flows(df, args.top)
        analyze_traffic_patterns(df)
    
    # Generate plots
    if args.heatmap:
        plot_traffic_heatmap(df, args.heatmap)
    
    if args.plots:
        plot_traffic_distribution(df, args.plots)
    
    # Export CSV
    if args.csv:
        export_to_csv(df, args.csv)
    
    if not args.quiet:
        print("\n" + "="*60)
        print("Analysis complete!")
        print("="*60 + "\n")
       
if __name__ == '__main__':
    main()
