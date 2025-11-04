#!/usr/bin/env python3
"""
Analyze RDMA flow completion statistics from FlowCompletionMonitor Avro output.
Provides flow completion time (FCT) analysis, throughput metrics, and visualizations.
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
    
    # Convert bytes to MB and GB
    df['bytes_requested_MB'] = df['bytes_requested'] / (1024 * 1024)
    df['bytes_requested_GB'] = df['bytes_requested'] / (1024 * 1024 * 1024)
    df['bytes_sent_MB'] = df['bytes_sent'] / (1024 * 1024)
    df['bytes_sent_GB'] = df['bytes_sent'] / (1024 * 1024 * 1024)
    
    # Calculate throughput
    df['throughput_MBps'] = df['bytes_sent_MB'] / df['duration']
    df['throughput_Gbps'] = df['throughput_MBps'] * 8 / 1000
    
    # Calculate overhead (retransmissions)
    df['overhead_bytes'] = df['bytes_sent'] - df['bytes_requested']
    df['overhead_percent'] = (df['overhead_bytes'] / df['bytes_requested']) * 100
    
    # Flow size categories
    df['flow_size_category'] = pd.cut(
        df['bytes_requested_MB'],
        bins=[0, 1, 10, 100, 1000, float('inf')],
        labels=['<1MB', '1-10MB', '10-100MB', '100MB-1GB', '>1GB']
    )
    
    return df


def print_basic_stats(df: pd.DataFrame):
    """Print basic statistics about flow completions."""
    print("\n" + "="*70)
    print("BASIC STATISTICS")
    print("="*70)
    
    print(f"\nTotal Flows Completed: {len(df)}")
    print(f"Reliable Flows: {df['is_reliable'].sum()} ({df['is_reliable'].sum()/len(df)*100:.1f}%)")
    print(f"Unreliable Flows: {(~df['is_reliable']).sum()} ({(~df['is_reliable']).sum()/len(df)*100:.1f}%)")
    
    print(f"\nSimulation Time Window:")
    print(f"  First flow start:      {df['start_time'].min():.6f}s")
    print(f"  Last flow completion:  {df['completion_time'].max():.6f}s")
    print(f"  Simulation duration:   {df['completion_time'].max() - df['start_time'].min():.6f}s")
    
    total_bytes = df['bytes_requested'].sum()
    total_sent = df['bytes_sent'].sum()
    
    print(f"\nTotal Data:")
    print(f"  Requested: {total_bytes / (1024**3):.4f} GB")
    print(f"  Sent:      {total_sent / (1024**3):.4f} GB")
    print(f"  Overhead:  {(total_sent - total_bytes) / (1024**3):.4f} GB ({(total_sent/total_bytes - 1)*100:.2f}%)")


def print_fct_statistics(df: pd.DataFrame):
    """Print Flow Completion Time (FCT) statistics."""
    print("\n" + "="*70)
    print("FLOW COMPLETION TIME (FCT) STATISTICS")
    print("="*70)
    
    print(f"\nOverall FCT:")
    print(f"  Mean:   {df['duration'].mean():.6f}s")
    print(f"  Median: {df['duration'].median():.6f}s")
    print(f"  Std:    {df['duration'].std():.6f}s")
    print(f"  Min:    {df['duration'].min():.6f}s")
    print(f"  Max:    {df['duration'].max():.6f}s")
    
    # FCT by flow size
    print(f"\nFCT by Flow Size Category:")
    print(f"{'Category':<15}{'Count':<8}{'Mean FCT':<12}{'Median FCT':<12}{'Max FCT':<12}")
    print("-" * 70)
    
    for category in df['flow_size_category'].cat.categories:
        cat_df = df[df['flow_size_category'] == category]
        if len(cat_df) > 0:
            print(f"{category:<15}{len(cat_df):<8}"
                  f"{cat_df['duration'].mean():>8.6f}s   "
                  f"{cat_df['duration'].median():>8.6f}s   "
                  f"{cat_df['duration'].max():>8.6f}s")
    
    # FCT percentiles
    percentiles = [50, 75, 90, 95, 99]
    print(f"\nFCT Percentiles:")
    for p in percentiles:
        val = np.percentile(df['duration'], p)
        print(f"  P{p:2d}: {val:.6f}s")


def print_throughput_statistics(df: pd.DataFrame):
    """Print throughput statistics."""
    print("\n" + "="*70)
    print("THROUGHPUT STATISTICS")
    print("="*70)
    
    print(f"\nPer-Flow Throughput:")
    print(f"  Mean:   {df['throughput_Gbps'].mean():.3f} Gbps ({df['throughput_MBps'].mean():.2f} MB/s)")
    print(f"  Median: {df['throughput_Gbps'].median():.3f} Gbps ({df['throughput_MBps'].median():.2f} MB/s)")
    print(f"  Std:    {df['throughput_Gbps'].std():.3f} Gbps ({df['throughput_MBps'].std():.2f} MB/s)")
    print(f"  Min:    {df['throughput_Gbps'].min():.3f} Gbps ({df['throughput_MBps'].min():.2f} MB/s)")
    print(f"  Max:    {df['throughput_Gbps'].max():.3f} Gbps ({df['throughput_MBps'].max():.2f} MB/s)")
    
    # Throughput by flow size
    print(f"\nThroughput by Flow Size Category:")
    print(f"{'Category':<15}{'Count':<8}{'Mean (Gbps)':<15}{'Median (Gbps)':<15}")
    print("-" * 60)
    
    for category in df['flow_size_category'].cat.categories:
        cat_df = df[df['flow_size_category'] == category]
        if len(cat_df) > 0:
            print(f"{category:<15}{len(cat_df):<8}"
                  f"{cat_df['throughput_Gbps'].mean():>10.3f}     "
                  f"{cat_df['throughput_Gbps'].median():>10.3f}")


def print_overhead_analysis(df: pd.DataFrame):
    """Print overhead/retransmission analysis."""
    print("\n" + "="*70)
    print("OVERHEAD ANALYSIS (Retransmissions)")
    print("="*70)
    
    flows_with_overhead = df[df['overhead_percent'] > 0]
    
    if len(flows_with_overhead) == 0:
        print("\nNo retransmissions detected!")
        return
    
    print(f"\nFlows with Retransmissions: {len(flows_with_overhead)} ({len(flows_with_overhead)/len(df)*100:.1f}%)")
    print(f"\nOverhead Statistics:")
    print(f"  Mean:   {flows_with_overhead['overhead_percent'].mean():.2f}%")
    print(f"  Median: {flows_with_overhead['overhead_percent'].median():.2f}%")
    print(f"  Max:    {flows_with_overhead['overhead_percent'].max():.2f}%")
    
    # Top flows with highest overhead
    print(f"\nTop 10 Flows with Highest Overhead:")
    print(f"{'Flow ID':<15}{'Src→Dst':<12}{'Size (MB)':<12}{'Overhead %':<12}{'Extra Bytes':<15}")
    print("-" * 75)
    
    top_overhead = flows_with_overhead.nlargest(10, 'overhead_percent')
    for row in top_overhead.itertuples():
        print(f"{row.flow_id:<15}{row.src}→{row.dst:<8}"
              f"{row.bytes_requested_MB:>8.2f}    "
              f"{row.overhead_percent:>8.2f}%   "
              f"{row.overhead_bytes:>12,} B")


def print_flow_pairs_analysis(df: pd.DataFrame):
    """Print analysis of communication patterns."""
    print("\n" + "="*70)
    print("COMMUNICATION PATTERN ANALYSIS")
    print("="*70)
    
    # Most active src-dst pairs
    pair_stats = df.groupby(['src', 'dst']).agg({
        'flow_id': 'count',
        'bytes_requested_MB': 'sum',
        'duration': 'mean',
        'throughput_Gbps': 'mean'
    }).reset_index()
    
    pair_stats.columns = ['src', 'dst', 'num_flows', 'total_MB', 'avg_duration', 'avg_throughput_Gbps']
    pair_stats = pair_stats.sort_values('total_MB', ascending=False)
    
    print(f"\nTop 10 Communication Pairs (by total traffic):")
    print(f"{'Src→Dst':<12}{'Flows':<8}{'Total (MB)':<15}{'Avg Duration':<15}{'Avg Tput (Gbps)':<15}")
    print("-" * 75)
    
    for row in pair_stats.head(10).itertuples():
        print(f"{row.src}→{row.dst:<8}{row.num_flows:<8}"
              f"{row.total_MB:>10.2f}     "
              f"{row.avg_duration:>10.6f}s    "
              f"{row.avg_throughput_Gbps:>10.3f}")
    
    # Incast detection
    dst_stats = df.groupby('dst').agg({
        'src': 'nunique',
        'flow_id': 'count',
        'bytes_requested_MB': 'sum'
    }).reset_index()
    
    dst_stats.columns = ['dst', 'num_sources', 'num_flows', 'total_MB']
    dst_stats = dst_stats.sort_values('num_sources', ascending=False)
    
    print(f"\nIncast Analysis (destinations receiving from multiple sources):")
    print(f"{'Destination':<15}{'Sources':<10}{'Flows':<10}{'Total (MB)':<15}")
    print("-" * 60)
    
    for row in dst_stats.head(10).itertuples():
        if row.num_sources > 1:
            print(f"Node {row.dst:<10}{row.num_sources:<10}{row.num_flows:<10}{row.total_MB:>10.2f}")


def print_slowest_flows(df: pd.DataFrame, n: int = 10):
    """Print slowest flows (lowest throughput)."""
    print("\n" + "="*70)
    print(f"TOP {n} SLOWEST FLOWS (Lowest Throughput)")
    print("="*70)
    
    slowest = df.nsmallest(n, 'throughput_Gbps')
    
    print(f"\n{'Flow ID':<15}{'Src→Dst':<12}{'Size (MB)':<12}{'Duration (s)':<15}{'Tput (Gbps)':<15}{'Overhead %':<12}")
    print("-" * 90)
    
    for row in slowest.itertuples():
        print(f"{row.flow_id:<15}{row.src}→{row.dst:<8}"
              f"{row.bytes_requested_MB:>8.2f}    "
              f"{row.duration:>10.6f}     "
              f"{row.throughput_Gbps:>10.3f}     "
              f"{row.overhead_percent:>8.2f}%")


def plot_fct_analysis(df: pd.DataFrame, output_file: str = None):
    """Plot FCT analysis."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: FCT distribution
    axes[0, 0].hist(df['duration'], bins=50, color='steelblue', alpha=0.7, edgecolor='black')
    axes[0, 0].set_xlabel('Flow Completion Time (s)')
    axes[0, 0].set_ylabel('Number of Flows')
    axes[0, 0].set_title('FCT Distribution', fontweight='bold')
    axes[0, 0].set_yscale('log')
    axes[0, 0].grid(alpha=0.3)
    
    # Plot 2: FCT vs Flow Size
    axes[0, 1].scatter(df['bytes_requested_MB'], df['duration'], alpha=0.6, s=30)
    axes[0, 1].set_xlabel('Flow Size (MB)')
    axes[0, 1].set_ylabel('Flow Completion Time (s)')
    axes[0, 1].set_title('FCT vs Flow Size', fontweight='bold')
    axes[0, 1].set_xscale('log')
    axes[0, 1].set_yscale('log')
    axes[0, 1].grid(alpha=0.3)
    
    # Plot 3: FCT CDF
    sorted_fct = np.sort(df['duration'])
    cdf = np.arange(1, len(sorted_fct) + 1) / len(sorted_fct)
    
    axes[1, 0].plot(sorted_fct, cdf * 100, linewidth=2, color='green')
    axes[1, 0].set_xlabel('Flow Completion Time (s)')
    axes[1, 0].set_ylabel('CDF (%)')
    axes[1, 0].set_title('FCT Cumulative Distribution', fontweight='bold')
    axes[1, 0].grid(alpha=0.3)
    
    # Add percentile lines
    percentiles = [50, 90, 99]
    for p in percentiles:
        val = np.percentile(sorted_fct, p)
        axes[1, 0].axvline(x=val, color='red', linestyle='--', alpha=0.5)
        axes[1, 0].text(val, 50, f'P{p}', rotation=90, verticalalignment='center')
    
    # Plot 4: FCT by flow size category
    category_data = [df[df['flow_size_category'] == cat]['duration'].values 
                     for cat in df['flow_size_category'].cat.categories 
                     if len(df[df['flow_size_category'] == cat]) > 0]
    category_labels = [cat for cat in df['flow_size_category'].cat.categories 
                       if len(df[df['flow_size_category'] == cat]) > 0]
    
    bp = axes[1, 1].boxplot(category_data, labels=category_labels, patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor('lightblue')
    
    axes[1, 1].set_xlabel('Flow Size Category')
    axes[1, 1].set_ylabel('Flow Completion Time (s)')
    axes[1, 1].set_title('FCT by Flow Size Category', fontweight='bold')
    axes[1, 1].set_yscale('log')
    axes[1, 1].grid(axis='y', alpha=0.3)
    plt.setp(axes[1, 1].xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"\nFCT analysis plot saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def plot_throughput_analysis(df: pd.DataFrame, output_file: str = None):
    """Plot throughput analysis."""
    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    
    # Plot 1: Throughput distribution
    axes[0, 0].hist(df['throughput_Gbps'], bins=50, color='coral', alpha=0.7, edgecolor='black')
    axes[0, 0].set_xlabel('Throughput (Gbps)')
    axes[0, 0].set_ylabel('Number of Flows')
    axes[0, 0].set_title('Throughput Distribution', fontweight='bold')
    axes[0, 0].grid(alpha=0.3)
    
    # Plot 2: Throughput vs Flow Size
    axes[0, 1].scatter(df['bytes_requested_MB'], df['throughput_Gbps'], alpha=0.6, s=30, color='coral')
    axes[0, 1].set_xlabel('Flow Size (MB)')
    axes[0, 1].set_ylabel('Throughput (Gbps)')
    axes[0, 1].set_title('Throughput vs Flow Size', fontweight='bold')
    axes[0, 1].set_xscale('log')
    axes[0, 1].grid(alpha=0.3)
    
    # Plot 3: Per-Node Throughput (NEW!)
    # Calculate average throughput per source node
    node_throughput = df.groupby('src')['throughput_Gbps'].mean().sort_index()
    
    nodes = node_throughput.index.tolist()
    throughputs = node_throughput.values
    
    bars = axes[0, 2].bar(range(len(nodes)), throughputs, color='steelblue', alpha=0.8, edgecolor='black')
    axes[0, 2].set_xlabel('Source Node')
    axes[0, 2].set_ylabel('Average Throughput (Gbps)')
    axes[0, 2].set_title('Average Throughput per Node', fontweight='bold')
    axes[0, 2].set_xticks(range(len(nodes)))
    axes[0, 2].set_xticklabels([f'N{n}' for n in nodes], rotation=45)
    axes[0, 2].grid(axis='y', alpha=0.3)
    
    # Add value labels on top of bars
    for i, (bar, val) in enumerate(zip(bars, throughputs)):
        axes[0, 2].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                       f'{val:.2f}', ha='center', va='bottom', fontsize=9)
    
    # Plot 4: Overhead percentage
    if df['overhead_percent'].max() > 0:
        axes[1, 0].hist(df['overhead_percent'], bins=50, color='red', alpha=0.7, edgecolor='black')
        axes[1, 0].set_xlabel('Overhead (%)')
        axes[1, 0].set_ylabel('Number of Flows')
        axes[1, 0].set_title('Retransmission Overhead Distribution', fontweight='bold')
        axes[1, 0].grid(alpha=0.3)
    else:
        axes[1, 0].text(0.5, 0.5, 'No Retransmissions', ha='center', va='center', fontsize=14)
        axes[1, 0].set_title('Retransmission Overhead Distribution', fontweight='bold')
    
    # Plot 5: Timeline of flow completions
    axes[1, 1].scatter(df['completion_time'], df['throughput_Gbps'], alpha=0.6, s=30, color='purple')
    axes[1, 1].set_xlabel('Completion Time (s)')
    axes[1, 1].set_ylabel('Throughput (Gbps)')
    axes[1, 1].set_title('Flow Throughput Over Time', fontweight='bold')
    axes[1, 1].grid(alpha=0.3)
    
    # Plot 6: Per-Node Throughput with individual flows (NEW!)
    # Show all flows grouped by source node
    node_flow_data = []
    node_labels = []
    for node in sorted(df['src'].unique()):
        node_flows = df[df['src'] == node]['throughput_Gbps'].values
        if len(node_flows) > 0:
            node_flow_data.append(node_flows)
            node_labels.append(f'N{node}')
    
    bp = axes[1, 2].boxplot(node_flow_data, labels=node_labels, patch_artist=True)
    for patch in bp['boxes']:
        patch.set_facecolor('lightcoral')
    
    axes[1, 2].set_xlabel('Source Node')
    axes[1, 2].set_ylabel('Throughput (Gbps)')
    axes[1, 2].set_title('Throughput Distribution per Node', fontweight='bold')
    axes[1, 2].grid(axis='y', alpha=0.3)
    plt.setp(axes[1, 2].xaxis.get_majorticklabels(), rotation=45)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Throughput analysis plot saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def plot_node_throughput(df: pd.DataFrame, output_file: str = None):
    """Plot per-node throughput histogram."""
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Calculate average throughput per source node
    src_throughput = df.groupby('src')['throughput_Gbps'].mean().sort_index()
    src_nodes = src_throughput.index.tolist()
    src_values = src_throughput.values
    
    # Calculate average throughput per destination node
    dst_throughput = df.groupby('dst')['throughput_Gbps'].mean().sort_index()
    dst_nodes = dst_throughput.index.tolist()
    dst_values = dst_throughput.values
    
    # Plot 1: Source nodes
    bars1 = axes[0].bar(range(len(src_nodes)), src_values, color='steelblue', 
                        alpha=0.8, edgecolor='black', linewidth=1.5)
    axes[0].set_xlabel('Source Node', fontsize=12)
    axes[0].set_ylabel('Average Throughput (Gbps)', fontsize=12)
    axes[0].set_title('Average Throughput per Source Node', fontsize=14, fontweight='bold')
    axes[0].set_xticks(range(len(src_nodes)))
    axes[0].set_xticklabels([f'Node {n}' for n in src_nodes], rotation=45, ha='right')
    axes[0].grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on top of bars
    for bar, val in zip(bars1, src_values):
        height = bar.get_height()
        axes[0].text(bar.get_x() + bar.get_width()/2, height + 0.05,
                    f'{val:.2f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Add flow count as text inside bars
    src_flow_counts = df.groupby('src').size()
    for i, (bar, node) in enumerate(zip(bars1, src_nodes)):
        count = src_flow_counts[node]
        axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height()/2,
                    f'{count} flow{"s" if count > 1 else ""}',
                    ha='center', va='center', fontsize=9, color='white', fontweight='bold')
    
    # Plot 2: Destination nodes
    bars2 = axes[1].bar(range(len(dst_nodes)), dst_values, color='coral',
                        alpha=0.8, edgecolor='black', linewidth=1.5)
    axes[1].set_xlabel('Destination Node', fontsize=12)
    axes[1].set_ylabel('Average Throughput (Gbps)', fontsize=12)
    axes[1].set_title('Average Throughput per Destination Node', fontsize=14, fontweight='bold')
    axes[1].set_xticks(range(len(dst_nodes)))
    axes[1].set_xticklabels([f'Node {n}' for n in dst_nodes], rotation=45, ha='right')
    axes[1].grid(axis='y', alpha=0.3, linestyle='--')
    
    # Add value labels on top of bars
    for bar, val in zip(bars2, dst_values):
        height = bar.get_height()
        axes[1].text(bar.get_x() + bar.get_width()/2, height + 0.05,
                    f'{val:.2f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Add flow count as text inside bars
    dst_flow_counts = df.groupby('dst').size()
    for i, (bar, node) in enumerate(zip(bars2, dst_nodes)):
        count = dst_flow_counts[node]
        axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height()/2,
                    f'{count} flow{"s" if count > 1 else ""}',
                    ha='center', va='center', fontsize=9, color='white', fontweight='bold')
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Per-node throughput plot saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()
    """Plot communication heatmap (src vs dst)."""
    # Create pivot table
    comm_matrix = df.groupby(['src', 'dst'])['bytes_requested_MB'].sum().unstack(fill_value=0)
    
    plt.figure(figsize=(10, 8))
    
    sns.heatmap(comm_matrix, cmap='YlOrRd', cbar_kws={'label': 'Total Traffic (MB)'},
                linewidths=0.5, linecolor='gray', annot=False)
    
    plt.title('Communication Pattern (Source → Destination)', fontsize=14, fontweight='bold')
    plt.xlabel('Destination Node', fontsize=12)
    plt.ylabel('Source Node', fontsize=12)
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Communication heatmap saved to: {output_file}")
    else:
        plt.show()
    
    plt.close()


def export_to_csv(df: pd.DataFrame, output_file: str):
    """Export analysis to CSV files."""
    # Export full data
    df.to_csv(output_file, index=False)
    print(f"\nFull data exported to: {output_file}")
    
    # Export summary statistics
    summary = df.groupby(['src', 'dst']).agg({
        'flow_id': 'count',
        'bytes_requested_MB': ['sum', 'mean'],
        'duration': ['mean', 'median', 'std'],
        'throughput_Gbps': ['mean', 'median'],
        'overhead_percent': 'mean'
    }).reset_index()
    
    summary.columns = ['_'.join(col).strip('_') for col in summary.columns.values]
    summary_file = output_file.replace('.csv', '_summary.csv')
    summary.to_csv(summary_file, index=False)
    print(f"Summary statistics exported to: {summary_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze RDMA flow completion statistics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s flow_completion.avro
  %(prog)s flow_completion.avro --fct-plot fct.png
  %(prog)s flow_completion.avro --node-throughput node_tput.png
  %(prog)s flow_completion.avro --throughput-plot throughput.png
  %(prog)s flow_completion.avro --all --output-prefix results/flow
  %(prog)s flow_completion.avro --csv output.csv --top 20
        """
    )
    
    parser.add_argument('input', help='Input Avro file from FlowCompletionMonitor')
    parser.add_argument('--top', type=int, default=10,
                       help='Number of top/slowest flows to show (default: 10)')
    parser.add_argument('--fct-plot', metavar='FILE',
                       help='Generate FCT analysis plots')
    parser.add_argument('--throughput-plot', metavar='FILE',
                       help='Generate throughput analysis plots')
    parser.add_argument('--node-throughput', metavar='FILE',
                       help='Generate per-node throughput histogram')
    parser.add_argument('--heatmap', metavar='FILE',
                       help='Generate communication pattern heatmap')
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
        args.fct_plot = f"{args.output_prefix}_fct.png"
        args.throughput_plot = f"{args.output_prefix}_throughput.png"
        args.node_throughput = f"{args.output_prefix}_node_throughput.png"
        args.heatmap = f"{args.output_prefix}_heatmap.png"
        args.csv = f"{args.output_prefix}_data.csv"
    
    # Read data
    if not args.quiet:
        print(f"Reading Avro file: {args.input}")
    
    records = read_avro_file(args.input)
    
    if not records:
        print("Error: No records found in Avro file")
        sys.exit(1)
    
    if not args.quiet:
        print(f"Found {len(records)} flow completion records")
    
    df = records_to_dataframe(records)
    
    # Print analysis
    if not args.quiet:
        print_basic_stats(df)
        print_fct_statistics(df)
        print_throughput_statistics(df)
        print_overhead_analysis(df)
        print_flow_pairs_analysis(df)
        print_slowest_flows(df, args.top)
    
    # Generate plots
    if args.fct_plot:
        plot_fct_analysis(df, args.fct_plot)
    
    if args.throughput_plot:
        plot_throughput_analysis(df, args.throughput_plot)
    
    if args.node_throughput:
        plot_node_throughput(df, args.node_throughput)
    
    if args.heatmap:
        plot_communication_heatmap(df, args.heatmap)
    
    # Export CSV
    if args.csv:
        export_to_csv(df, args.csv)
    
    if not args.quiet:
        print("\n" + "="*70)
        print("Analysis complete!")
        print("="*70 + "\n")


if __name__ == '__main__':
    main()
