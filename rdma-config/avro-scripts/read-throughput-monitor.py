#!/usr/bin/env python3
"""
Analyze throughput data from NS-3 RDMA simulation
"""

import fastavro
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import argparse
import sys
from pathlib import Path

# Try to import tqdm, fallback to simple progress if not available
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Note: Install 'tqdm' for better progress bars: pip install tqdm")


def simple_progress_bar(iterable, total=None, desc="Processing"):
    """Simple fallback progress bar if tqdm is not available"""
    if total is None:
        try:
            total = len(iterable)
        except TypeError:
            total = None
    
    if total is None:
        # Can't show progress without total
        for item in iterable:
            yield item
        return
    
    for i, item in enumerate(iterable, 1):
        percent = (i / total) * 100
        bar_length = 40
        filled = int(bar_length * i / total)
        bar = '█' * filled + '-' * (bar_length - filled)
        print(f'\r{desc}: |{bar}| {percent:.1f}% ({i}/{total})', end='', flush=True)
        yield item
    print()  # New line after completion


class ThroughputAnalyzer:
    """Analyzer for throughput records from NS-3 simulation"""
    
    def __init__(self, avro_file, verbose=True):
        """
        Initialize analyzer with Avro file
        
        Args:
            avro_file: Path to the throughput.avro file
            verbose: Show progress bars and messages
        """
        self.avro_file = avro_file
        self.records = []
        self.verbose = verbose
        self._load_data()
    
    def _load_data(self):
        """Load data from Avro file"""
        try:
            if self.verbose:
                print(f"Loading data from {self.avro_file}...")
            
            with open(self.avro_file, 'rb') as f:
                reader = fastavro.reader(f)
                
                # First pass to count records (for progress bar)
                if self.verbose:
                    print("Counting records...")
                    temp_records = list(reader)
                    total = len(temp_records)
                    
                    # Reset file pointer
                    f.seek(0)
                    reader = fastavro.reader(f)
                    
                    # Second pass with progress bar
                    if TQDM_AVAILABLE:
                        self.records = list(tqdm(reader, total=total, desc="Loading records", unit="rec"))
                    else:
                        self.records = list(simple_progress_bar(reader, total=total, desc="Loading records"))
                else:
                    self.records = list(reader)
            
            if self.verbose:
                print(f"✓ Loaded {len(self.records)} throughput records\n")
        except FileNotFoundError:
            print(f"Error: File '{self.avro_file}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error loading Avro file: {e}")
            sys.exit(1)
    
    def get_all_nodes(self):
        """Get set of all node IDs (both src and dst)"""
        nodes = set()
        
        iterator = self.records
        if self.verbose and TQDM_AVAILABLE:
            iterator = tqdm(self.records, desc="Finding nodes", unit="rec", leave=False)
        
        for r in iterator:
            nodes.add(r['src'])
            nodes.add(r['dst'])
        return sorted(nodes)
    
    def get_node_pairs(self):
        """Get all unique (src, dst) pairs"""
        pairs = set()
        
        iterator = self.records
        if self.verbose and TQDM_AVAILABLE:
            iterator = tqdm(self.records, desc="Finding pairs", unit="rec", leave=False)
        
        for r in iterator:
            pairs.add((r['src'], r['dst']))
        return sorted(pairs)
    
    def filter_by_node(self, node_id, direction='tx'):
        """
        Filter records by node
        
        Args:
            node_id: Node ID to filter
            direction: 'tx' (transmitted), 'rx' (received), or 'both'
        
        Returns:
            List of filtered records
        """
        iterator = self.records
        if self.verbose and TQDM_AVAILABLE and len(self.records) > 10000:
            iterator = tqdm(self.records, desc=f"Filtering node {node_id}", unit="rec", leave=False)
        
        if direction == 'tx':
            return [r for r in iterator if r['src'] == node_id]
        elif direction == 'rx':
            return [r for r in iterator if r['dst'] == node_id]
        elif direction == 'both':
            return [r for r in iterator if r['src'] == node_id or r['dst'] == node_id]
        else:
            raise ValueError("direction must be 'tx', 'rx', or 'both'")
    
    def filter_by_link(self, src, dst):
        """Filter records for specific link"""
        iterator = self.records
        if self.verbose and TQDM_AVAILABLE and len(self.records) > 10000:
            iterator = tqdm(self.records, desc=f"Filtering link {src}→{dst}", unit="rec", leave=False)
        
        return [r for r in iterator if r['src'] == src and r['dst'] == dst]
    
    def aggregate_by_time(self, records):
        """
        Aggregate throughput by time
        
        Args:
            records: List of records to aggregate
        
        Returns:
            Tuple of (times, throughputs) sorted by time
        """
        time_throughput = defaultdict(float)
        
        iterator = records
        if self.verbose and TQDM_AVAILABLE and len(records) > 10000:
            iterator = tqdm(records, desc="Aggregating", unit="rec", leave=False)
        
        for r in iterator:
            time_throughput[r['time']] += r['throughput_gbps']
        
        times = sorted(time_throughput.keys())
        throughputs = [time_throughput[t] for t in times]
        return times, throughputs
    
    def get_statistics(self, records):
        """
        Calculate statistics for given records
        
        Returns:
            Dictionary with statistics
        """
        if not records:
            return None
        
        throughputs = [r['throughput_gbps'] for r in records]
        bytes_deltas = [r['bytes_delta'] for r in records]
        
        return {
            'count': len(records),
            'avg_throughput_gbps': np.mean(throughputs),
            'median_throughput_gbps': np.median(throughputs),
            'max_throughput_gbps': np.max(throughputs),
            'min_throughput_gbps': np.min(throughputs),
            'std_throughput_gbps': np.std(throughputs),
            'total_bytes': sum(bytes_deltas),
            'total_bytes_mb': sum(bytes_deltas) / (1024 * 1024)
        }
    
    def print_summary(self):
        """Print summary statistics"""
        print("\n" + "="*70)
        print("THROUGHPUT ANALYSIS SUMMARY")
        print("="*70)
        
        # Overall statistics
        if self.verbose:
            print("Calculating statistics...")
        
        stats = self.get_statistics(self.records)
        print(f"\nOverall Statistics:")
        print(f"  Total records:        {stats['count']}")
        print(f"  Total data:           {stats['total_bytes_mb']:.2f} MB")
        print(f"  Avg throughput:       {stats['avg_throughput_gbps']:.3f} Gbps")
        print(f"  Max throughput:       {stats['max_throughput_gbps']:.3f} Gbps")
        print(f"  Min throughput:       {stats['min_throughput_gbps']:.3f} Gbps")
        print(f"  Std dev:              {stats['std_throughput_gbps']:.3f} Gbps")
        
        # Time range
        times = [r['time'] for r in self.records]
        print(f"\nTime Range:")
        print(f"  Start:                {min(times):.6f} s")
        print(f"  End:                  {max(times):.6f} s")
        print(f"  Duration:             {max(times) - min(times):.6f} s")
        
        # Per-node statistics
        nodes = self.get_all_nodes()
        print(f"\nPer-Node TX Statistics:")
        print(f"{'Node':<8} {'Avg (Gbps)':<12} {'Max (Gbps)':<12} {'Total (MB)':<12} {'Records':<10}")
        print("-" * 70)
        
        iterator = nodes
        if self.verbose and TQDM_AVAILABLE:
            iterator = tqdm(nodes, desc="Computing per-node stats", leave=False)
        
        for node in iterator:
            node_records = self.filter_by_node(node, 'tx')
            if node_records:
                stats = self.get_statistics(node_records)
                print(f"{node:<8} {stats['avg_throughput_gbps']:<12.3f} "
                      f"{stats['max_throughput_gbps']:<12.3f} "
                      f"{stats['total_bytes_mb']:<12.2f} "
                      f"{stats['count']:<10}")
    
    def print_link_statistics(self):
        """Print per-link statistics"""
        print("\n" + "="*70)
        print("PER-LINK STATISTICS")
        print("="*70)
        
        pairs = self.get_node_pairs()
        print(f"\n{'Src':<6} {'Dst':<6} {'Avg (Gbps)':<12} {'Max (Gbps)':<12} {'Total (MB)':<12} {'Records':<10}")
        print("-" * 70)
        
        iterator = pairs
        if self.verbose and TQDM_AVAILABLE:
            iterator = tqdm(pairs, desc="Computing per-link stats", leave=False)
        
        for src, dst in iterator:
            link_records = self.filter_by_link(src, dst)
            stats = self.get_statistics(link_records)
            print(f"{src:<6} {dst:<6} {stats['avg_throughput_gbps']:<12.3f} "
                  f"{stats['max_throughput_gbps']:<12.3f} "
                  f"{stats['total_bytes_mb']:<12.2f} "
                  f"{stats['count']:<10}")
    
    def plot_node_throughput(self, node_id, direction='tx', save_path=None):
        """
        Plot throughput for a specific node
        
        Args:
            node_id: Node ID to plot
            direction: 'tx', 'rx', or 'both'
            save_path: Optional path to save figure
        """
        if self.verbose:
            print(f"Plotting node {node_id} throughput ({direction})...")
        
        records = self.filter_by_node(node_id, direction)
        
        if not records:
            print(f"Warning: No records found for node {node_id} ({direction})")
            return
        
        times, throughputs = self.aggregate_by_time(records)
        stats = self.get_statistics(records)
        
        plt.figure(figsize=(12, 6))
        plt.plot(times, throughputs, linewidth=1.5, label=f'Node {node_id}')
        
        # Add average line
        plt.axhline(y=stats['avg_throughput_gbps'], color='r', linestyle='--', 
                   linewidth=1, alpha=0.7, label=f'Average: {stats["avg_throughput_gbps"]:.2f} Gbps')
        
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Throughput (Gbps)', fontsize=12)
        
        direction_str = {'tx': 'Transmitted', 'rx': 'Received', 'both': 'Total'}[direction]
        plt.title(f'Node {node_id} {direction_str} Throughput Over Time', fontsize=14)
        
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            if self.verbose:
                print(f"✓ Saved plot to {save_path}")
        
        return plt.gcf()
    
    def plot_link_throughput(self, src, dst, save_path=None):
        """
        Plot throughput for a specific link
        
        Args:
            src: Source node ID
            dst: Destination node ID
            save_path: Optional path to save figure
        """
        if self.verbose:
            print(f"Plotting link {src} → {dst} throughput...")
        
        records = self.filter_by_link(src, dst)
        
        if not records:
            print(f"Warning: No records found for link {src} -> {dst}")
            return
        
        times = [r['time'] for r in records]
        throughputs = [r['throughput_gbps'] for r in records]
        stats = self.get_statistics(records)
        
        plt.figure(figsize=(12, 6))
        plt.plot(times, throughputs, linewidth=1.5, marker='o', markersize=3,
                label=f'Link {src} → {dst}')
        
        # Add average line
        plt.axhline(y=stats['avg_throughput_gbps'], color='r', linestyle='--',
                   linewidth=1, alpha=0.7, label=f'Average: {stats["avg_throughput_gbps"]:.2f} Gbps')
        
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Throughput (Gbps)', fontsize=12)
        plt.title(f'Link Throughput: Node {src} → Node {dst}', fontsize=14)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            if self.verbose:
                print(f"✓ Saved plot to {save_path}")
        
        return plt.gcf()
    
    def plot_aggregate_throughput(self, save_path=None):
        """
        Plot aggregate network throughput
        
        Args:
            save_path: Optional path to save figure
        """
        if self.verbose:
            print("Plotting aggregate throughput...")
        
        times, throughputs = self.aggregate_by_time(self.records)
        
        plt.figure(figsize=(12, 6))
        plt.plot(times, throughputs, linewidth=2, color='navy', label='Aggregate')
        
        # Add statistics
        avg = np.mean(throughputs)
        plt.axhline(y=avg, color='r', linestyle='--', linewidth=1, alpha=0.7,
                   label=f'Average: {avg:.2f} Gbps')
        
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Aggregate Throughput (Gbps)', fontsize=12)
        plt.title('Network-Wide Aggregate Throughput', fontsize=14)
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            if self.verbose:
                print(f"✓ Saved plot to {save_path}")
        
        return plt.gcf()
    
    def plot_all_nodes(self, direction='tx', save_path=None):
        """
        Plot throughput for all nodes on the same graph
        
        Args:
            direction: 'tx' or 'rx'
            save_path: Optional path to save figure
        """
        if self.verbose:
            print(f"Plotting all nodes ({direction})...")
        
        nodes = self.get_all_nodes()
        
        plt.figure(figsize=(14, 8))
        
        iterator = nodes
        if self.verbose and TQDM_AVAILABLE:
            iterator = tqdm(nodes, desc="Plotting nodes", leave=False)
        
        for node in iterator:
            records = self.filter_by_node(node, direction)
            if records:
                times, throughputs = self.aggregate_by_time(records)
                plt.plot(times, throughputs, linewidth=1.5, label=f'Node {node}', alpha=0.8)
        
        plt.xlabel('Time (s)', fontsize=12)
        plt.ylabel('Throughput (Gbps)', fontsize=12)
        
        direction_str = {'tx': 'Transmitted', 'rx': 'Received'}[direction]
        plt.title(f'All Nodes {direction_str} Throughput', fontsize=14)
        
        plt.grid(True, alpha=0.3)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            if self.verbose:
                print(f"✓ Saved plot to {save_path}")
        
        return plt.gcf()
    
    def plot_heatmap(self, save_path=None):
        """
        Plot throughput heatmap (average throughput per link)
        
        Args:
            save_path: Optional path to save figure
        """
        if self.verbose:
            print("Generating heatmap...")
        
        nodes = self.get_all_nodes()
        n = len(nodes)
        
        # Create matrix
        matrix = np.zeros((n, n))
        node_to_idx = {node: i for i, node in enumerate(nodes)}
        
        pairs = self.get_node_pairs()
        iterator = pairs
        if self.verbose and TQDM_AVAILABLE:
            iterator = tqdm(pairs, desc="Computing heatmap", leave=False)
        
        for src, dst in iterator:
            records = self.filter_by_link(src, dst)
            stats = self.get_statistics(records)
            i = node_to_idx[src]
            j = node_to_idx[dst]
            matrix[i, j] = stats['avg_throughput_gbps']
        
        plt.figure(figsize=(10, 8))
        im = plt.imshow(matrix, cmap='YlOrRd', aspect='auto')
        
        plt.colorbar(im, label='Average Throughput (Gbps)')
        plt.xlabel('Destination Node', fontsize=12)
        plt.ylabel('Source Node', fontsize=12)
        plt.title('Average Throughput Heatmap', fontsize=14)
        
        # Set ticks
        plt.xticks(range(n), nodes)
        plt.yticks(range(n), nodes)
        
        # Add text annotations
        for i in range(n):
            for j in range(n):
                if matrix[i, j] > 0:
                    text = plt.text(j, i, f'{matrix[i, j]:.1f}',
                                  ha="center", va="center", color="black", fontsize=8)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            if self.verbose:
                print(f"✓ Saved plot to {save_path}")
        
        return plt.gcf()
    
    def export_to_csv(self, output_file):
        """Export records to CSV file"""
        import csv
        
        if self.verbose:
            print(f"Exporting to CSV...")
        
        with open(output_file, 'w', newline='') as f:
            if not self.records:
                return
            
            fieldnames = ['time', 'src', 'dst', 'bytes_delta', 'throughput_gbps']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            
            iterator = self.records
            if self.verbose and TQDM_AVAILABLE:
                iterator = tqdm(self.records, desc="Writing CSV", unit="rec", leave=False)
            
            for record in iterator:
                writer.writerow(record)
        
        if self.verbose:
            print(f"✓ Exported {len(self.records)} records to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze throughput data from NS-3 RDMA simulation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Print summary statistics
  python analyze_throughput.py throughput.avro
  
  # Plot node 10's transmitted throughput
  python analyze_throughput.py throughput.avro --node 10 --direction tx
  
  # Plot link from node 5 to node 10
  python analyze_throughput.py throughput.avro --link 5 10
  
  # Plot aggregate and save
  python analyze_throughput.py throughput.avro --aggregate --save aggregate.png
  
  # Plot all nodes
  python analyze_throughput.py throughput.avro --all-nodes --direction tx
  
  # Generate heatmap
  python analyze_throughput.py throughput.avro --heatmap
  
  # Export to CSV
  python analyze_throughput.py throughput.avro --csv output.csv
  
  # Quiet mode (no progress bars)
  python analyze_throughput.py throughput.avro --quiet
        """)
    
    parser.add_argument('avro_file', help='Path to throughput.avro file')
    parser.add_argument('--node', type=int, help='Plot specific node throughput')
    parser.add_argument('--direction', choices=['tx', 'rx', 'both'], default='tx',
                       help='Direction for node plot (default: tx)')
    parser.add_argument('--link', nargs=2, type=int, metavar=('SRC', 'DST'),
                       help='Plot specific link (src dst)')
    parser.add_argument('--aggregate', action='store_true',
                       help='Plot aggregate network throughput')
    parser.add_argument('--all-nodes', action='store_true',
                       help='Plot all nodes on same graph')
    parser.add_argument('--heatmap', action='store_true',
                       help='Generate throughput heatmap')
    parser.add_argument('--links', action='store_true',
                       help='Print per-link statistics')
    parser.add_argument('--save', metavar='PATH', help='Save plot to file')
    parser.add_argument('--csv', metavar='PATH', help='Export data to CSV')
    parser.add_argument('--no-show', action='store_true',
                       help='Do not display plots (useful with --save)')
    parser.add_argument('--quiet', '-q', action='store_true',
                       help='Suppress progress bars and verbose output')
    
    args = parser.parse_args()
    
    # Create analyzer
    analyzer = ThroughputAnalyzer(args.avro_file, verbose=not args.quiet)
    
    # Print summary (always)
    analyzer.print_summary()
    
    # Print link statistics if requested
    if args.links:
        analyzer.print_link_statistics()
    
    # Export to CSV if requested
    if args.csv:
        analyzer.export_to_csv(args.csv)
    
    # Generate plots
    plot_generated = False
    
    if args.node is not None:
        analyzer.plot_node_throughput(args.node, args.direction, args.save)
        plot_generated = True
    
    if args.link:
        src, dst = args.link
        analyzer.plot_link_throughput(src, dst, args.save)
        plot_generated = True
    
    if args.aggregate:
        analyzer.plot_aggregate_throughput(args.save)
        plot_generated = True
    
    if args.all_nodes:
        analyzer.plot_all_nodes(args.direction, args.save)
        plot_generated = True
    
    if args.heatmap:
        analyzer.plot_heatmap(args.save)
        plot_generated = True
    
    # Show plots if generated and not disabled
    if plot_generated and not args.no_show:
        plt.show()


if __name__ == "__main__":
    main()
