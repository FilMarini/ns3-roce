#!/usr/bin/env python3
"""
Script to read and analyze QP Monitor Avro output files from ns-3 RDMA simulation.

Based on the QpMonitor module which records:
- node: Node ID
- lkey: Local key (QP identifier)
- time: Simulation time in seconds
- lowest_unacked_psn: First unacknowledged packet sequence number
- lowest_unsent_psn: Next packet sequence number to send
- end_work_psn: End of work PSN (last packet in queue)
"""

import avro.datafile
import avro.io
import sys
import json
from pathlib import Path
from typing import List, Dict, Any
import argparse


def read_avro_file(filepath: str) -> List[Dict[str, Any]]:
    """
    Read an Avro file and return a list of records.
    
    Args:
        filepath: Path to the Avro file
        
    Returns:
        List of dictionaries containing the records
    """
    records = []
    
    try:
        with open(filepath, 'rb') as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            
            for record in reader:
                records.append(record)
            
            reader.close()
            
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading Avro file: {e}", file=sys.stderr)
        sys.exit(1)
    
    return records


def analyze_qp_records(records: List[Dict[str, Any]]) -> None:
    """
    Analyze and display statistics about QP records.
    
    Args:
        records: List of QP records
    """
    if not records:
        print("No records found in the file")
        return
    
    # Group records by node and QP
    qp_data = {}
    for record in records:
        node = record['node']
        lkey = record['lkey']
        key = (node, lkey)
        
        if key not in qp_data:
            qp_data[key] = []
        qp_data[key].append(record)
    
    print(f"\n{'='*80}")
    print(f"QP Monitor Analysis")
    print(f"{'='*80}")
    print(f"Total records: {len(records)}")
    print(f"Number of unique QPs: {len(qp_data)}")
    print(f"{'='*80}\n")
    
    # Analyze each QP
    for (node, lkey), qp_records in sorted(qp_data.items()):
        qp_records.sort(key=lambda x: x['time'])
        
        first = qp_records[0]
        last = qp_records[-1]
        
        print(f"Node {node}, QP {lkey}:")
        print(f"  Number of samples: {len(qp_records)}")
        print(f"  Time range: {first['time']:.6f}s - {last['time']:.6f}s")
        print(f"  Duration: {last['time'] - first['time']:.6f}s")
        
        # Calculate progress metrics
        total_work = last['end_work_psn'] - first['lowest_unacked_psn']
        completed_work = last['lowest_unacked_psn'] - first['lowest_unacked_psn']
        
        print(f"  Total work (PSNs): {total_work}")
        print(f"  Completed work (PSNs): {completed_work}")
        
        if total_work > 0:
            progress = (completed_work / total_work) * 100
            print(f"  Progress: {progress:.2f}%")
        
        # Check if QP completed
        if last['lowest_unacked_psn'] == last['end_work_psn']:
            print(f"  Status: COMPLETED")
        else:
            outstanding = last['end_work_psn'] - last['lowest_unacked_psn']
            print(f"  Status: IN PROGRESS (outstanding PSNs: {outstanding})")
        
        print()


def export_to_csv(records: List[Dict[str, Any]], output_file: str) -> None:
    """
    Export records to CSV format.
    
    Args:
        records: List of QP records
        output_file: Output CSV file path
    """
    import csv
    
    if not records:
        print("No records to export")
        return
    
    # Get field names from first record
    fieldnames = list(records[0].keys())
    
    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        
        print(f"Exported {len(records)} records to {output_file}")
    except Exception as e:
        print(f"Error exporting to CSV: {e}", file=sys.stderr)


def export_to_json(records: List[Dict[str, Any]], output_file: str) -> None:
    """
    Export records to JSON format.
    
    Args:
        records: List of QP records
        output_file: Output JSON file path
    """
    try:
        with open(output_file, 'w') as jsonfile:
            json.dump(records, jsonfile, indent=2)
        
        print(f"Exported {len(records)} records to {output_file}")
    except Exception as e:
        print(f"Error exporting to JSON: {e}", file=sys.stderr)


def plot_qp_progress(records: List[Dict[str, Any]], output_file: str = None) -> None:
    """
    Create a plot showing QP progress over time.
    Requires matplotlib.
    
    Args:
        records: List of QP records
        output_file: Optional output file for the plot
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed. Install it with: pip install matplotlib")
        return
    
    if not records:
        print("No records to plot")
        return
    
    # Group by QP
    qp_data = {}
    for record in records:
        key = (record['node'], record['lkey'])
        if key not in qp_data:
            qp_data[key] = {'time': [], 'unacked': [], 'unsent': [], 'end': []}
        
        qp_data[key]['time'].append(record['time'])
        qp_data[key]['unacked'].append(record['lowest_unacked_psn'])
        qp_data[key]['unsent'].append(record['lowest_unsent_psn'])
        qp_data[key]['end'].append(record['end_work_psn'])
    
    # Create subplots for each QP
    n_qps = len(qp_data)
    fig, axes = plt.subplots(n_qps, 1, figsize=(12, 4*n_qps))
    
    if n_qps == 1:
        axes = [axes]
    
    for ax, ((node, lkey), data) in zip(axes, sorted(qp_data.items())):
        ax.plot(data['time'], data['unacked'], label='Lowest Unacked PSN', marker='o')
        ax.plot(data['time'], data['unsent'], label='Lowest Unsent PSN', marker='s')
        ax.plot(data['time'], data['end'], label='End Work PSN', marker='^')
        
        ax.set_xlabel('Time (s)')
        ax.set_ylabel('PSN')
        ax.set_title(f'QP Progress - Node {node}, QP {lkey}')
        ax.legend()
        ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"Plot saved to {output_file}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description='Read and analyze QP Monitor Avro output files from ns-3 RDMA simulation'
    )
    parser.add_argument('input_file', help='Path to the Avro file')
    parser.add_argument('--csv', help='Export to CSV file')
    parser.add_argument('--json', help='Export to JSON file')
    parser.add_argument('--plot', nargs='?', const='qp_progress.png', 
                       help='Generate plot (optionally specify output file)')
    parser.add_argument('--no-analysis', action='store_true',
                       help='Skip printing analysis to console')
    
    args = parser.parse_args()
    
    # Read the Avro file
    print(f"Reading Avro file: {args.input_file}")
    records = read_avro_file(args.input_file)
    
    # Print analysis
    if not args.no_analysis:
        analyze_qp_records(records)
    
    # Export to CSV if requested
    if args.csv:
        export_to_csv(records, args.csv)
    
    # Export to JSON if requested
    if args.json:
        export_to_json(records, args.json)
    
    # Generate plot if requested
    if args.plot is not None:
        plot_qp_progress(records, args.plot if args.plot else None)


if __name__ == '__main__':
    main()
