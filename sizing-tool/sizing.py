"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Description:
    This script analyzes MongoDB collections to generate sizing data for the 
    Amazon DocumentDB Sizing Calculator. It runs compression analysis using 
    compression-review.py and combines the results with collection statistics 
    to produce a CSV file compatible with the sizing calculator.

Usage:
    python sizing.py --uri <mongodb-connection-uri> \\
        [--sample-size <number>] \\
        [--dictionary-sample-size <number>] \\
        [--dictionary-size <bytes>]

Script Parameters
-----------------
--uri: str (required)
    MongoDB Connection URI for the source database
--sample-size: int
    Number of documents to sample in each collection (default: 1000)
--dictionary-sample-size: int
    Number of documents to sample for dictionary creation (default: 100)
--dictionary-size: int
    Size of dictionary in bytes (default: 4096)

Output:
    Generates a CSV file named 'sizing-<timestamp>.csv' with 
    collection statistics and compression ratios for use in the DocumentDB 
    Sizing Calculator.
"""
import argparse
import sys
import csv
import glob
import os
import datetime as dt
import pymongo
import importlib.util

# Load the compression-review.py module using importlib
script_dir = os.path.dirname(os.path.abspath(__file__))
compression_script = os.path.join(script_dir, '..', 'performance', 'compression-review', 'compression-review.py')

spec = importlib.util.spec_from_file_location("compression_review", compression_script)
compression_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compression_module)

# Compressor to use for compression analysis
# zstd-5-dict matches Amazon DocumentDB 8.0 dictionary-based compression
COMPRESSOR = 'zstd-5-dict'

# Server alias for output file naming
SERVER_ALIAS = 'temp'


def run_compression_and_get_output(uri, sample_size, dictionary_sample_size, dictionary_size):
    """
    Run compression analysis and return the path to the generated CSV file.
    
    Args:
        uri: MongoDB connection URI
        sample_size: Number of documents to sample per collection
        dictionary_sample_size: Number of documents for dictionary creation
        dictionary_size: Size of dictionary in bytes
        
    Returns:
        str: Path to the generated compression CSV file
        
    Raises:
        RuntimeError: If compression analysis fails or no CSV file is created
    """
    print("Running compression analysis...")
    
    # Get list of existing CSV files before running compression analysis
    csv_pattern = f"{SERVER_ALIAS}-*-compression-review.csv"
    existing_csv_files = set(glob.glob(csv_pattern))
    
    # Configure and run compression analysis
    app_config = {
        'uri': uri,
        'serverAlias': SERVER_ALIAS,
        'sampleSize': sample_size,
        'compressor': COMPRESSOR,
        'dictionarySampleSize': dictionary_sample_size,
        'dictionarySize': dictionary_size
    }
    
    try:
        compression_module.getData(app_config)
    except Exception as e:
        raise RuntimeError(f"Error running compression analysis: {e}")
    
    # Find the newly created CSV file by comparing before and after
    current_csv_files = set(glob.glob(csv_pattern))
    new_csv_files = current_csv_files - existing_csv_files
    
    if not new_csv_files:
        raise RuntimeError(f"No new CSV file created. Expected pattern: {csv_pattern}")
    
    if len(new_csv_files) > 1:
        print(f"Warning: Multiple new CSV files found: {new_csv_files}")
        # Use the most recent one
        latest_csv = max(new_csv_files, key=os.path.getmtime)
    else:
        latest_csv = new_csv_files.pop()
    
    print(f"Parsing results from: {latest_csv}")
    return latest_csv


def parse_compression_csv(csv_filepath):
    """
    Parse compression review CSV and extract collection data.
    
    Args:
        csv_filepath: Path to the compression review CSV file
        
    Returns:
        dict: Dictionary mapping 'db.collection' to compression data
        
    Raises:
        RuntimeError: If CSV header cannot be found or file is invalid
    """
    comp_data = {}
    
    with open(csv_filepath, 'r') as f:
        # Read all lines to find where the actual data starts
        lines = f.readlines()
        
        # Find the header line (starts with dbName)
        header_idx = None
        for i, line in enumerate(lines):
            if line.startswith('dbName'):
                header_idx = i
                break
        
        if header_idx is None:
            raise RuntimeError("Could not find data header in CSV")
        
        # Use DictReader for named column access
        reader = csv.DictReader(lines[header_idx:])
        
        for row in reader:
            try:
                # Access columns by name instead of index
                db_name = row['dbName']
                coll_name = row['collName']
                num_docs = int(row['numDocs'])
                avg_doc_size = int(row['avgDocSize'])
                comp_ratio = float(row['compRatio'])
                
                key = f"{db_name}.{coll_name}"
                comp_data[key] = {
                    'db_name': db_name,
                    'coll_name': coll_name,
                    'num_docs': num_docs,
                    'avg_doc_size': avg_doc_size,
                    'comp_ratio': comp_ratio
                }
            except (KeyError, ValueError) as e:
                # Skip rows with missing columns or invalid data
                print(f"Warning: Skipping row due to error: {e}")
                continue
    
    return comp_data


def generate_sizing_csv(comp_data, uri):
    """
    Generate sizing calculator CSV by combining compression data with MongoDB stats.
    
    Args:
        comp_data: Dictionary of compression data from parse_compression_csv()
        uri: MongoDB connection URI
        
    Returns:
        str: Path to the generated sizing CSV file
    """
    print("Connecting to MongoDB to gather additional stats...")
    
    # Create output CSV file
    log_timestamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')
    output_filename = f"sizing-{log_timestamp}.csv"
    
    with pymongo.MongoClient(host=uri, appname='workload-calc') as client:
        with open(output_filename, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Write header
            csvwriter.writerow([
                'SLNo', 'Database_Name', 'Collection_Name', 'Document_Count',
                'Average_Document_Size', 'Total_Indexes', 'Index_Size',
                'Index_Working_Set', 'Data_Working_Set', 'Inserts_Per_Day',
                'Updates_Per_Day', 'Deletes_Per_Day', 'Reads_Per_Day',
                'Compression_Ratio'
            ])
            
            sl_no = 1
            
            # Iterate through collections from compression data
            for key, data in comp_data.items():
                db_name = data['db_name']
                coll_name = data['coll_name']
                
                try:
                    # Get collection stats from MongoDB
                    stats = client[db_name].command("collStats", coll_name)
                    
                    doc_count = data['num_docs']
                    avg_doc_size = data['avg_doc_size']
                    total_indexes = stats.get('nindexes', 0)
                    index_size_bytes = stats.get('totalIndexSize', 0)
                    index_size_gb = index_size_bytes / (1024 * 1024 * 1024)
                    comp_ratio = data['comp_ratio']
                    
                    # Default estimates for workload metrics
                    index_working_set = 100
                    data_working_set = 10
                    inserts_per_day = 0
                    updates_per_day = 0
                    deletes_per_day = 0
                    reads_per_day = 0
                    
                    # Write row
                    csvwriter.writerow([
                        sl_no,
                        db_name,
                        coll_name,
                        doc_count,
                        f"{avg_doc_size:.2f}",
                        total_indexes,
                        f"{index_size_gb:.4f}",
                        index_working_set,
                        data_working_set,
                        inserts_per_day,
                        updates_per_day,
                        deletes_per_day,
                        reads_per_day,
                        f"{comp_ratio:.4f}"
                    ])
                    
                    sl_no += 1
                    
                except Exception as e:
                    print(f"Error processing {db_name}.{coll_name}: {e}")
                    continue
    
    return output_filename


def validate_args(args):
    """
    Validate command-line arguments.
    
    Args:
        args: Parsed arguments from argparse
        
    Raises:
        ValueError: If any argument is invalid
    """
    # Validate URI format
    if not args.uri:
        raise ValueError("MongoDB URI cannot be empty")
    
    if not (args.uri.startswith('mongodb://') or args.uri.startswith('mongodb+srv://')):
        raise ValueError("MongoDB URI must start with 'mongodb://' or 'mongodb+srv://'")
    
    # Validate sample size (only check lower bound)
    if args.sample_size <= 0:
        raise ValueError(f"Sample size must be positive, got: {args.sample_size}")
    
    # Validate dictionary sample size (only check lower bound)
    if args.dictionary_sample_size <= 0:
        raise ValueError(f"Dictionary sample size must be positive, got: {args.dictionary_sample_size}")
    
    # Validate dictionary size (only check lower bound)
    if args.dictionary_size <= 0:
        raise ValueError(f"Dictionary size must be positive, got: {args.dictionary_size}")


def main():
    parser = argparse.ArgumentParser(description='Run compression review and analyze results')
    
    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='MongoDB Connection URI')
    
    parser.add_argument('--sample-size',
                        required=False,
                        type=int,
                        default=1000,
                        help='Number of documents to sample in each collection, default 1000')
    
    parser.add_argument('--dictionary-sample-size',
                        required=False,
                        type=int,
                        default=100,
                        help='Number of documents to sample for dictionary creation')
    
    parser.add_argument('--dictionary-size',
                        required=False,
                        type=int,
                        default=4096,
                        help='Size of dictionary (bytes)')
    
    args = parser.parse_args()
    
    # Validate arguments
    try:
        validate_args(args)
    except ValueError as e:
        parser.error(str(e))
        return
    
    # Run compression analysis and get the output CSV file
    try:
        compression_csv = run_compression_and_get_output(
            uri=args.uri,
            sample_size=args.sample_size,
            dictionary_sample_size=args.dictionary_sample_size,
            dictionary_size=args.dictionary_size
        )
    except RuntimeError as e:
        print(str(e))
        return
    
    # Parse compression CSV to extract collection data
    try:
        comp_data = parse_compression_csv(compression_csv)
    except RuntimeError as e:
        print(str(e))
        return
    
    # Generate sizing CSV by combining compression data with MongoDB stats
    output_filename = generate_sizing_csv(comp_data, args.uri)
    
    # Clean up the compression-review CSV file
    try:
        os.remove(compression_csv)
        print(f"Cleaned up temporary file: {compression_csv}")
    except Exception as e:
        print(f"Warning: Could not remove temporary file {compression_csv}: {e}")
    
    print(f"\nSizing CSV generated: {output_filename}")
    print("\n" + "="*80)
    print("IMPORTANT: Manual Updates Required")
    print("="*80)
    print("\nThe following fields have been set to default values and MUST be updated")
    print("manually in a text editor based on your workload knowledge:\n")
    print("  • Index_Working_Set (default: 100%) - Percentage of indexes in memory")
    print("  • Data_Working_Set (default: 10%) - Percentage of data in memory")
    print("  • Inserts_Per_Day (default: 0) - Daily insert operations")
    print("  • Updates_Per_Day (default: 0) - Daily update operations")
    print("  • Deletes_Per_Day (default: 0) - Daily delete operations")
    print("  • Reads_Per_Day (default: 0) - Daily read operations")
    print("\nThese statistics cannot be calculated automatically and require knowledge")
    print("of your existing workload patterns. Open the CSV file in a text editor")
    print("and update these values for accurate sizing recommendations.")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
