# DocumentDB Sizing Tool

The sizing tool analyzes your MongoDB database and generates a CSV file for use with the DocumentDB Sizing Calculator. The tool automatically measures compression ratios using zstd-5-dict (matching Amazon DocumentDB 8.0), collects database statistics, and produces a properly formatted CSV file ready for upload to the sizing calculator.

# Requirements
 - Python 3.7+
 - pymongo Python package
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10+
   - If not installed - "$ pip3 install pymongo"
 - lz4 Python package
   - If not installed - "$ pip3 install lz4"
 - zstandard Python package
   - If not installed - "$ pip3 install zstandard"
 - compression-review.py script (included in this directory)

**Quick Install**: `pip3 install -r requirements.txt`

## Using the Sizing Tool
`python3 sizing.py --uri <server-uri>`

- Automatically uses zstd-5-dict compression (matching DocumentDB 8.0)
- Samples 1000 documents per collection by default
- Run on any instance in the replica set
- Creates a single CSV file per execution: `sizing-<timestamp>.csv`
- The \<server-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/
  - If your URI contains ampersand (&) characters they must be escaped with the backslash or enclosed your URI in double quotes
- For DocumentDB use either the cluster endpoint or any of the instance endpoints

### Optional Parameters

| Parameter | Default | Description |
| ----------- | ----------- | ----------- |
| --sample-size | 1000 | Number of documents to sample per collection |
| --dictionary-sample-size | 100 | Number of documents for dictionary creation |
| --dictionary-size | 4096 | Dictionary size in bytes |

### Example Usage

Localhost (no authentication):
```
python3 sizing.py --uri "mongodb://localhost:27017"
```

Remote server with authentication:
```
python3 sizing.py --uri "mongodb://username:password@hostname:27017"
```

With custom sample size:
```
python3 sizing.py --uri "mongodb://username:password@hostname:27017" --sample-size 2000
```

## Output

The tool generates a CSV file named: `sizing-<timestamp>.csv`

Example: `sizing-20260204123045.csv`

### CSV Columns
- SLNo
- Database_Name
- Collection_Name
- Document_Count
- Average_Document_Size
- Total_Indexes
- Index_Size
- Index_Working_Set
- Data_Working_Set
- Inserts_Per_Day
- Updates_Per_Day
- Deletes_Per_Day
- Reads_Per_Day
- Compression_Ratio

### Important Note: Manual Updates Required

The generated CSV includes default placeholder values for workload metrics that **MUST be manually updated** in a text editor:

| Field | Default Value | Description |
|-------|---------------|-------------|
| **Index_Working_Set** | 100% | Percentage of indexes that need to be in memory |
| **Data_Working_Set** | 10% | Percentage of data that needs to be in memory |
| **Inserts_Per_Day** | 0 | Number of insert operations per day |
| **Updates_Per_Day** | 0 | Number of update operations per day |
| **Deletes_Per_Day** | 0 | Number of delete operations per day |
| **Reads_Per_Day** | 0 | Number of read operations per day |

**Why manual updates are required:**
- These statistics cannot be calculated automatically from database metadata
- They require knowledge of your application's workload patterns
- Accurate values are critical for proper instance sizing and cost estimation

**How to update:**
1. Open the generated CSV file in a text editor (not Excel, which may corrupt the format)
2. Locate the columns for the fields above
3. Update each row with values based on your workload knowledge
4. Save the file
5. Upload to the DocumentDB Sizing Calculator

**Tips for determining values:**
- **Working Sets**: Use MongoDB monitoring tools or `db.serverStatus()` to understand memory usage patterns
- **Daily Operations**: Check application logs, MongoDB profiler, or monitoring dashboards for operation counts
- **Conservative estimates**: If unsure, use higher working set percentages and operation counts for safer sizing

## How It Works
1. Runs compression-review.py to analyze compression ratios using zstd-5-dict
2. Connects to MongoDB to gather collection statistics (document counts, sizes, indexes)
3. Combines compression data with collection metadata
4. Generates a CSV file formatted for the DocumentDB Sizing Calculator
5. Cleans up temporary files

## Next Steps
1. Run the sizing tool to generate your CSV file
2. Open the CSV and update workload metrics (working sets and daily operations) with your actual values
3. Upload the CSV to the DocumentDB Sizing Calculator
4. Review the sizing recommendations

## License
This tool is licensed under the Apache 2.0 License.
