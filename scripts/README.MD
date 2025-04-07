
# Scripts

## Data Loading (stage1)

The data loading is done using `stage1.sh`. Read more about preprocessing scripts in [dataset-organization](scripts/dataset-organization/README.md).

This script does the following:
1. Download all dataset files from original sources
2. Merge parquets files into a single file and compresses it using Zstandard with compression level 22
3. Loads data into PostgreSQL table
4. Loads data from PostgreSQL table to cluster using Scoop