# `merge-parquets.py` script

- [`merge-parquets.py` script](#merge-parquetspy-script)
  - [Script Arguments](#script-arguments)
  - [Script Execution](#script-execution)
  - [Used Libraries](#used-libraries)


This script merges all parquet files from the provided directory into a single parquet file. It uses the [polars](https://www.pola.rs/) library for merging the data. The script supports the following compression types:
- lz4
- uncompressed
- snappy
- gzip
- lzo
- brotli
- zstd

Most notable, the script uses the lazyframe streaming API, which allows to process the data in a streaming manner, processing chunk by chunk keeping the memory usage low. Using lazyframe API allows a devloper to defined DAG of operations, which is optimized and executed in the most effective way using the Rust core near the CPU (without Python overhead and GC pauses). This part of the polars is very similar to the spark API and approach, but, it is still executed on a single node.

Another important aspect of the script is that it uses `pl.concat(frames, how="vertical_relaxed")` function. With the provided `how` parameter, it allows to merge the data in a way, that the polars iteratively concantenates the dataframes, updating their schema with the most common neighbouring type. For example, if in `dataframeA` column `C` has type `Int32` and in `dataframeB` column `C` has type `Int64`, and in `dataframeC` column `C` has type `Float64`, then the type relaxation will be `Int32 -> Int64 -> Float64` and the final type of the merged dataframe will be `Float64`. However, if types are not compatible, e.g. `List[String]` and `Int64`, then relaxation will fail and polars will raise an exception.


## Script Arguments

- `--source-dir` - source directory for dataset files
- `--output-file` - output file path
- `--prefix` - prefix for downloaded files
- `--file-extension` - file extension for downloaded files
- `--compression` - compression type for output parquet file (lz4, uncompressed, snappy, gzip, lzo, brotli, zstd)
- `--compression-level` - compression level.

## Script Execution

```bash
uv run merge/merge-parquets.py \
    --source-dir data \
    --output-file merged.parquet \
    --prefix green_tripdata \
    --file-extension parquet \
    --compression zstd \
    --compression-level 22
```

## Used Libraries

- [argparse](https://docs.python.org/3/library/argparse.html) - for parsing script arguments
- [polars](https://www.pola.rs/) - for merging parquet files & streaming API