# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "argparse>=1.4.0",
#     "polars>=1.26.0",
# ]
# ///


from datetime import datetime
from pathlib import Path
import polars as pl
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate report from parquet files.")
    parser.add_argument(
        "--source-dir",
        default="data",
        help="Directory containing parquet files",
    )
    parser.add_argument(
        "--output-file",
        default="green_data.parquet",
        help="Output file path",
    )
    parser.add_argument(
        "--prefix",
        default="green_tripdata_",
        help="Prefix for downloaded files",
    )
    parser.add_argument(
        "--file-extension",
        default=".parquet",
        help="File extension for downloaded files",
    )

    args = parser.parse_args()

    parquets_files = sorted(Path(args.source_dir).glob("*.parquet"))
    lazy_frames: list[pl.LazyFrame] = []
    print("lazy loading parquet files")
    for parquet in parquets_files:
        date = datetime.strptime(
            parquet.stem.removeprefix(args.prefix).removesuffix(args.file_extension),
            "%Y-%m",
        )
        print(f"loading: {str(parquet.absolute())}")
        lazy_frames.append(
            pl.scan_parquet(str(parquet.absolute())).with_columns(
                pl.lit(date.year).alias("year"),
                pl.lit(date.month).alias("month"),
            )
        )

    print("merging parquet files")
    pl.concat(lazy_frames, how="vertical_relaxed").sink_parquet(
        args.output_file,
        compression="zstd",
        compression_level=22,
        statistics=False,
    )
    print("done!")
