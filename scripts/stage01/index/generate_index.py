# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#     "argparse>=1.4.0",
#     "jinja2>=3.1.6",
#     "pyarrow>=19.0.1",
# ]
# ///

"""Module for generating report with number of rows in parquet files in a provided directory."""

import glob
import os
import argparse
import pyarrow.parquet as pq
from jinja2 import Environment, FileSystemLoader


def get_parquet_files_data(directory, replace_root=None):
    """Function extracting file data from files in provided directory"""
    files = sorted(glob.glob(f"{directory}/green_tripdata_*.parquet"))
    files_data = []

    for file in files:
        print(f"processing: {file}")
        filename = os.path.basename(file)
        year, month = (
            filename.removeprefix("green_tripdata_").removesuffix(".parquet").split("-")
        )
        size = os.path.getsize(file) / 1024 / 1024
        row_count = pq.read_metadata(file).num_rows

        file_path = file
        if replace_root is not None:
            file_path = os.path.join(replace_root, os.path.basename(file))

        files_data.append(
            {
                "year": year,
                "month": month,
                "size": f"{size:.1f}",
                "row_count": row_count,
                "file_path": file_path,
                "file_name": filename,
            }
        )

    return files_data


def generate_output(
    source_dir,
    output_format,
    output_file,
    include_summary=False,
    replace_root=None,
):
    """Function creating output about files in provided directory using provided output format"""
    env = Environment(loader=FileSystemLoader("templates"))

    if output_format.lower() == "html":
        template = env.get_template("template.html")
    elif output_format.lower() == "markdown":
        template = env.get_template("template.md")
    elif output_format.lower() == "typst":
        template = env.get_template("template.typ")
    else:
        raise ValueError("Output format must be 'html', 'markdown' or 'typst'")

    files_data = get_parquet_files_data(source_dir, replace_root)

    summary = None
    if include_summary:
        total_size = sum(float(file["size"]) for file in files_data)
        total_rows = sum(file["row_count"] for file in files_data)
        summary = {
            "title": "total",
            "total_size": f"{total_size:.1f}",
            "total_rows": total_rows,
        }

    rendered_content = template.render(files=files_data, summary=summary)

    with open(output_file, "w", encoding='utf-8') as f:
        f.write(rendered_content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate report from parquet files.")
    parser.add_argument(
        "--source-dir",
        default="data",
        help="Directory containing parquet files",
    )
    parser.add_argument(
        "--output-format",
        default="html",
        choices=["html", "markdown", "typst"],
        help="Output format: html, markdown or typst",
    )
    parser.add_argument(
        "--output-file",
        default="index.html",
        help="Output file path",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Include summary row with totals",
    )
    parser.add_argument(
        "--replace-root",
        default="data",
        help="Replace the root path with this custom base (e.g. URL)",
    )

    args = parser.parse_args()

    generate_output(
        args.source_dir,
        args.output_format,
        args.output_file,
        args.summary,
        args.replace_root,
    )
